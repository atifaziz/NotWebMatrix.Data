#region License, Terms and Author(s)
//
// NotWebMatrix
// Copyright (c) 2012 Atif Aziz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#endregion

namespace NotWebMatrix.Data
{
    #region Imports

    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Eggado;

    #endregion

    /// <summary>
    /// Provides methods and properties that are used to access and manage
    /// data that is stored in a database.
    /// </summary>

    public class Database : IDisposable
    {
        readonly Func<DbConnection> _connectionFactory;
        DbConnection _connection;

        public static Func<DbConnection, DbConnection> ConnectionDecorator { get; set; }

        public static event EventHandler<ConnectionEventArgs> ConnectionOpened;
        static event EventHandler<CommandEventArgs> GlobalCommandCreated;

        public static class GlobalEvents
        {
            // ReSharper disable MemberHidesStaticFromOuterClass
            public static event EventHandler<ConnectionEventArgs> ConnectionOpened // ReSharper restore MemberHidesStaticFromOuterClass
            {
                add    => Database.ConnectionOpened += value;
                remove => Database.ConnectionOpened -= value;
            }

            public static event EventHandler<CommandEventArgs> CommandCreated
            {
                add    => GlobalCommandCreated += value;
                remove => GlobalCommandCreated -= value;
            }
        }

        public event EventHandler<CommandEventArgs> CommandCreated;

        Database(Func<DbConnection> connectionFactory)
        {
            Debug.Assert(connectionFactory != null);
            _connectionFactory = connectionFactory;
        }

        public DbConnection Connection => _connection ??= _connectionFactory();

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            var connection = _connection;
            if (!disposing || connection == null)
                return;
            connection.Close();
            _connection = null;
        }

        public void Close() { Dispose(); }

        [Serializable]
        public class CommandOptions
        {
            public static readonly CommandOptions Default = new CommandOptions(null);

            protected CommandOptions(TimeSpan? commandTimeout) =>
                CommandTimeout = commandTimeout;

            public TimeSpan? CommandTimeout { get; }

            public CommandOptions WithTimeout(TimeSpan? value) =>
                CommandTimeout == value ? this : Update(value);

            protected virtual CommandOptions Update(TimeSpan? value) =>
                new CommandOptions(value);
        }

        public DbCommand Command(string commandText, params object[] args) =>
            Command(CommandOptions.Default, commandText, args);

        public DbCommand Command(CommandOptions options, string commandText, params object[] args)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            ValidatingCommandText(commandText);

            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
                ConnectionOpened?.Invoke(this, new ConnectionEventArgs(Connection));
            }

            var command = Connection.CreateCommand();
            command.CommandText = commandText;
            if (options?.CommandTimeout != null)
                command.CommandTimeout = (int)options.CommandTimeout.Value.TotalSeconds;
            var parameters = CreateParameters(command.CreateParameter, args);
            command.Parameters.AddRange(parameters.ToArray());
            OnCommandCreated(new CommandEventArgs(command));
            return command;
        }

        static string ValidatingCommandText(string commandText)
        {
            if (string.IsNullOrEmpty(commandText)) throw Exceptions.ArgumentNullOrEmpty(nameof(commandText));
            return commandText;
        }

        static IEnumerable<T> CreateParameters<T>(Func<T> parameterFactory, IEnumerable<object> args)
            where T : IDbDataParameter
        {
            Debug.Assert(parameterFactory != null);
            return args == null
                 ? Enumerable.Empty<T>()
                 : from arg in args.Select((a, i) => new KeyValuePair<int, object>(i, a))
                   select CreateParameter(parameterFactory, arg.Key, arg.Value);
        }

        static T CreateParameter<T>(Func<T> parameterFactory, int index, object value)
            where T : IDbDataParameter
        {
            Debug.Assert(parameterFactory != null);
            var parameter = parameterFactory();
            parameter.ParameterName = index.ToString(CultureInfo.InvariantCulture);
            if (value is Action<IDbDataParameter> actor)
                actor(parameter);
            else
                parameter.Value = value ?? DBNull.Value;
            return parameter;
        }

        void OnCommandCreated(CommandEventArgs args)
        {
            CommandCreated?.Invoke(this, args);
            GlobalCommandCreated?.Invoke(this, args);
        }

        [Serializable]
        public class QueryOptions : CommandOptions
        {
            public new static readonly QueryOptions Default = new QueryOptions(null, false);

            protected QueryOptions(TimeSpan? commandTimeout, bool unbuffered) :
                base(commandTimeout) =>
                Unbuffered = unbuffered;

            public bool Unbuffered { get; }

            public QueryOptions WithCommandTimeout(TimeSpan? value) =>
                value == CommandTimeout ? this : Update(value, Unbuffered);

            public QueryOptions WithUnbuffered(bool value) =>
                value == Unbuffered ? this : Update(CommandTimeout, value);

            protected override CommandOptions Update(TimeSpan? value) =>
                Update(value, Unbuffered);

            protected virtual QueryOptions Update(TimeSpan? commandTimeout, bool unbuffered) =>
                new QueryOptions(CommandTimeout, Unbuffered);
        }

        public IEnumerable<dynamic> Query(string commandText, params object[] args) =>
            Query(QueryOptions.Default, commandText, args);

        public IEnumerable<dynamic> Query(QueryOptions options, string commandText, params object[] args) =>
            QueryImpl(options, commandText, args);

        static readonly QueryOptions UnbufferedQueryOptions = QueryOptions.Default.WithUnbuffered(true);

        public dynamic QuerySingle(string commandText, params object[] args) =>
            QueryImpl(UnbufferedQueryOptions, commandText, args).FirstOrDefault();

        public IEnumerable<IDataRecord> QueryRecords(string commandText, params object[] args) =>
            QueryRecords(QueryOptions.Default, commandText, args);

        public IEnumerable<IDataRecord> QueryRecords(QueryOptions options, string commandText, params object[] args) =>
            Query(options, commandText, args, r => r.SelectRecords());

        IEnumerable<dynamic> QueryImpl(QueryOptions options, string commandText, object[] args) =>
            Query(options, commandText, args, r => r.Select());

        IEnumerable<T> Query<T>(QueryOptions options, string commandText, object[] args, Func<IDataReader, IEnumerator<T>> selector)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            ValidatingCommandText(commandText);

            Debug.Assert(selector != null);

            var items = _(); IEnumerable<T> _()
            {
                using var command = Command(options, commandText, args);
                var items = Eggnumerable.From(command.ExecuteReader, selector);
                foreach (var item in items)
                    yield return item;
            }

            return !options.Unbuffered
                 ? Array.AsReadOnly(items.ToArray())
                 : items;
        }

        public dynamic QueryValue(string commandText, params object[] args) =>
            QueryValue(CommandOptions.Default, commandText, args);

        public dynamic QueryValue(CommandOptions options, string commandText, params object[] args)
        {
            using var command = Command(options, commandText, args);
            return command.ExecuteScalar();
        }

        public Task<dynamic> QueryValueAsync(string commandText, params object[] args) =>
            QueryValueAsync(null, commandText, args);

        public Task<dynamic> QueryValueAsync(CommandOptions options,
                                             string commandText, params object[] args) =>
            QueryValueAsync(options, CancellationToken.None, commandText, args);

        public Task<dynamic> QueryValueAsync(CancellationToken cancellationToken,
                                             string commandText, params object[] args) =>
            QueryValueAsync(CommandOptions.Default, cancellationToken, commandText, args);

        public async Task<dynamic>
            QueryValueAsync(CommandOptions options,
                            CancellationToken cancellationToken,
                            string commandText, params object[] args)
        {
        #if ASYNC_DISPOSAL
            await //...
        #endif
            using var command = Command(options, commandText, args);
            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }

        public T QueryValue<T>(string commandText, params object[] args) =>
            QueryValue<T>(CommandOptions.Default, commandText, args);

        public T QueryValue<T>(CommandOptions options, string commandText, params object[] args) =>
            Convert<T>((object)QueryValue(options, commandText, args));

        public Task<T> QueryValueAsync<T>(string commandText, params object[] args) =>
            QueryValueAsync<T>(null, commandText, args);

        public Task<T> QueryValueAsync<T>(CommandOptions options,
                                          string commandText, params object[] args) =>
            QueryValueAsync<T>(options, CancellationToken.None, commandText, args);

        public Task<T> QueryValueAsync<T>(CancellationToken cancellationToken,
                                          string commandText, params object[] args) =>
            QueryValueAsync<T>(CommandOptions.Default, cancellationToken, commandText, args);

        public async Task<T> QueryValueAsync<T>(CommandOptions options,
                                                CancellationToken cancellationToken,
                                                string commandText, params object[] args) =>
            Convert<T>((object)await QueryValueAsync(options, cancellationToken,
                                                     commandText, args).ConfigureAwait(false));

        static T Convert<T>(object value)
        {
            if (value == null || System.Convert.IsDBNull(value))
                return (T)(object)null;

            var type = typeof(T);
            var conversionType = type.IsGenericType
                                 && !type.IsGenericTypeDefinition
                                 && typeof(Nullable<>) == type.GetGenericTypeDefinition()
                                 ? Nullable.GetUnderlyingType(type)
                                 : type;

            return (T)System.Convert.ChangeType(value, conversionType, CultureInfo.InvariantCulture);
        }

        public int Execute(string commandText, params object[] args) =>
            Execute(CommandOptions.Default, commandText, args);

        public int Execute(CommandOptions options, string commandText, params object[] args)
        {
            using var command = Command(options, commandText, args);
            return command.ExecuteNonQuery();
        }

        public dynamic GetLastInsertId() =>
            QueryValue("SELECT @@Identity");

        public Task<dynamic> GetLastInsertIdAsync() =>
            GetLastInsertIdAsync(new CancellationToken());

        public Task<dynamic> GetLastInsertIdAsync(CancellationToken cancellationToken) =>
            QueryValueAsync(cancellationToken, "SELECT @@Identity");

        public static Database Open(string name)
        {
            if (string.IsNullOrEmpty(name)) throw Exceptions.ArgumentNullOrEmpty(nameof(name));
            return OpenNamedConnection(name);
        }

        public static Database OpenConnectionString(string connectionString) =>
            OpenConnectionString(connectionString, (DbProviderFactory)null);

        public static Database OpenConnectionString(string connectionString, string providerName) =>
            OpenConnectionStringImpl(providerName, null, connectionString);

        public static Database OpenConnectionString(string connectionString, DbProviderFactory providerFactory) =>
            OpenConnectionStringImpl(null, providerFactory, connectionString);

        static Database OpenConnectionStringImpl(string providerName, DbProviderFactory providerFactory, string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString)) throw Exceptions.ArgumentNullOrEmpty(nameof(connectionString));

            var decorator = ConnectionDecorator ?? (connection => connection);

            if (providerFactory == null)
            {
                #if NETSTANDARD2_1

                if (string.IsNullOrEmpty(providerName))
                    providerName = GetDefaultProviderName();

                #else

                throw new ArgumentNullException(nameof(providerFactory));

                #endif
            }
            else
            {
                Debug.Assert(string.IsNullOrEmpty(providerName), @"Specify either provider factory or name but not both.");
            }

            return new Database(() =>
            {
                #if NETSTANDARD2_1

                if (providerFactory == null)
                {
                    Debug.Assert(providerName != null);
                    providerFactory = DbProviderFactories.GetFactory(providerName);
                }

                #endif

                var connection = providerFactory.CreateConnection();
                if (connection == null)
                    throw new NullReferenceException($"{providerFactory.GetType()} returned a connection reference not set to an instance.");

                connection.ConnectionString = connectionString;
                return decorator(connection);
            });
        }

        public static readonly Func<string, ConnectionStringSettings> DefaultNamedConnectionStringResolver = name => ConfigurationManager.ConnectionStrings[name];

        static Func<string, ConnectionStringSettings> _namedConnectionStringResolver;

        public static Func<string, ConnectionStringSettings> NamedConnectionStringResolver
        {
            get => _namedConnectionStringResolver ?? DefaultNamedConnectionStringResolver;
            set => _namedConnectionStringResolver = value ?? throw new ArgumentNullException(nameof(value));
        }

        static Database OpenNamedConnection(string name)
        {
            var connectionStringSettings = NamedConnectionStringResolver(name);
            return connectionStringSettings != null
                 ? OpenConnectionStringImpl(connectionStringSettings.ProviderName, null, connectionStringSettings.ConnectionString)
                 : throw new InvalidOperationException($@"Connection string ""{name}"" was not found.");
        }

        static string GetDefaultProviderName()
        {
            var value = ConfigurationManager.AppSettings["NotWebMatrix.Data.Database.DefaultProvider"];
            return !string.IsNullOrWhiteSpace(value) ? value : "System.Data.SqlServerCe.4.0";
        }

        sealed class DatabaseOpener : IDatabaseOpener
        {
            readonly Func<Database> _opener;

            public DatabaseOpener(Func<Database> opener)
            {
                Debug.Assert(opener != null);
                _opener = opener;
            }

            public Database Open() => _opener();
        }

        public static IDatabaseOpener Opener(string name) =>
            new DatabaseOpener(() => Open(name));

        public static IDatabaseOpener ConnectionStringOpener(string connectionString) =>
            new DatabaseOpener(() => OpenConnectionString(connectionString));

        public static IDatabaseOpener ConnectionStringOpener(string connectionString, string providerName) =>
            new DatabaseOpener(() => OpenConnectionString(connectionString, providerName));

        public static IDatabaseOpener ConnectionStringOpener(string connectionString, DbProviderFactory providerFactory) =>
            new DatabaseOpener(() => OpenConnectionString(connectionString, providerFactory));

        static class Exceptions
        {
            public static ArgumentException ArgumentNullOrEmpty(string paramName) =>
                new ArgumentException(@"Value cannot be null or an empty string.", paramName);
        }
    }
}

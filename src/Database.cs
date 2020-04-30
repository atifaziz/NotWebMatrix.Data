#region Copyright (c) 2012 Atif Aziz. All rights reserved.
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
    using Experimental;

    #endregion

    /// <summary>
    /// Provides methods and properties that are used to access and manage
    /// data that is stored in a database.
    /// </summary>

    public partial class Database : IDisposable
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

        public void Close() => Dispose();

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
            Command(commandText, args, CommandOptions.Default);

        [Obsolete("Use the non-variadic overload.")]
        public DbCommand Command(CommandOptions options, string commandText, params object[] args) =>
            Command(commandText, args, options);

        public DbCommand Command(string commandText, IEnumerable<object> args, CommandOptions options) =>
            Command(new CommandText(commandText), args, options);

        internal DbCommand Command(CommandText commandText, IEnumerable<object> args, CommandOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            if (commandText.Formattable is null)
                ValidatingCommandText(commandText.Literal);

            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
                ConnectionOpened?.Invoke(this, new ConnectionEventArgs(Connection));
            }

            var command = Connection.CreateCommand();
            var anonymousIndex = 0;

            switch (commandText.Literal, commandText.Formattable)
            {
                case (var literal, null):
                {
                    command.CommandText = literal;
                    foreach (var arg in args ?? Enumerable.Empty<object>())
                        command.Parameters.Add(CreateParameter(arg));
                    break;
                }
                case (null, var formattable):
                {
                    var (text, parameters) =
                        Sql.FormatCommand(formattable.Formatter,
                                          formattable.FormattableString,
                                          CreateParameter);
                    command.CommandText = text;
                    foreach (var parameter in parameters)
                        command.Parameters.Add(parameter);
                    break;
                }
            }

            if (options.CommandTimeout is TimeSpan timeout)
                command.CommandTimeout = (int)timeout.TotalSeconds;

            OnCommandCreated(new CommandEventArgs(command));
            return command;

            DbParameter CreateParameter(object arg)
            {
                var parameter = command.CreateParameter();
                if (arg is Action<IDbDataParameter> actor)
                    actor(parameter);
                else
                    parameter.Value = arg ?? DBNull.Value;
                if (string.IsNullOrEmpty(parameter.ParameterName))
                    parameter.ParameterName = anonymousIndex++.ToString(CultureInfo.InvariantCulture);
                return parameter;
            }
        }

        static string ValidatingCommandText(string commandText)
        {
            if (string.IsNullOrEmpty(commandText)) throw Exceptions.ArgumentNullOrEmpty(nameof(commandText));
            return commandText;
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
            Query(commandText, args, QueryOptions.Default);

        [Obsolete("Use the non-variadic overload.")]
        public IEnumerable<dynamic> Query(QueryOptions options, string commandText, params object[] args) =>
            Query(commandText, args, options);

        public IEnumerable<dynamic> Query(string commandText, IEnumerable<object> args, QueryOptions options) =>
            QueryImpl(commandText, args, options);

        static readonly QueryOptions UnbufferedQueryOptions = QueryOptions.Default.WithUnbuffered(true);

        public dynamic QuerySingle(string commandText, params object[] args) =>
            QueryImpl(commandText, args, UnbufferedQueryOptions).FirstOrDefault();

        public dynamic QuerySingle(string commandText, IEnumerable<object> args, QueryOptions options) =>
            QueryImpl(commandText, args, options.WithUnbuffered(true)).FirstOrDefault();

        #if ASYNC_STREAMS

        public Task<dynamic> QuerySingleAsync(string commandText, params object[] args) =>
            QuerySingleAsync(commandText, args, QueryOptions.Default);

        public Task<dynamic> QuerySingleAsync(string commandText, IEnumerable<object> args,
                                              QueryOptions options) =>
            QuerySingleAsync(commandText, args, options, CancellationToken.None);

        public Task<dynamic> QuerySingleAsync(string commandText, IEnumerable<object> args,
                                              CancellationToken cancellationToken) =>
            QuerySingleAsync(commandText, args, QueryOptions.Default, cancellationToken);

        public async Task<dynamic>
            QuerySingleAsync(string commandText, IEnumerable<object> args,
                             QueryOptions options,
                             CancellationToken cancellationToken)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            ValidatingCommandText(commandText);

            var query = QueryAsync(commandText, args, options);

            await foreach (var item in query.ConfigureAwait(false)
                                            .WithCancellation(cancellationToken))
            {
                return item;
            }

            return null;
        }

        #endif

        public IEnumerable<IDataRecord> QueryRecords(string commandText, params object[] args) =>
            QueryRecords(commandText, args, QueryOptions.Default);

        [Obsolete("Use the non-variadic overload.")]
        public IEnumerable<IDataRecord> QueryRecords(QueryOptions options, string commandText, params object[] args) =>
            Query(commandText, args, options, r => r.SelectRecords());

        public IEnumerable<IDataRecord> QueryRecords(string commandText, IEnumerable<object> args,
                                                     QueryOptions options) =>
            Query(commandText, args, options, r => r.SelectRecords());

        IEnumerable<dynamic> QueryImpl(string commandText, IEnumerable<object> args, QueryOptions options) =>
            Query(commandText, args, options, r => r.Select());

        IEnumerable<T> Query<T>(string commandText, IEnumerable<object> args,
                                QueryOptions options,
                                Func<IDataReader, IEnumerator<T>> selector)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            ValidatingCommandText(commandText);

            Debug.Assert(selector != null);

            var items = _(); IEnumerable<T> _()
            {
                using var command = Command(commandText, args, options);
                var items = Eggnumerable.From(command.ExecuteReader, selector);
                foreach (var item in items)
                    yield return item;
            }

            return !options.Unbuffered
                 ? Array.AsReadOnly(items.ToArray())
                 : items;
        }

        #if ASYNC_STREAMS

        public IAsyncEnumerable<dynamic>
            QueryAsync(string commandText, params object[] args) =>
            QueryAsync(commandText, args, QueryOptions.Default);

        public IAsyncEnumerable<dynamic>
            QueryAsync(string commandText, IEnumerable<object> args,
                       QueryOptions options) =>
            QueryAsync(commandText, args, options, (r, ct) => r.SelectAsync(ct));

        public IAsyncEnumerable<IDataRecord>
            QueryRecordsAsync(string commandText, params object[] args) =>
            QueryRecordsAsync(commandText, args, QueryOptions.Default);

        public IAsyncEnumerable<IDataRecord>
            QueryRecordsAsync(string commandText, IEnumerable<object> args,
                              QueryOptions options) =>
            QueryAsync(commandText, args, options, (r, ct) => r.SelectRecordsAsync(ct));

        IAsyncEnumerable<T> QueryAsync<T>(string commandText, IEnumerable<object> args,
                                          CommandOptions options,
                                          Func<DbDataReader, CancellationToken, IAsyncEnumerator<T>> selector)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            ValidatingCommandText(commandText);

            Debug.Assert(selector != null);

            return _(); async IAsyncEnumerable<T> _()
            {
                await using var command = Command(commandText, args, options);
                var items = Eggnumerable.FromAsync(ct => command.ExecuteReaderAsync(ct), selector);
                await foreach (var item in items.ConfigureAwait(false))
                    yield return item;
            }
        }

        #endif

        public dynamic QueryValue(string commandText, params object[] args) =>
            QueryValue(commandText, args, CommandOptions.Default);

        [Obsolete("Use the non-variadic overload.")]
        public dynamic QueryValue(CommandOptions options, string commandText, params object[] args) =>
            QueryValue(commandText, args, options);

        public dynamic QueryValue(string commandText, IEnumerable<object> args, CommandOptions options)
        {
            using var command = Command(commandText, args, options);
            return command.ExecuteScalar();
        }

        public Task<dynamic> QueryValueAsync(string commandText, params object[] args) =>
            QueryValueAsync(commandText, args, CancellationToken.None);

        public Task<dynamic> QueryValueAsync(string commandText, IEnumerable<object> args,
                                             CommandOptions options) =>
            QueryValueAsync(commandText, args, options, CancellationToken.None);

        public Task<dynamic> QueryValueAsync(string commandText, IEnumerable<object> args,
                                             CancellationToken cancellationToken) =>
            QueryValueAsync(commandText, args, CommandOptions.Default, cancellationToken);

        public async Task<dynamic>
            QueryValueAsync(string commandText, IEnumerable<object> args,
                            CommandOptions options, CancellationToken cancellationToken)
        {
        #if ASYNC_DISPOSAL
            await //...
        #endif
            using var command = Command(commandText, args, options);
            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }

        public T QueryValue<T>(string commandText, params object[] args) =>
            QueryValue<T>(commandText, args, CommandOptions.Default);

        [Obsolete("Use the non-variadic overload.")]
        public T QueryValue<T>(CommandOptions options, string commandText, params object[] args) =>
            QueryValue<T>(commandText, args, options);

        public T QueryValue<T>(string commandText, IEnumerable<object> args, CommandOptions options) =>
            Convert<T>((object)QueryValue(commandText, args, options));

        public Task<T> QueryValueAsync<T>(string commandText, params object[] args) =>
            QueryValueAsync<T>(commandText, args, CancellationToken.None);

        public Task<T> QueryValueAsync<T>(string commandText, IEnumerable<object> args,
                                          CommandOptions options) =>
            QueryValueAsync<T>(commandText, args, options, CancellationToken.None);

        public Task<T> QueryValueAsync<T>(string commandText, IEnumerable<object> args,
                                          CancellationToken cancellationToken) =>
            QueryValueAsync<T>(commandText, args, CommandOptions.Default, cancellationToken);

        public async Task<T> QueryValueAsync<T>(string commandText, IEnumerable<object> args,
                                                CommandOptions options,
                                                CancellationToken cancellationToken) =>
            Convert<T>((object)await QueryValueAsync(commandText, args, options, cancellationToken).ConfigureAwait(false));

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
            Execute(commandText, args, CommandOptions.Default);

        [Obsolete("Use the non-variadic overload.")]
        public int Execute(CommandOptions options, string commandText, params object[] args) =>
            Execute(commandText, args, options);

        public int Execute(string commandText, IEnumerable<object> args, CommandOptions options)
        {
            using var command = Command(commandText, args, options);
            return command.ExecuteNonQuery();
        }

        public Task<int> ExecuteAsync(string commandText, params object[] args) =>
            ExecuteAsync(commandText, args, CancellationToken.None);

        public Task<int> ExecuteAsync(string commandText, IEnumerable<object> args,
                                      CancellationToken cancellationToken) =>
            ExecuteAsync(commandText, args, CommandOptions.Default, cancellationToken);

        public Task<int> ExecuteAsync(string commandText, IEnumerable<object> args,
                                      CommandOptions options) =>
            ExecuteAsync(commandText, args, options, CancellationToken.None);

        public async Task<int>
            ExecuteAsync(string commandText, IEnumerable<object> args,
                         CommandOptions options, CancellationToken cancellationToken)
        {
        #if ASYNC_DISPOSAL
            await //...
        #endif
            using var command = Command(commandText, args, options);
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        public dynamic GetLastInsertId() =>
            QueryValue("SELECT @@Identity");

        public Task<dynamic> GetLastInsertIdAsync() =>
            GetLastInsertIdAsync(new CancellationToken());

        public Task<dynamic> GetLastInsertIdAsync(CancellationToken cancellationToken) =>
            QueryValueAsync("SELECT @@Identity", cancellationToken);

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

    #if ASYNC_DISPOSAL

    partial class Database : IAsyncDisposable
    {
        public virtual async ValueTask CloseAsync()
        {
            var connection = _connection;
            if (connection == null)
                return;
            await connection.CloseAsync().ConfigureAwait(false);
            _connection = null;
            GC.SuppressFinalize(this);
        }

        ValueTask IAsyncDisposable.DisposeAsync() => CloseAsync();
    }

    #endif

    struct CommandText
    {
        public readonly string Literal;
        public readonly FormattableCommandText Formattable;

        public CommandText(string literal) : this(literal, null) {}
        public CommandText(FormattableCommandText formattable) : this(null, formattable) { }

        CommandText(string literal, FormattableCommandText formattable) =>
            (Literal, Formattable) = (literal, formattable);
    }

    namespace Experimental
    {
        public sealed class FormattableCommandText
        {
            object[] _cachedArguments;

            public IFormatter Formatter { get; }
            public FormattableString FormattableString { get; }
            public string Format => FormattableString.Format;
            public object[] Arguments => _cachedArguments ??= FormattableString.GetArguments();

            public FormattableCommandText(IFormatter formatter, FormattableString formattableString) =>
                (Formatter, FormattableString) = (formatter, formattableString);
        }

        public static class TransactSqlModule
        {
            public static FormattableCommandText TSqlFormat(FormattableString fs) =>
                new FormattableCommandText(TSqlFormatter.Instance, fs);
        }

        public static class DatabaseExtensions
        {
            public static DbCommand Command(this Database db, FormattableCommandText commandText) =>
                Command(db, commandText, Database.CommandOptions.Default);

            public static DbCommand Command(this Database db, FormattableCommandText commandText, Database.CommandOptions options)
            {
                if (db == null) throw new ArgumentNullException(nameof(db));
                if (commandText == null) throw new ArgumentNullException(nameof(commandText));
                return db.Command(new CommandText(commandText), commandText.Arguments, options);
            }
        }
    }
}

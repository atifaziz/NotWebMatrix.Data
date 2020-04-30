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
            Command(this, commandText, args, options);

        internal static DbCommand Command(Database db, CommandText commandText, IEnumerable<object> args, CommandOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            if (commandText.Formattable is null)
                ValidatingCommandText(commandText.Literal);

            if (db.Connection.State != ConnectionState.Open)
            {
                db.Connection.Open();
                ConnectionOpened?.Invoke(db, new ConnectionEventArgs(db.Connection));
            }

            var command = db.Connection.CreateCommand();
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
                case (null, var fs):
                {
                    var (text, parameters) = Sql.FormatCommand(db.Formatter, fs, CreateParameter);
                    command.CommandText = text;
                    foreach (var parameter in parameters)
                        command.Parameters.Add(parameter);
                    break;
                }
            }

            if (options.CommandTimeout is TimeSpan timeout)
                command.CommandTimeout = (int)timeout.TotalSeconds;

            db.OnCommandCreated(new CommandEventArgs(command));
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
            Query(this, commandText, args, options);

        internal static IEnumerable<dynamic>
            Query(Database db, CommandText commandText, IEnumerable<object> args, QueryOptions options) =>
            db.QueryImpl(commandText, args, options);

        static readonly QueryOptions UnbufferedQueryOptions = QueryOptions.Default.WithUnbuffered(true);

        public dynamic QuerySingle(string commandText, params object[] args) =>
            QuerySingle(this, commandText, args, UnbufferedQueryOptions);

        public dynamic QuerySingle(string commandText, IEnumerable<object> args, QueryOptions options) =>
            QuerySingle(this, commandText, args, options.WithUnbuffered(true));

        internal static dynamic
            QuerySingle(Database db, CommandText commandText, IEnumerable<object> args, QueryOptions options) =>
            db.QueryImpl(commandText, args, options.WithUnbuffered(true)).FirstOrDefault();

#if ASYNC_STREAMS

        public Task<dynamic> QuerySingleAsync(string commandText, params object[] args) =>
            QuerySingleAsync(commandText, args, QueryOptions.Default);

        public Task<dynamic> QuerySingleAsync(string commandText, IEnumerable<object> args,
                                              QueryOptions options) =>
            QuerySingleAsync(commandText, args, options, CancellationToken.None);

        public Task<dynamic> QuerySingleAsync(string commandText, IEnumerable<object> args,
                                              CancellationToken cancellationToken) =>
            QuerySingleAsync(commandText, args, QueryOptions.Default, cancellationToken);

        public Task<dynamic>
            QuerySingleAsync(string commandText, IEnumerable<object> args,
                             QueryOptions options,
                             CancellationToken cancellationToken) =>
            QuerySingleAsync(this, commandText, args, options, cancellationToken);

        internal static async Task<dynamic>
            QuerySingleAsync(Database db, CommandText commandText, IEnumerable<object> args,
                             QueryOptions options,
                             CancellationToken cancellationToken)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            if (commandText.Formattable is null)
                ValidatingCommandText(commandText.Literal);

            var query = QueryAsync(db, commandText, args, options);

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
            QueryRecords(commandText, args, options);

        public IEnumerable<IDataRecord> QueryRecords(string commandText, IEnumerable<object> args,
                                                     QueryOptions options) =>
            QueryRecords(this, commandText, args, options);

        internal static IEnumerable<IDataRecord>
            QueryRecords(Database db, CommandText commandText, IEnumerable<object> args,
                         QueryOptions options) =>
            db.Query(commandText, args, options, r => r.SelectRecords());

        IEnumerable<dynamic> QueryImpl(CommandText commandText, IEnumerable<object> args, QueryOptions options) =>
            Query(commandText, args, options, r => r.Select());

        internal IEnumerable<T>
            Query<T>(CommandText commandText, IEnumerable<object> args,
                     QueryOptions options,
                     Func<IDataReader, IEnumerator<T>> selector)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            if (commandText.Formattable is null)
                ValidatingCommandText(commandText.Literal);

            Debug.Assert(selector != null);

            var items = _(); IEnumerable<T> _()
            {
                using var command = Command(this, commandText, args, options);
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
            QueryAsync(this,  commandText, args, options);

        internal static IAsyncEnumerable<dynamic>
            QueryAsync(Database db, CommandText commandText, IEnumerable<object> args,
                       QueryOptions options) =>
            db.QueryAsync(commandText, args, options, (r, ct) => r.SelectAsync(ct));

        public IAsyncEnumerable<IDataRecord>
            QueryRecordsAsync(string commandText, params object[] args) =>
            QueryRecordsAsync(commandText, args, QueryOptions.Default);

        public IAsyncEnumerable<IDataRecord>
            QueryRecordsAsync(string commandText, IEnumerable<object> args,
                              QueryOptions options) =>
            QueryRecordsAsync(this, commandText, args, options);

        internal static IAsyncEnumerable<IDataRecord>
            QueryRecordsAsync(Database db, CommandText commandText, IEnumerable<object> args,
                              QueryOptions options) =>
            db.QueryAsync(commandText, args, options, (r, ct) => r.SelectRecordsAsync(ct));

        IAsyncEnumerable<T> QueryAsync<T>(CommandText commandText, IEnumerable<object> args,
                                          CommandOptions options,
                                          Func<DbDataReader, CancellationToken, IAsyncEnumerator<T>> selector)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            if (commandText.Formattable is null)
                ValidatingCommandText(commandText.Literal);

            Debug.Assert(selector != null);

            return _(); async IAsyncEnumerable<T> _()
            {
                await using var command = Command(this, commandText, args, options);
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

        public dynamic QueryValue(string commandText, IEnumerable<object> args, CommandOptions options) =>
            QueryValue(this, commandText, args, options);

        internal static dynamic QueryValue(Database db,
                                           CommandText commandText, IEnumerable<object> args,
                                           CommandOptions options)
        {
            using var command = Command(db, commandText, args, options);
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

        public Task<dynamic>
            QueryValueAsync(string commandText, IEnumerable<object> args,
                            CommandOptions options, CancellationToken cancellationToken) =>
            QueryValueAsync(this, commandText, args, options, cancellationToken);

        internal static async Task<dynamic>
            QueryValueAsync(Database db, CommandText commandText, IEnumerable<object> args,
                            CommandOptions options, CancellationToken cancellationToken)
        {
        #if ASYNC_DISPOSAL
            await //...
        #endif
            using var command = Command(db, commandText, args, options);
            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }

        public T QueryValue<T>(string commandText, params object[] args) =>
            QueryValue<T>(commandText, args, CommandOptions.Default);

        [Obsolete("Use the non-variadic overload.")]
        public T QueryValue<T>(CommandOptions options, string commandText, params object[] args) =>
            QueryValue<T>(commandText, args, options);

        public T QueryValue<T>(string commandText, IEnumerable<object> args, CommandOptions options) =>
            QueryValue<T>(this, commandText, args, options);

        internal static T QueryValue<T>(Database db, CommandText commandText, IEnumerable<object> args,
                                        CommandOptions options) =>
            Convert<T>((object)QueryValue(db, commandText, args, options));

        public Task<T> QueryValueAsync<T>(string commandText, params object[] args) =>
            QueryValueAsync<T>(commandText, args, CancellationToken.None);

        public Task<T> QueryValueAsync<T>(string commandText, IEnumerable<object> args,
                                          CommandOptions options) =>
            QueryValueAsync<T>(commandText, args, options, CancellationToken.None);

        public Task<T> QueryValueAsync<T>(string commandText, IEnumerable<object> args,
                                          CancellationToken cancellationToken) =>
            QueryValueAsync<T>(commandText, args, CommandOptions.Default, cancellationToken);

        public Task<T> QueryValueAsync<T>(string commandText, IEnumerable<object> args,
                                          CommandOptions options,
                                          CancellationToken cancellationToken) =>
            QueryValueAsync<T>(this, commandText, args, options, cancellationToken);

        internal static async Task<T>
            QueryValueAsync<T>(Database db,
                               CommandText commandText, IEnumerable<object> args,
                               CommandOptions options,
                               CancellationToken cancellationToken) =>
            Convert<T>((object)await QueryValueAsync(db, commandText, args, options, cancellationToken).ConfigureAwait(false));

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

        public int Execute(string commandText, IEnumerable<object> args, CommandOptions options) =>
            Execute(this, commandText, args, options);

        internal static int Execute(Database db, CommandText commandText, IEnumerable<object> args,
                                    CommandOptions options)
        {
            using var command = Command(db, commandText, args, options);
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

        public Task<int>
            ExecuteAsync(string commandText, IEnumerable<object> args,
                         CommandOptions options, CancellationToken cancellationToken) =>
            ExecuteAsync(this, commandText, args, options, cancellationToken);

        internal static async Task<int>
            ExecuteAsync(Database db, CommandText commandText, IEnumerable<object> args,
                         CommandOptions options, CancellationToken cancellationToken)
        {
        #if ASYNC_DISPOSAL
            await //...
        #endif
            using var command = Command(db, commandText, args, options);
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

    partial class Database : IDatabase
    {
        IFormatter _formatter;

        internal IFormatter Formatter
        {
            get => _formatter ?? throw new InvalidOperationException();
            set => _formatter = value;
        }

        Database IDatabase.Base => this;

        DbCommand IDatabase.Command(FormattableString commandText, CommandOptions options) =>
            Command(this, commandText, commandText.GetArguments(), options);
    }

    struct CommandText
    {
        public readonly string Literal;
        public readonly FormattableString Formattable;

        public CommandText(string literal) : this(literal, null) {}
        public CommandText(FormattableString fs) : this(null, fs) { }

        CommandText(string literal, FormattableString fs) =>
            (Literal, Formattable) = (literal, fs);

        public static implicit operator CommandText(string s) => new CommandText(s);
        public static implicit operator CommandText(FormattableString fs) => new CommandText(fs);
    }

    namespace Experimental
    {
        using System.Runtime.CompilerServices;

        public partial interface IDatabase : IDisposable
        {
            Data.Database Base { get; }

            event EventHandler<CommandEventArgs> CommandCreated;

            DbConnection Connection { get; }

            DbCommand Command(FormattableString commandText, Data.Database.CommandOptions options);
            /*
            int Execute(FormattableString commandText, Database.CommandOptions options);
            Task<int> ExecuteAsync(FormattableString commandText,
                                   Database.CommandOptions options,
                                   CancellationToken cancellationToken);

            IEnumerable<dynamic> Query(FormattableString commandText, Database.QueryOptions options);
            IAsyncEnumerable<dynamic> QueryAsync(FormattableString commandText,
                                                 Database.QueryOptions options,
                                                 CancellationToken cancellationToken);

            IEnumerable<IDataRecord> QueryRecords(FormattableString commandText,
                                                  Database.QueryOptions options);

            IAsyncEnumerable<IDataRecord>
                QueryRecordsAsync(FormattableString commandText,
                                  Database.QueryOptions options,
                                  CancellationToken cancellationToken);

            dynamic QuerySingle(FormattableString commandText, Database.QueryOptions options);
            Task<dynamic> QuerySingleAsync(FormattableString commandText,
                                           Database.QueryOptions options,
                                           CancellationToken cancellationToken);

            dynamic QueryValue(FormattableString commandText, Database.QueryOptions options);
            Task<dynamic> QueryValueAsync(FormattableString commandText,
                                          Database.QueryOptions options,
                                          CancellationToken cancellationToken);

            T QueryValue<T>(FormattableString commandText, Database.QueryOptions options);
            Task<T> QueryValueAsync<T>(FormattableString commandText,
                                       Database.QueryOptions options,
                                       CancellationToken cancellationToken);

            dynamic GetLastInsertId();
            */
            Task<dynamic> GetLastInsertIdAsync(CancellationToken cancellationToken);
        }

        #if ASYNC_DISPOSAL

        partial interface IDatabase : IAsyncDisposable {}

        #endif

        public interface IDatabaseOpener
        {
            IDatabase Open();
        }

        public static class DatabaseExtensions
        {
            public static IDatabaseOpener WithFormatter(this Data.IDatabaseOpener opener, IFormatter formatter) =>
                new DatabaseOpener(() =>
                {
                    var db = opener.Open();
                    db.SetFormatter(formatter);
                    return db;
                });

            sealed class DatabaseOpener : IDatabaseOpener
            {
                readonly Func<Data.Database> _opener;

                public DatabaseOpener(Func<Data.Database> opener)
                {
                    Debug.Assert(opener != null);
                    _opener = opener;
                }

                public IDatabase Open() => _opener();
            }

            public static void SetFormatter(this Data.Database db, IFormatter formatter) =>
                db.Formatter = formatter;

            public static DbCommand Command(this IDatabase db, FormattableString commandText) =>
                db.Command(commandText, Data.Database.CommandOptions.Default);

            public static Task<dynamic> GetLastInsertIdAsync(this IDatabase db) =>
                db.GetLastInsertIdAsync(CancellationToken.None);
        }

        public interface IDatabase2<out T>
        {
            T GetResult(Data.Database db);
        }

        public static partial class Database
        {
            public static IDatabase2<T> Create<T>(Func<Data.Database, T> runner) =>
                new Delegating<T>(runner);

            sealed class Delegating<T> : IDatabase2<T>
            {
                readonly Func<Data.Database, T> _runner;

                public Delegating(Func<Data.Database, T> runner) =>
                    _runner = runner ?? throw new ArgumentNullException(nameof(runner));

                public T GetResult(Data.Database db) => _runner(db);
            }

            // GetResult

            public static IEnumerable<T>
                GetResult<T>(this IDatabase2<IEnumerable<T>> db, IDatabaseOpener dbo)
            {
                using var db2 = dbo.Open().Base;
                foreach (var item in db.GetResult(db2))
                    yield return item;
            }

            // Command

            public static IDatabase2<DbCommand> Command(FormattableString commandText) =>
                Command(commandText, Data.Database.CommandOptions.Default);

            public static IDatabase2<DbCommand> Command(FormattableString commandText,
                                                        Data.Database.CommandOptions options) =>
                Create(db => Data.Database.Command(db, commandText, commandText.GetArguments(), options));

            // Execute

            public static IDatabase2<int> Execute(FormattableString commandText) =>
                Execute(commandText, Data.Database.CommandOptions.Default);

            public static IDatabase2<int> Execute(FormattableString commandText,
                                                  Data.Database.CommandOptions options) =>
                Create(db => Data.Database.Execute(db, commandText, commandText.GetArguments(), options));

            // Execute (async)

            public static IDatabase2<Task<int>>
                ExecuteAsync(FormattableString commandText) =>
                ExecuteAsync(commandText, CancellationToken.None);

            public static IDatabase2<Task<int>>
                ExecuteAsync(FormattableString commandText, CancellationToken cancellationToken) =>
                ExecuteAsync(commandText, Data.Database.CommandOptions.Default, cancellationToken);

            public static IDatabase2<Task<int>>
                ExecuteAsync(FormattableString commandText, Data.Database.CommandOptions options) =>
                ExecuteAsync(commandText, options, CancellationToken.None);

            public static IDatabase2<Task<int>>
                ExecuteAsync(FormattableString commandText,
                             Data.Database.CommandOptions options,
                             CancellationToken cancellationToken) =>
                Create(db => Data.Database.ExecuteAsync(db, commandText, commandText.GetArguments(),
                                                        options, cancellationToken));

            // Query

            public static IDatabase2<IEnumerable<dynamic>>
                Query(FormattableString commandText) =>
                Query(commandText, Data.Database.QueryOptions.Default);

            public static IDatabase2<IEnumerable<dynamic>>
                Query(FormattableString commandText, Data.Database.QueryOptions options) =>
                Create(db => Data.Database.Query(db, commandText, commandText.GetArguments(), options));

            // QueryRecords

            public static IDatabase2<IEnumerable<IDataRecord>>
                QueryRecords(FormattableString commandText) =>
                QueryRecords(commandText, Data.Database.QueryOptions.Default);

            public static IDatabase2<IEnumerable<IDataRecord>>
                QueryRecords(FormattableString commandText,
                             Data.Database.QueryOptions options) =>
                Create(db => Data.Database.QueryRecords(db, commandText, commandText.GetArguments(), options));

            // QuerySingle

            public static IDatabase2<dynamic>
                QuerySingle(FormattableString commandText) =>
                QuerySingle(commandText, Data.Database.QueryOptions.Default);

            public static IDatabase2<dynamic>
                QuerySingle(FormattableString commandText, Data.Database.QueryOptions options) =>
                Create(db => Data.Database.QuerySingle(db, commandText, commandText.GetArguments(), options));

            // QueryValue

            public static IDatabase2<dynamic> QueryValue(FormattableString commandText) =>
                QueryValue(commandText, Data.Database.QueryOptions.Default);

            public static IDatabase2<dynamic>
                QueryValue(FormattableString commandText, Data.Database.QueryOptions options) =>
                Create(db => Data.Database.QueryValue(db, commandText, commandText.GetArguments(), options));

            public static IDatabase2<T> QueryValue<T>(FormattableString commandText) =>
                QueryValue<T>(commandText, Data.Database.QueryOptions.Default);

            public static IDatabase2<T> QueryValue<T>(FormattableString commandText,
                                                      Data.Database.QueryOptions options) =>
                Create(db => Data.Database.QueryValue<T>(db, commandText, commandText.GetArguments(), options));

            // QueryValue (async)

            public static IDatabase2<Task<dynamic>>
                QueryValueAsync(FormattableString commandText,
                                Data.Database.QueryOptions options,
                                CancellationToken cancellationToken) =>
                Create(db => Data.Database.QueryValueAsync(db, commandText, commandText.GetArguments(),
                                                           options, cancellationToken));

            public static IDatabase2<Task<T>>
                QueryValueAsync<T>(FormattableString commandText,
                                   Data.Database.QueryOptions options,
                                   CancellationToken cancellationToken) =>
                Create(db => Data.Database.QueryValueAsync<T>(db, commandText, commandText.GetArguments(),
                                                              options, cancellationToken));

            // GetLastInsertId + async

            public static IDatabase2<dynamic> GetLastInsertId() =>
                Create(db => db.GetLastInsertId());

            public static IDatabase2<Task<dynamic>> GetLastInsertIdAsync() =>
                Create(db => db.GetLastInsertIdAsync());
        }

        #if ASYNC_STREAMS

        partial class Database
        {
            // Query (async)

            public static IDatabase2<IAsyncEnumerable<dynamic>>
                QueryAsync(FormattableString commandText) =>
                QueryAsync(commandText, Data.Database.QueryOptions.Default);

            public static IDatabase2<IAsyncEnumerable<dynamic>>
                QueryAsync(FormattableString commandText,
                           Data.Database.QueryOptions options) =>
                Create(db => Data.Database.QueryAsync(db, commandText, commandText.GetArguments(), options));

            // QueryRecords (async)

            public static IDatabase2<IAsyncEnumerable<IDataRecord>>
                QueryRecordsAsync(FormattableString commandText) =>
                QueryRecordsAsync(commandText, Data.Database.QueryOptions.Default);

            public static IDatabase2<IAsyncEnumerable<IDataRecord>>
                QueryRecordsAsync(FormattableString commandText,
                                  Data.Database.QueryOptions options) =>
                Create(db => Data.Database.QueryRecordsAsync(db, commandText, commandText.GetArguments(), options));

            // QuerySingle (async)

            public static IDatabase2<Task<dynamic>>
                QuerySingleAsync(FormattableString commandText) =>
                QuerySingleAsync(commandText, CancellationToken.None);

            public static IDatabase2<Task<dynamic>>
                QuerySingleAsync(FormattableString commandText,
                                 CancellationToken cancellationToken) =>
                QuerySingleAsync(commandText, Data.Database.QueryOptions.Default, cancellationToken);

            public static IDatabase2<Task<dynamic>>
                QuerySingleAsync(FormattableString commandText,
                                 Data.Database.QueryOptions options) =>
                QuerySingleAsync(commandText, options, CancellationToken.None);

            public static IDatabase2<Task<dynamic>>
                QuerySingleAsync(FormattableString commandText,
                                 Data.Database.QueryOptions options,
                                 CancellationToken cancellationToken) =>
                Create(db => Data.Database.QuerySingleAsync(db, commandText, commandText.GetArguments(),
                                                            options, cancellationToken));

            // GetResult (async)

            public static async IAsyncEnumerable<T>
                GetResult<T>(this IDatabase2<IAsyncEnumerable<T>> db, IDatabaseOpener dbo,
                             [EnumeratorCancellation]CancellationToken cancellationToken = default)
            {
                var db2 = dbo.Open().Base;
                await using (db2.ConfigureAwait(false))
                {
                    await foreach (var item in db.GetResult(db2)
                                                 .ConfigureAwait(false)
                                                 .WithCancellation(cancellationToken))
                    {
                        yield return item;
                    }
                }
            }
        }

        #endif
    }
}

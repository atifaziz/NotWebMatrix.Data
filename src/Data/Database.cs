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
    using Eggado;
    using JetBrains.Annotations;

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
        static Func<string, ConnectionStringSettings> _namedConnectionStringResolver;

        public static event EventHandler<ConnectionEventArgs> ConnectionOpened;

        Database(Func<DbConnection> connectionFactory)
        {
            Debug.Assert(connectionFactory != null);
            _connectionFactory = connectionFactory;
        }

        public DbConnection Connection { get { return _connection ?? (_connection = _connectionFactory()); } }

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
            public TimeSpan? CommandTimeout { get; set; }
        }

        public DbCommand Command(string commandText, params object[] args)
        {
            return Command(null, commandText, args);
        }

        public DbCommand Command(CommandOptions options, string commandText, params object[] args)
        {
            if (string.IsNullOrEmpty(commandText)) throw Exceptions.ArgumentNullOrEmpty("commandText");

            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
                OnConnectionOpened();
            }

            var command = Connection.CreateCommand();
            command.CommandText = commandText;
            if (options != null && options.CommandTimeout != null)
                command.CommandTimeout = (int) options.CommandTimeout.Value.TotalSeconds;
            var parameters = CreateParameters(command.CreateParameter, args);
            command.Parameters.AddRange(parameters.ToArray());
            return command;
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
            var actor = value as Action<IDbDataParameter>;
            if (actor != null)
                actor(parameter);
            else
                parameter.Value = value ?? DBNull.Value;
            return parameter;
        }

        void OnConnectionOpened()
        {
            var handler = ConnectionOpened;
            if (handler == null)
                return;
            handler(this, new ConnectionEventArgs(Connection));
        }

        [Serializable]
        public class QueryOptions : CommandOptions
        {
            public bool Unbuffered { get; set; }
        }

        public IEnumerable<dynamic> Query(string commandText, params object[] args)
        {
            return Query(null, commandText, args);
        }

        public IEnumerable<dynamic> Query(QueryOptions options, string commandText, params object[] args)
        {
            return QueryImpl(options, commandText, args);
        }

        public dynamic QuerySingle(string commandText, params object[] args)
        {
            var options = new QueryOptions { Unbuffered = true };
            return QueryImpl(options, commandText, args).FirstOrDefault();
        }

        public IEnumerable<IDataRecord> QueryRecords(string commandText, params object[] args)
        {
            return QueryRecords(null, commandText, args);
        }

        public IEnumerable<IDataRecord> QueryRecords(QueryOptions options, string commandText, params object[] args)
        {
            return QueryImpl(options, commandText, args, r => r.SelectRecords());
        }

        IEnumerable<dynamic> QueryImpl(QueryOptions options, string commandText, object[] args)
        {
            return QueryImpl(options, commandText, args, r => r.Select());
        }
        
        IEnumerable<T> QueryImpl<T>(QueryOptions options, string commandText, object[] args, Func<IDataReader, IEnumerator<T>> selector)
        {
            Debug.Assert(selector != null);

            using (var command = Command(options, commandText, args))
            {
                var items = Eggnumerable.From(command.ExecuteReader, selector);
                if (options != null && !options.Unbuffered)
                    items = items.ToList().AsReadOnly();
                foreach (var item in items)
                    yield return item;
            }
        }

        public dynamic QueryValue(string commandText, params object[] args)
        {
            return QueryValue(null, commandText, args);
        }

        public dynamic QueryValue(CommandOptions options, string commandText, params object[] args)
        {
            using (var command = Command(options, commandText, args))
                return command.ExecuteScalar();
        }

        public T QueryValue<T>(string commandText, params object[] args)
        {
            return QueryValue<T>(null, commandText, args);
        }

        public T QueryValue<T>(CommandOptions options, string commandText, params object[] args)
        {
            var value = (object) QueryValue(options, commandText, args);

            if (Convert.IsDBNull(value))
                return (T) (object) null;

            var type = typeof(T);
            var conversionType = type.IsGenericType
                                 && !type.IsGenericTypeDefinition
                                 && typeof(Nullable<>) == type.GetGenericTypeDefinition()
                                 ? Nullable.GetUnderlyingType(type)
                                 : type;

            return (T) Convert.ChangeType(value, conversionType, CultureInfo.InvariantCulture);
        }

        public int Execute(string commandText, params object[] args) 
        {
            return Execute(null, commandText, args);
        }

        public int Execute(CommandOptions options, string commandText, params object[] args)
        {
            using (var command = Command(options, commandText, args))
                return command.ExecuteNonQuery();
        }

        public dynamic GetLastInsertId()
        {
            return QueryValue("SELECT @@Identity");
        }

        public static Database Open(string name)
        {
            if (string.IsNullOrEmpty(name)) throw Exceptions.ArgumentNullOrEmpty("name");
            return OpenNamedConnection(name);
        }

        public static Database OpenConnectionString(string connectionString)
        {
            return OpenConnectionString(connectionString, (DbProviderFactory) null);
        }

        public static Database OpenConnectionString(string connectionString, string providerName)
        {
            return OpenConnectionStringImpl(providerName, null, connectionString);
        }

        public static Database OpenConnectionString(string connectionString, DbProviderFactory providerFactory)
        {
            return OpenConnectionStringImpl(null, providerFactory, connectionString);
        }

        static Database OpenConnectionStringImpl(string providerName, DbProviderFactory providerFactory, string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString)) throw Exceptions.ArgumentNullOrEmpty("connectionString");

            var decorator = ConnectionDecorator ?? (connection => connection);

            if (providerFactory == null)
            {
                if (string.IsNullOrEmpty(providerName))
                    providerName = GetDefaultProviderName();
            }
            else
            {
                Debug.Assert(string.IsNullOrEmpty(providerName), @"Specify either provider factory or name but not both.");
            }

            return new Database(() =>
            {
                if (providerFactory == null)
                {
                    Debug.Assert(providerName != null);
                    providerFactory = DbProviderFactories.GetFactory(providerName);
                }

                var connection = providerFactory.CreateConnection();
                if (connection == null)
                    throw new NullReferenceException(string.Format("{0} returned a connection reference not set to an instance.", providerFactory.GetType()));

                connection.ConnectionString = connectionString;
                return decorator(connection);
            });
        }

        [NotNull]
        public static readonly Func<string, ConnectionStringSettings> DefaultNamedConnectionStringResolver = name => ConfigurationManager.ConnectionStrings[name];

        [NotNull] 
        public static Func<string, ConnectionStringSettings> NamedConnectionStringResolver
        {
            get { return _namedConnectionStringResolver ?? DefaultNamedConnectionStringResolver; }
            set
            {
                if (value == null) throw new ArgumentNullException("value");
                _namedConnectionStringResolver = value;
            }
        }

        static Database OpenNamedConnection(string name)
        {
            var connectionStringSettings = NamedConnectionStringResolver(name);
            if (connectionStringSettings == null)
            {
                var message = string.Format(@"Connection string ""{0}"" was not found.", name);
                throw new InvalidOperationException(message);
            }
            return OpenConnectionStringImpl(connectionStringSettings.ProviderName, null, connectionStringSettings.ConnectionString);
        }

        static string GetDefaultProviderName()
        {
            var value = ConfigurationManager.AppSettings["NotWebMatrix.Data.Database.DefaultProvider"];
            return !string.IsNullOrWhiteSpace(value) ? value : "System.Data.SqlServerCe.4.0";
        }

        sealed class DatabaseOpener : IDatabaseOpener
        {
            readonly Func<Database> _opener;

            public DatabaseOpener([NotNull] Func<Database> opener)
            {
                Debug.Assert(opener != null);
                _opener = opener;
            }

            public Database Open() { return _opener(); }
        }

        public static IDatabaseOpener Opener(string name)
        {
            return new DatabaseOpener(() => Open(name));
        }

        public static IDatabaseOpener ConnectionStringOpener(string connectionString)
        {
            return new DatabaseOpener(() => OpenConnectionString(connectionString));
        }

        public static IDatabaseOpener ConnectionStringOpener(string connectionString, string providerName)
        {
            return new DatabaseOpener(() => OpenConnectionString(connectionString, providerName));
        }

        public static IDatabaseOpener ConnectionStringOpener(string connectionString, DbProviderFactory providerFactory)
        {
            return new DatabaseOpener(() => OpenConnectionString(connectionString, providerFactory));
        }

        static class Exceptions
        {
            public static ArgumentException ArgumentNullOrEmpty(string paramName)
            {
                return new ArgumentException(@"Value cannot be null or an empty string.", paramName);
            }
        }
    }
}

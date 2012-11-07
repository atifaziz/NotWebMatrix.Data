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
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Threading;

    #endregion

    /// <summary>
    /// Provides methods and properties that are used to access and manage 
    /// data that is stored in a database.
    /// </summary>

    public class Database : IDisposable
    {
        static Func<string, ConnectionStringSettings> _namedConnectionStringResolver;

        Func<DbConnection> _connectionFactory;
        DbConnection _connection;

        public static event EventHandler<ConnectionEventArgs> ConnectionOpened;

        Database(Func<DbConnection> connectionFactory)
        {
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

        DbCommand CreateConnectedCommand(string commandText, params object[] args)
        {
            if (string.IsNullOrEmpty(commandText)) throw Exceptions.ArgumentNullOrEmpty("commandText");
        
            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
                OnConnectionOpened();
            }
            
            var command = Connection.CreateCommand();
            command.CommandText = commandText;
            
            if (args != null)
            {
                var parameters = args.Select((arg, index) =>
                {
                    DbParameter param = command.CreateParameter();
                    param.ParameterName = index.ToString(CultureInfo.InvariantCulture);
                    param.Value = arg ?? DBNull.Value;
                    return param;
                });
                command.Parameters.AddRange(parameters.ToArray());
            }
            
            return command;
        }

        void OnConnectionOpened()
        {
            var handler = ConnectionOpened;
            if (handler == null)
                return;
            handler(this, new ConnectionEventArgs(Connection));
        }

        public IEnumerable<dynamic> Query(string commandText, params object[] args)
        {
            return QueryImpl(commandText, args).ToList().AsReadOnly();
        }

        public dynamic QuerySingle(string commandText, params object[] args)
        {
            return QueryImpl(commandText, args).FirstOrDefault();
        }

        IEnumerable<DynamicRecord> QueryImpl(string commandText, params object[] args)
        {
            using (var command = CreateConnectedCommand(commandText, args))
            using (var reader = command.ExecuteReader())
            {
                var columns = Enumerable.Range(0, reader.FieldCount)
                                        .Select(i => reader.GetName(i))
                                        .ToList()
                                        .AsReadOnly();

                foreach (DbDataRecord record in reader)
                    yield return new DynamicRecord(columns, record);
            }
        }

        public dynamic QueryValue(string commandText, params object[] args)
        {
            using (var command = CreateConnectedCommand(commandText, args))
                return command.ExecuteScalar();
        }

        public int Execute(string commandText, params object[] args)
        {
            using (var command = CreateConnectedCommand(commandText, args))
                return command.ExecuteNonQuery();
        }

        public dynamic GetLastInsertId()
        {
            return QueryValue("SELECT @@Identity");
        }

        public static Database OpenConnectionString(string connectionString)
        {
            return Database.OpenConnectionString(connectionString, null);
        }

        public static Database OpenConnectionString(string connectionString, string providerName)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw Exceptions.ArgumentNullOrEmpty("connectionString");
            return Database.OpenConnectionStringInternal(providerName, connectionString);
        }

        public static Database Open(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw Exceptions.ArgumentNullOrEmpty("name");
            return Database.OpenNamedConnection(name);
        }

        static Database OpenConnectionStringInternal(string providerName, string connectionString)
        {
            if (string.IsNullOrEmpty(providerName))
                providerName = GetDefaultProviderName();

            DbProviderFactory providerFactory = null;

            return new Database(() =>
            {
                if (providerFactory == null)
                    providerFactory = DbProviderFactories.GetFactory(providerName);

                var connection = providerFactory.CreateConnection();
                if (connection == null)
                    throw new NullReferenceException(string.Format("{0} returned a connection reference not set to an instance.", providerFactory.GetType()));

                connection.ConnectionString = connectionString;
                return connection;
            });
        }

        public static readonly Func<string, ConnectionStringSettings> DefaultNamedConnectionStringResolver = name => ConfigurationManager.ConnectionStrings[name];

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
            return OpenConnectionStringInternal(connectionStringSettings.ProviderName, connectionStringSettings.ConnectionString);
        }

        static string GetDefaultProviderName()
        {
            var value = ConfigurationManager.AppSettings["NotWebMatrix.Data.Database.DefaultProvider"];
            return !string.IsNullOrWhiteSpace(value) ? value : "System.Data.SqlServerCe.4.0";
        }

        static class Exceptions
        {
            public static ArgumentException ArgumentNullOrEmpty(string paramName)
            {
                return new ArgumentException(@"Value cannot be null or an empty string.");
            }
        }
    }
}

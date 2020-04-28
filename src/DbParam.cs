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
    using System.Data;
    using System.Linq;

    #endregion

    public static class DbParam
    {
        static readonly Action<IDbDataParameter> Nop = delegate {};

        public static Action<IDbDataParameter> DbType(DbType value)  => p => p.DbType = value;
        public static Action<IDbDataParameter> Size(int value) => p  => p.Size = value;
        public static Action<IDbDataParameter> Precision(byte value) => p => p.Precision = value;
        public static Action<IDbDataParameter> Scale(byte value)     => p => p.Scale = value;
        public static Action<IDbDataParameter> Value(object value)   => p => p.Value = value;

        public static Action<IDbDataParameter> Name(string value) =>
            !string.IsNullOrEmpty(value) ? (p => p.ParameterName = value) : Nop;

        public static Action<IDbDataParameter> Value(DbType dbType, object value) => Value(null, dbType, value);
        public static Action<IDbDataParameter> Value(string name, object value)   => Value(name, null, value);

        public static Action<IDbDataParameter> Value(string name, DbType? dbType, object value) =>
            Name(name)
            + (dbType != null ? DbType(dbType.Value) : null)
            + Value(value);

        public static Action<IDbDataParameter> AnsiString(string value)              => AnsiString(null, value);
        public static Action<IDbDataParameter> AnsiString(string name, string value) => AnsiString(name, value, null);
        public static Action<IDbDataParameter> AnsiString(string value, int size)    => AnsiString(null, value, size);

        public static Action<IDbDataParameter> AnsiString(string name, string value, int? size) =>
            DbType(System.Data.DbType.AnsiString)
            + Value(name, value)
            + (size != null ? Size(size.Value) : null);

        public static Action<IDbDataParameter> Specific<T>(Action<T> action)
            where T : IDbDataParameter
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            return p => action((T)p);
        }

        public static Action<IDbDataParameter> Specific<T>(params Action<T>[] actions)
            where T : IDbDataParameter
        {
            if (actions == null) throw new ArgumentNullException(nameof(actions));

            actions = actions.Length > 0
                    ? actions
                    : new Action<T>[] { delegate {} };

            return Specific(actions.Aggregate((acc, a) => acc + a));
        }
    }
}

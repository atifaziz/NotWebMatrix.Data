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
    using System.Data;
    using System.Linq;

    #endregion

    public static class DbParam
    {
        public static Action<IDbDataParameter> DbType(DbType value)  { return p => p.DbType = value;    }
        public static Action<IDbDataParameter> Size(int value)       { return p => p.Size = value;      }
        public static Action<IDbDataParameter> Precision(byte value) { return p => p.Precision = value; }
        public static Action<IDbDataParameter> Scale(byte value)     { return p => p.Scale = value;     }
        public static Action<IDbDataParameter> Value(object value)   { return p => p.Value = value;     }

        public static Action<IDbDataParameter> AnsiString(string value)
        {
            return AnsiString(value, null);
        }

        public static Action<IDbDataParameter> AnsiString(string value, int? size)
        {
            return DbType(System.Data.DbType.AnsiString)
                 + Value(value)
                 + (size != null ? Size(size.Value) : null);
        }

        public static Action<IDbDataParameter> Specific<T>(Action<T> action) 
            where T : IDbDataParameter
        {
            if (action == null) throw new ArgumentNullException("action");
            return p => action((T) p);
        }

        public static Action<IDbDataParameter> Specific<T>(params Action<T>[] actions) 
            where T : IDbDataParameter 
        {  
            if (actions == null) throw new ArgumentNullException("actions");

            actions = actions.Length > 0 
                    ? actions 
                    : new Action<T>[] { delegate {} };

            return Specific(actions.Aggregate((acc, a) => acc += a));
        }
    }
}

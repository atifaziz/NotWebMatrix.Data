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

#nullable enable

namespace NotWebMatrix.Data.Experimental
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Text;

    public static class Sql
    {
        public static (string, List<DbParameter>)
            Format(IFormatter formatter, FormattableString fs,
                   Func<object, DbParameter> parameterFactory)
        {
            if (formatter == null) throw new ArgumentNullException(nameof(formatter));
            if (fs == null) throw new ArgumentNullException(nameof(fs));

            var parameters = new List<DbParameter>();
            var text = Format(fs);
            return (text, parameters);

            string Format(FormattableString fs)
            {
                var args = new object[fs.ArgumentCount];
                for (var i = 0; i < fs.ArgumentCount; i++)
                {
                    switch (fs.GetArgument(i))
                    {
                        case SqlFormat f:
                        {
                            args[i] = Format(f.FormattableString);
                            break;
                        }
                        case SqlList list:
                        {
                            var sb = new StringBuilder();
                            if (list.Values.Count > 0)
                                sb.Append(list.Before);
                            foreach (var (j, arg) in list.Values.Select((e, i) => (i, e)))
                            {
                                if (j > 0)
                                    sb.Append(list.Separator);
                                var parameter = parameterFactory(arg);
                                parameters.Add(parameter);
                                var name = formatter.Named(parameter.ParameterName);
                                sb.Append(name);
                            }
                            if (list.Values.Count > 0)
                                sb.Append(list.After);
                            args[i] = sb.ToString();
                            break;
                        }
                        case var arg:
                        {
                            var parameter = parameterFactory(arg);
                            parameters.Add(parameter);
                            //var name = formatter.Anonymous(anonymousIndex++);
                            var name = formatter.Named(parameter.ParameterName);
                            args[i] = name;
                            break;
                        }
                    };
                }

                return string.Format(fs.Format, args);
            }
        }
    }

    public interface ISqlFormatArgument {}

    public sealed class SqlList : ISqlFormatArgument
    {
        public string? Before    { get; }
        public string? After     { get; }
        public string  Separator { get; }
        public IReadOnlyCollection<object> Values { get; }

        public SqlList(string separator, IEnumerable<object> values, string? before = null, string? after = null) :
            this(before, after, separator, values.ToArray()) {}

        public SqlList(string separator, params object[] values) :
            this(null, null, separator, values) {}

        public SqlList(string? before, string? after, string separator, params object[] values) =>
            (Before, After, Separator, Values) = (before, after, separator, Array.AsReadOnly(values));
    }

    public sealed class SqlFormat : ISqlFormatArgument
    {
        public readonly FormattableString FormattableString;

        public SqlFormat(FormattableString formattableString) =>
            FormattableString = formattableString;
    }

    public interface IFormatter
    {
        string Named(string name);
        string Anonymous(int index);
        string Literal(object value);
    }

    public sealed class TSqlFormatter : IFormatter
    {
        public static readonly IFormatter Instance = new TSqlFormatter();

        public StringComparer NameComparer => StringComparer.OrdinalIgnoreCase;

        public string Named(string name) => "@" + name;

        public string Anonymous(int index) => "@" + index.ToString(CultureInfo.InvariantCulture);

        public string Literal(object value) => value switch
        {
            null => "NULL",
            int v => v.ToString(CultureInfo.InvariantCulture),
            string s => "'" + s.Replace("'", "''") + "'",
            _ => throw new NotSupportedException("Unsupported type: " + value.GetType())
        };
    }
}

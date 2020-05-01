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
    using System.Collections.ObjectModel;
    using System.Data.Common;
    using System.Globalization;
    using System.Linq;
    using System.Text;

    public interface ISqlFormatter
    {
        (string CommandText, IReadOnlyList<DbParameter> Parameters)
            Format(FormattableString fs, Func<object, DbParameter> parameterFactory);
    }

    static class SqlFormatter
    {
        public static readonly ISqlFormatter TSql = Create('@', null);

        public static ISqlFormatter Create(char parameterNameStartToken) =>
            Create(parameterNameStartToken, null);

        public static ISqlFormatter Create(char parameterNameStartToken,
                                           string? anonymousParameterPrefix) =>
            new Formatter(parameterNameStartToken, anonymousParameterPrefix);

        sealed class Formatter : ISqlFormatter
        {
            readonly char _anonymousParameterPrefix;
            readonly string _anonymousPrefix;

            public Formatter(char anonymousParameterPrefix, string? anonymousPrefix)
            {
                _anonymousParameterPrefix = anonymousParameterPrefix;
                _anonymousPrefix = anonymousPrefix ?? string.Empty;
            }

            public (string CommandText, IReadOnlyList<DbParameter> Parameters)
                Format(FormattableString fs, Func<object, DbParameter> parameterFactory) =>
                SqlFormatter.Format(_anonymousParameterPrefix, _anonymousPrefix, fs, parameterFactory);
        }

        static (string, IReadOnlyList<DbParameter>)
            Format(char parameterNameStartToken, string anonymousParameterPrefix,
                   FormattableString fs, Func<object, DbParameter> parameterFactory)
        {
            if (fs == null) throw new ArgumentNullException(nameof(fs));
            if (parameterFactory == null) throw new ArgumentNullException(nameof(parameterFactory));

            var parameters = new List<DbParameter>();
            var anonymousCount = 0;
            var text = Format(fs);
            IReadOnlyList<DbParameter> roParameters = new ReadOnlyCollection<DbParameter>(parameters);
            return (text, roParameters);

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
                                if (string.IsNullOrEmpty(parameter.ParameterName))
                                    parameter.ParameterName = anonymousParameterPrefix + anonymousCount++.ToString(CultureInfo.InvariantCulture);
                                parameters.Add(parameter);
                                sb.Append(parameterNameStartToken);
                                sb.Append(parameter.ParameterName);
                            }
                            if (list.Values.Count > 0)
                                sb.Append(list.After);
                            args[i] = sb.ToString();
                            break;
                        }
                        case var arg:
                        {
                            var parameter = parameterFactory(arg);
                            if (string.IsNullOrEmpty(parameter.ParameterName))
                                parameter.ParameterName = anonymousParameterPrefix + anonymousCount++.ToString(CultureInfo.InvariantCulture);
                            parameters.Add(parameter);
                            args[i] = parameterNameStartToken + parameter.ParameterName;
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
}

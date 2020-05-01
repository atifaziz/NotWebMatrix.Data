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

    public interface IInterpolatedSqlFormatter
    {
        (string CommandText, IReadOnlyList<DbParameter> Parameters)
            Format(FormattableString formattableString, Func<object, DbParameter> parameterFactory);
    }

    public static class InterpolatedSql
    {
        public static readonly IInterpolatedSqlFormatter TSqlFormatter = CreateFormatter('@', null);

        public static IInterpolatedSqlFormatter CreateFormatter(char parameterSigil) =>
            CreateFormatter(parameterSigil, null);

        public static IInterpolatedSqlFormatter
            CreateFormatter(char parameterSigil, string? anonymousParameterPrefix) =>
            new Formatter(parameterSigil, anonymousParameterPrefix);

        sealed class Formatter : IInterpolatedSqlFormatter
        {
            readonly char _parameterSigil;
            readonly string _anonymousParameterPrefix;

            public Formatter(char parameterSigil, string? anonymousParameterPrefix)
            {
                _parameterSigil = parameterSigil;
                _anonymousParameterPrefix = anonymousParameterPrefix ?? string.Empty;
            }

            public (string CommandText, IReadOnlyList<DbParameter> Parameters)
                Format(FormattableString fs, Func<object, DbParameter> parameterFactory) =>
                InterpolatedSql.Format(_parameterSigil, _anonymousParameterPrefix, fs, parameterFactory);
        }

        static (string, IReadOnlyList<DbParameter>)
            Format(char parameterSigil, string anonymousParameterPrefix,
                   FormattableString fs, Func<object, DbParameter> parameterFactory)
        {
            if (fs == null) throw new ArgumentNullException(nameof(fs));
            if (parameterFactory == null) throw new ArgumentNullException(nameof(parameterFactory));

            var parameters = new List<DbParameter>();
            var anonymousCount = 0;
            StringBuilder? sb = null;
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
                        case InlinePartial f:
                        {
                            args[i] = Format(f.FormattableString);
                            break;
                        }
                        case ListPartial list:
                        {
                            sb ??= new StringBuilder();
                            if (list.Values.Count > 0)
                                sb.Append(list.Before);
                            foreach (var (j, arg) in list.Values.Select((e, i) => (i, e)))
                            {
                                if (j > 0)
                                    sb.Append(list.Separator);
                                var parameter = CreateParameter(arg);
                                sb.Append(parameterSigil);
                                sb.Append(parameter.ParameterName);
                            }
                            if (list.Values.Count > 0)
                                sb.Append(list.After);
                            args[i] = sb.ToString();
                            sb.Clear();
                            break;
                        }
                        case var arg:
                        {
                            var parameter = CreateParameter(arg);
                            args[i] = parameterSigil + parameter.ParameterName;
                            break;
                        }
                    };
                }

                return string.Format(fs.Format, args);

                DbParameter CreateParameter(object value)
                {
                    var parameter = parameterFactory(value);
                    if (string.IsNullOrEmpty(parameter.ParameterName))
                        parameter.ParameterName = anonymousParameterPrefix + anonymousCount++.ToString(CultureInfo.InvariantCulture);
                    parameters.Add(parameter);
                    return parameter;
                }
            }
        }

        public static IPartial List(string separator, IEnumerable<object> values, string? before = null, string? after = null) =>
            new ListPartial(before, after, separator, values.ToArray());

        public static IPartial List(string separator, params object[] values) =>
            new ListPartial(null, null, separator, values);

        public static IPartial List(string? before, string? after, string separator, params object[] values) =>
            new ListPartial(before, after, separator, values);

        public static IPartial Inline(FormattableString formattableString) =>
            new InlinePartial(formattableString);

        public interface IPartial {}

        sealed class ListPartial : IPartial
        {
            public string? Before    { get; }
            public string? After     { get; }
            public string  Separator { get; }
            public IReadOnlyList<object> Values { get; }

            public ListPartial(string? before, string? after, string separator, params object[] values) =>
                (Before, After, Separator, Values) = (before, after, separator, values);
        }

        sealed class InlinePartial : IPartial
        {
            public readonly FormattableString FormattableString;

            public InlinePartial(FormattableString formattableString) =>
                FormattableString = formattableString;
        }
    }
}

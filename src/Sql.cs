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

    public sealed class CommandTextArguments
    {
        public string CommandText { get; }
        public IReadOnlyCollection<object> Arguments { get; }

        public CommandTextArguments(string commandText, object[] arguments)
        {
            CommandText = commandText;
            Arguments = arguments;
        }
    }

    public static class Sql
    {
        public static DbCommand FormatCommand(this Database db, IFormatter formatter, FormattableString fs)
        {
            var command = db.Connection.CreateCommand();
            command.FormatCommand(formatter, fs);
            return command;
        }

        public static void FormatCommand(this DbCommand command, IFormatter formatter, FormattableString fs)
        {
            if (command == null) throw new ArgumentNullException(nameof(command));
            if (formatter == null) throw new ArgumentNullException(nameof(formatter));
            if (fs == null) throw new ArgumentNullException(nameof(fs));

            var names = new Dictionary<string, DbParameter>(StringComparer.OrdinalIgnoreCase);
            var anonymousIndex = 0;
            var parameters = new List<DbParameter>();
            var text = Format(fs);
            command.Parameters.Clear();
            foreach (var parameter in parameters)
                command.Parameters.Add(parameter);
            command.CommandText = text;

            void AddParameter(DbParameter parameter)
            {
                parameters.Add(parameter);
                names.Add(parameter.ParameterName, parameter);
            }

            string Format(FormattableString fs)
            {
                var args = new object[fs.ArgumentCount];
                for (var i = 0; i < fs.ArgumentCount; i++)
                {
                    switch (fs.GetArgument(i))
                    {
                        case SqlLiteral literal:
                        {
                            args[i] = formatter.Literal(literal.Value);
                            break;
                        }
                        case SqlNamed named:
                        {
                            if (names.TryGetValue(named.Name, out var parameter))
                            {
                                if (named.Value != parameter.Value)
                                    throw new Exception($"Conflicting values supplied for parameter \"{named.Name}\".");
                            }
                            else
                            {
                                var name = formatter.Named(named.Name);
                                parameter = command.CreateParameter();
                                parameter.ParameterName = name;
                                parameter.Value = named.Value;
                                AddParameter(parameter);
                            }
                            args[i] = parameter.ParameterName;
                            break;
                        }
                        case SqlRef reference:
                        {
                            args[i] = formatter.Named(reference.Name);
                            break;
                        }
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
                            foreach (var (j, value) in list.Values.Select((e, i) => (i, e)))
                            {
                                if (j > 0)
                                    sb.Append(list.Separator);
                                var parameter = command.CreateParameter();
                                parameter.ParameterName = formatter.Named(string.Format(CultureInfo.InvariantCulture, list.Naming, j));
                                sb.Append(parameter.ParameterName);
                                parameter.Value = value;
                                AddParameter(parameter);
                            }
                            if (list.Values.Count > 0)
                                sb.Append(list.After);
                            args[i] = sb.ToString();
                            break;
                        }
                        case var arg:
                        {
                            var name = formatter.Anonymous(anonymousIndex++);
                            args[i] = name;
                            var parameter = command.CreateParameter();
                            parameter.ParameterName = name;
                            parameter.Value = arg;
                            command.Parameters.Add(parameter);
                            break;
                        }
                    };
                }

                return string.Format(fs.Format, args);
            }
        }
    }

    public interface ISqlFormatArgument {}

    public sealed class SqlLiteral : ISqlFormatArgument
    {
        public object Value { get; }

        public SqlLiteral(object value) =>
            Value = value;
    }

    public sealed class SqlRef : ISqlFormatArgument
    {
        public string Name { get; }

        public SqlRef(string name) =>
            Name = name;
    }

    public sealed class SqlNamed : ISqlFormatArgument
    {
        public string Name  { get; }
        public object Value { get; }

        public SqlNamed(string name, object value) =>
            (Name, Value) = (name, value);
    }

    public sealed class SqlList : ISqlFormatArgument
    {
        public string  Naming    { get; }
        public string? Before    { get; }
        public string? After     { get; }
        public string  Separator { get; }
        public IReadOnlyCollection<object> Values { get; }

        public SqlList(string naming, string separator, IEnumerable<object> values, string? before = null, string? after = null) :
            this(naming, before, after, separator, values.ToArray()) {}

        public SqlList(string naming, string separator, params object[] values) :
            this(naming, null, null, separator, values) {}

        public SqlList(string naming, string? before, string? after, string separator, params object[] values) =>
            (Naming, Before, After, Separator, Values) = (naming, before, after, separator, Array.AsReadOnly(values));
    }

    public sealed class SqlFormat : ISqlFormatArgument
    {
        public readonly FormattableString FormattableString;

        public SqlFormat(FormattableString formattableString) =>
            FormattableString = formattableString;
    }

    public interface IFormatter
    {
        StringComparer NameComparer { get; }
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

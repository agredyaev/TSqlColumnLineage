using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TSqlColumnLineage.Core.Parsing
{
    /// <summary>
    /// Exception thrown when SQL parsing errors occur
    /// </summary>
    public class SqlParsingException : Exception
    {
        /// <summary>
        /// List of parsing errors
        /// </summary>
        public IReadOnlyList<ParseError> Errors { get; }

        /// <summary>
        /// SQL query that caused the error
        /// </summary>
        public string SqlQuery { get; }

        public SqlParsingException(string message, IList<ParseError> errors, string sqlQuery)
            : base(FormatMessage(message, errors))
        {
            Errors = errors.ToList().AsReadOnly();
            SqlQuery = sqlQuery;
        }

        private static string FormatMessage(string message, IList<ParseError> errors)
        {
            if (errors == null || !errors.Any())
                return message;

            var errorMessages = string.Join(Environment.NewLine,
                errors.Select(e => $"Line {e.Line}, Column {e.Column}: {e.Message}"));

            return $"{message}{Environment.NewLine}{errorMessages}";
        }
    }
}

using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace TSqlColumnLineage.Core.Parsing.Exceptions
{
    /// <summary>
    /// Exception thrown when SQL parsing errors occur
    /// </summary>
    [Serializable]
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
        
        /// <summary>
        /// Position of the first error
        /// </summary>
        public (int Line, int Column)? ErrorPosition =>
            Errors.Count > 0 ? (Errors[0].Line, Errors[0].Column) : null;

        /// <summary>
        /// Creates a new SQL parsing exception
        /// </summary>
        /// <param name="message">Error message</param>
        /// <param name="errors">List of parse errors</param>
        /// <param name="sqlQuery">SQL query that caused the error</param>
        public SqlParsingException(string message, IList<ParseError> errors, string sqlQuery)
            : base(FormatMessage(message, errors))
        {
            Errors = errors?.ToList().AsReadOnly() ?? new List<ParseError>().AsReadOnly();
            SqlQuery = sqlQuery;
        }
        
        /// <summary>
        /// Creates a new SQL parsing exception with an inner exception
        /// </summary>
        /// <param name="message">Error message</param>
        /// <param name="errors">List of parse errors</param>
        /// <param name="sqlQuery">SQL query that caused the error</param>
        /// <param name="innerException">The inner exception</param>
        public SqlParsingException(string message, IList<ParseError> errors, string sqlQuery, Exception innerException)
            : base(FormatMessage(message, errors), innerException)
        {
            Errors = errors?.ToList().AsReadOnly() ?? new List<ParseError>().AsReadOnly();
            SqlQuery = sqlQuery;
        }
        
        /// <summary>
        /// Serialization constructor
        /// </summary>
        protected SqlParsingException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            SqlQuery = info.GetString(nameof(SqlQuery));
            // Note: Errors can't be directly deserialized this way, but we can create an empty list
            Errors = new List<ParseError>().AsReadOnly();
        }
        
        /// <summary>
        /// Provides data for serialization
        /// </summary>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null) throw new ArgumentNullException(nameof(info));
            
            info.AddValue(nameof(SqlQuery), SqlQuery);
            base.GetObjectData(info, context);
        }

        /// <summary>
        /// Gets the SQL fragment around the error position
        /// </summary>
        /// <param name="contextLength">Number of characters before and after the error</param>
        /// <returns>SQL fragment with error marker</returns>
        public string GetErrorContext(int contextLength = 20)
        {
            if (string.IsNullOrEmpty(SqlQuery) || Errors.Count == 0)
                return string.Empty;
                
            var error = Errors[0];
            
            // Split SQL by lines
            var lines = SqlQuery.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.None);
            if (error.Line <= 0 || error.Line > lines.Length)
                return string.Empty;
                
            var line = lines[error.Line - 1];
            if (error.Column <= 0 || error.Column > line.Length + 1)  // +1 because column might be at end
                return string.Empty;
                
            // Get context before and after the error
            int startPos = Math.Max(0, error.Column - 1 - contextLength);
            int length = Math.Min(line.Length - startPos, contextLength * 2);
            
            var context = line.Substring(startPos, length);
            
            // Create a marker pointing to the error position
            int markerPos = error.Column - 1 - startPos;
            if (markerPos >= 0 && markerPos <= context.Length)
            {
                var marker = new string(' ', markerPos) + "^";
                return context + Environment.NewLine + marker;
            }
            
            return context;
        }

        /// <summary>
        /// Formats the error message with details about parsing errors
        /// </summary>
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
using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace TSqlColumnLineage.Core.Engine.Parsing.Models
{
    /// <summary>
    /// Configuration options for SQL parsing using data-oriented design principles.
    /// </summary>
    public class ParsingOptions
    {
        /// <summary>
        /// Gets or sets whether to use quoted identifiers
        /// </summary>
        public bool UseQuotedIdentifiers { get; set; } = true;

        /// <summary>
        /// Gets or sets the SQL Server compatibility level
        /// </summary>
        public int CompatibilityLevel { get; set; } = 150; // SQL Server 2019

        /// <summary>
        /// Gets or sets the collation options
        /// </summary>
        public SqlCollationOptions CollationOptions { get; set; } = SqlCollationOptions.None;

        /// <summary>
        /// Gets or sets the batch separator type
        /// </summary>
        public BatchSeparatorType BatchSeparatorType { get; set; } = BatchSeparatorType.StandardSeparator;

        /// <summary>
        /// Gets or sets identity insert behavior
        /// </summary>
        public bool IdentityInserts { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to extract column references during parsing
        /// </summary>
        public bool ExtractColumnReferences { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to extract table references during parsing
        /// </summary>
        public bool ExtractTableReferences { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to include comments in parsing
        /// </summary>
        public bool IncludeComments { get; set; } = false;

        /// <summary>
        /// Gets or sets whether to include whitespace in parsing
        /// </summary>
        public bool IncludeWhitespace { get; set; } = false;

        /// <summary>
        /// Gets or sets the maximum depth for processing nested queries
        /// </summary>
        public int MaxNestedQueryDepth { get; set; } = 32;

        /// <summary>
        /// Gets or sets the maximum fragment size in characters
        /// </summary>
        public int MaxFragmentSize { get; set; } = 10000;

        /// <summary>
        /// Gets or sets whether to track source positions
        /// </summary>
        public bool TrackSourcePositions { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to extract expressions for column references
        /// </summary>
        public bool ExtractExpressions { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to extract object references
        /// </summary>
        public bool ExtractObjectReferences { get; set; } = true;
    }
    
    /// <summary>
    /// Types of SQL fragments
    /// </summary>
    public enum SqlFragmentType
    {
        Unknown,
        Batch,
        Select,
        Insert,
        Update,
        Delete,
        Merge,
        Create,
        Alter,
        Drop,
        Declare,
        Set,
        If,
        While,
        Begin,
        Commit,
        Rollback,
        Try,
        Catch,
        Execute,
        With,
        Union,
        Intersect,
        Except,
        Expression,
        Subquery,
        CTE,
        Join,
        TableValueConstructor,
        Case,
        Function,
        Procedure,
        Trigger,
        View,
        Index,
        Constraint,
        Comment,
        Whitespace,
        Other
    }

    /// <summary>
    /// Types of table references
    /// </summary>
    public enum TableReferenceType
    {
        Table,
        View,
        Function,
        TemporaryTable,
        CTE,
        Derived,
        Alias,
        Variable
    }
    
    /// <summary>
    /// Types of column references
    /// </summary>
    public enum ColumnReferenceType
    {
        Regular,
        Wildcard,
        Expression,
        Alias,
        Variable,
        Parameter
    }

    /// <summary>
    /// SQL collation options
    /// </summary>
    public enum SqlCollationOptions
    {
        None = 0,
        IgnoreCase = 1,
        IgnoreAccent = 2,
        IgnoreKanaType = 4,
        IgnoreWidth = 8
    }

    /// <summary>
    /// Batch separator types
    /// </summary>
    public enum BatchSeparatorType
    {
        StandardSeparator = 0,
        GoBatchTerminator = 1,
        Custom = 2
    }

    /// <summary>
    /// Represents a parsed SQL fragment optimized for lineage analysis.
    /// Implements data-oriented design for efficient memory usage.
    /// </summary>
    public class SqlFragment
    {
        /// <summary>
        /// Gets or sets the fragment type
        /// </summary>
        public SqlFragmentType FragmentType { get; set; }

        /// <summary>
        /// Gets or sets the original SQL text for this fragment
        /// </summary>
        public string SqlText { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the start position in the original script
        /// </summary>
        public int StartOffset { get; set; }

        /// <summary>
        /// Gets or sets the end position in the original script
        /// </summary>
        public int EndOffset { get; set; }

        /// <summary>
        /// Gets or sets the line number in the original script
        /// </summary>
        public int LineNumber { get; set; }

        /// <summary>
        /// Gets or sets the column number in the original script
        /// </summary>
        public int ColumnNumber { get; set; }

        /// <summary>
        /// Gets or sets the statement type this fragment belongs to
        /// </summary>
        public string Statement { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the parsed fragment
        /// </summary>
        public required TSqlFragment ParsedFragment { get; set; }

        /// <summary>
        /// Gets or sets the parent batch fragment, if any
        /// </summary>
        public required SqlFragment ParentBatch { get; set; }

        /// <summary>
        /// Gets or sets the table references in this fragment
        /// </summary>
        public List<TableReference> TableReferences { get; set; } = [];

        /// <summary>
        /// Gets or sets the column references in this fragment
        /// </summary>
        public List<ColumnReference> ColumnReferences { get; set; } = [];

        /// <summary>
        /// Gets or sets the child fragments
        /// </summary>
        public List<SqlFragment> Children { get; set; } = [];

        /// <summary>
        /// Gets or sets custom metadata for this fragment
        /// </summary>
        public Dictionary<string, object> Metadata { get; set; } = [];

        /// <summary>
        /// Returns a string representation of this fragment
        /// </summary>
        public override string ToString()
        {
            return $"{FragmentType} at line {LineNumber}: {TruncateText(SqlText, 50)}";
        }

        /// <summary>
        /// Truncates text for display
        /// </summary>
        private static string TruncateText(string text, int maxLength)
        {
            if (string.IsNullOrEmpty(text))
                return string.Empty;

            if (text.Length <= maxLength)
                return text;

            return text[..maxLength] + "...";
        }
    }

    /// <summary>
    /// Represents a SQL table reference
    /// </summary>
    public class TableReference
    {
        /// <summary>
        /// Gets or sets the database name
        /// </summary>
        public string DatabaseName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the schema name
        /// </summary>
        public string SchemaName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the table name
        /// </summary>
        public string TableName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the table alias
        /// </summary>
        public string Alias { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the type of table reference
        /// </summary>
        public TableReferenceType ReferenceType { get; set; }

        /// <summary>
        /// Gets or sets the start position in the original script
        /// </summary>
        public int StartOffset { get; set; }

        /// <summary>
        /// Gets or sets the end position in the original script
        /// </summary>
        public int EndOffset { get; set; }

        /// <summary>
        /// Returns the full name of the table
        /// </summary>
        public string GetFullName()
        {
            if (!string.IsNullOrEmpty(DatabaseName) && !string.IsNullOrEmpty(SchemaName))
                return $"{DatabaseName}.{SchemaName}.{TableName}";

            if (!string.IsNullOrEmpty(SchemaName))
                return $"{SchemaName}.{TableName}";

            return TableName;
        }

        /// <summary>
        /// Returns a string representation of this table reference
        /// </summary>
        public override string ToString()
        {
            string name = GetFullName();

            if (!string.IsNullOrEmpty(Alias))
                return $"{name} AS {Alias}";

            return name;
        }
    }

    /// <summary>
    /// Represents a SQL column reference
    /// </summary>
    public class ColumnReference
    {
        /// <summary>
        /// Gets or sets the server name
        /// </summary>
        public string ServerName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the database name
        /// </summary>
        public string DatabaseName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the schema name
        /// </summary>
        public string SchemaName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the table name
        /// </summary>
        public string TableName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the column name
        /// </summary>
        public string ColumnName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the alias
        /// </summary>
        public string Alias { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets whether this is a source column
        /// </summary>
        public bool IsSource { get; set; }

        /// <summary>
        /// Gets or sets whether this is a target column
        /// </summary>
        public bool IsTarget { get; set; }

        /// <summary>
        /// Gets or sets the type of column reference
        /// </summary>
        public ColumnReferenceType ReferenceType { get; set; }

        /// <summary>
        /// Gets or sets the start position in the original script
        /// </summary>
        public int StartOffset { get; set; }

        /// <summary>
        /// Gets or sets the end position in the original script
        /// </summary>
        public int EndOffset { get; set; }

        /// <summary>
        /// Gets or sets the original expression (if any)
        /// </summary>
        public string Expression { get; set; } = string.Empty;

        /// <summary>
        /// Returns the full name of the column
        /// </summary>
        public string GetFullName()
        {
            if (!string.IsNullOrEmpty(TableName))
                return $"{TableName}.{ColumnName}";

            return ColumnName;
        }

        /// <summary>
        /// Returns a string representation of this column reference
        /// </summary>
        public override string ToString()
        {
            string name = GetFullName();

            if (!string.IsNullOrEmpty(Alias))
                return $"{name} AS {Alias}";

            return name;
        }
    }
}
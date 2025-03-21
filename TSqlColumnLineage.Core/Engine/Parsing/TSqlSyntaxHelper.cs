using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Engine.Parsing.Models;
using TSqlColumnLineage.Core.Infrastructure.Memory;


namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// Provides utilities for working with T-SQL syntax specialties.
    /// Optimized for memory efficiency using data-oriented design.
    /// </summary>
    public static class TSqlSyntaxHelper
    {
        // Regex patterns for common T-SQL syntax elements
        private static readonly Regex _tableNamePattern = new(@"(?:(?:\[([^\]]+)\])|([a-zA-Z0-9_]+))(?:\.(?:\[([^\]]+)\])|\.([a-zA-Z0-9_]+))?", RegexOptions.Compiled);
        private static readonly Regex _columnNamePattern = new(@"(?:(?:\[([^\]]+)\])|([a-zA-Z0-9_]+))\.(?:(?:\[([^\]]+)\])|([a-zA-Z0-9_]+))", RegexOptions.Compiled);
        private static readonly Regex _aliasPattern = new(@"(?:AS\s+)?(?:\[([^\]]+)\]|([a-zA-Z0-9_]+))", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        
        // Cache for commonly used syntax objects
        private static readonly Dictionary<string, SqlFragmentType> _statementTypeMap = InitializeStatementTypeMap();

        /// <summary>
        /// Determines the fragment type from a statement type
        /// </summary>
        public static SqlFragmentType GetFragmentTypeFromStatementType(string statementType)
        {
            if (string.IsNullOrEmpty(statementType))
                return SqlFragmentType.Unknown;

            if (_statementTypeMap.TryGetValue(statementType, out var fragmentType))
            {
                return fragmentType;
            }

            // Try to infer from statement name
            if (statementType.Contains("Select", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.Select;
            if (statementType.Contains("Insert", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.Insert;
            if (statementType.Contains("Update", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.Update;
            if (statementType.Contains("Delete", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.Delete;
            if (statementType.Contains("Merge", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.Merge;
            if (statementType.Contains("Create", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.Create;
            if (statementType.Contains("Alter", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.Alter;
            if (statementType.Contains("Drop", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.Drop;
            if (statementType.Contains("Exec", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.Execute;
            if (statementType.Contains("With", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.With;
            if (statementType.Contains("Join", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.Join;
            if (statementType.Contains("CTE", StringComparison.OrdinalIgnoreCase))
                return SqlFragmentType.CTE;

            return SqlFragmentType.Unknown;
        }

        /// <summary>
        /// Extracts a table name parts from a qualified identifier
        /// </summary>
        public static (string Database, string Schema, string Table) ExtractTableNameParts(MultiPartIdentifier identifier)
        {
            if (identifier == null || identifier.Identifiers.Count == 0)
                return (string.Empty, string.Empty, string.Empty);

            string database = string.Empty;
            string schema = string.Empty;
            string table = string.Empty;

            int count = identifier.Identifiers.Count;
            if (count >= 1)
            {
                table = identifier.Identifiers[count - 1].Value;
            }
            if (count >= 2)
            {
                schema = identifier.Identifiers[count - 2].Value;
            }
            if (count >= 3)
            {
                database = identifier.Identifiers[count - 3].Value;
            }

            return (database, schema, table);
        }

        /// <summary>
        /// Extracts a column name parts from a qualified identifier
        /// </summary>
        public static (string Server, string Database, string Schema, string Table, string Column) ExtractColumnNameParts(MultiPartIdentifier identifier)
        {
            if (identifier == null || identifier.Identifiers.Count == 0)
                return (string.Empty, string.Empty, string.Empty, string.Empty, string.Empty);

            string server = string.Empty;
            string database = string.Empty;
            string schema = string.Empty;
            string table = string.Empty;
            string column = string.Empty;

            int count = identifier.Identifiers.Count;
            if (count >= 1)
            {
                column = identifier.Identifiers[count - 1].Value;
            }
            if (count >= 2)
            {
                table = identifier.Identifiers[count - 2].Value;
            }
            if (count >= 3)
            {
                schema = identifier.Identifiers[count - 3].Value;
            }
            if (count >= 4)
            {
                database = identifier.Identifiers[count - 4].Value;
            }
            if (count >= 5)
            {
                server = identifier.Identifiers[count - 5].Value;
            }

            return (server, database, schema, table, column);
        }

        /// <summary>
        /// Gets the full table name from parts
        /// </summary>
        public static string GetFullTableName(string database, string schema, string table)
        {
            if (string.IsNullOrEmpty(table))
                return string.Empty;

            string fullName = table;
            if (!string.IsNullOrEmpty(schema))
            {
                fullName = $"{schema}.{fullName}";
            }
            if (!string.IsNullOrEmpty(database))
            {
                fullName = $"{database}.{fullName}";
            }

            return fullName;
        }

        /// <summary>
        /// Gets the full column name from parts
        /// </summary>
        public static string GetFullColumnName(string table, string column)
        {
            if (string.IsNullOrEmpty(column))
                return string.Empty;

            if (string.IsNullOrEmpty(table))
                return column;

            return $"{table}.{column}";
        }

        /// <summary>
        /// Normalizes a SQL identifier by removing delimiters
        /// </summary>
        public static string NormalizeIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier))
                return string.Empty;

            // Remove brackets
            identifier = identifier.Trim();
            if (identifier.StartsWith("[") && identifier.EndsWith("]"))
            {
                identifier = identifier.Substring(1, identifier.Length - 2);
            }

            // Remove quotes
            if (identifier.StartsWith("\"") && identifier.EndsWith("\""))
            {
                identifier = identifier.Substring(1, identifier.Length - 2);
            }

            return identifier;
        }

        /// <summary>
        /// Attempts to extract alias from SQL text
        /// </summary>
        public static bool TryExtractAlias(string sqlText, out string alias)
        {
            alias = string.Empty;
            if (string.IsNullOrEmpty(sqlText))
                return false;

            var match = _aliasPattern.Match(sqlText);
            if (match.Success)
            {
                alias = match.Groups[1].Success ? match.Groups[1].Value : match.Groups[2].Value;
                return !string.IsNullOrEmpty(alias);
            }

            return false;
        }

        /// <summary>
        /// Gets the simplified SQL command type from fragment type
        /// </summary>
        public static string GetSqlCommandType(SqlFragmentType fragmentType)
        {
            return fragmentType switch
            {
                SqlFragmentType.Select => "SELECT",
                SqlFragmentType.Insert => "INSERT",
                SqlFragmentType.Update => "UPDATE",
                SqlFragmentType.Delete => "DELETE",
                SqlFragmentType.Merge => "MERGE",
                SqlFragmentType.Create => "CREATE",
                SqlFragmentType.Alter => "ALTER",
                SqlFragmentType.Drop => "DROP",
                SqlFragmentType.Declare => "DECLARE",
                SqlFragmentType.Set => "SET",
                SqlFragmentType.If => "IF",
                SqlFragmentType.While => "WHILE",
                SqlFragmentType.Begin => "BEGIN",
                SqlFragmentType.Commit => "COMMIT",
                SqlFragmentType.Rollback => "ROLLBACK",
                SqlFragmentType.Try => "TRY",
                SqlFragmentType.Catch => "CATCH",
                SqlFragmentType.Execute => "EXECUTE",
                SqlFragmentType.With => "WITH",
                SqlFragmentType.Union => "UNION",
                SqlFragmentType.Intersect => "INTERSECT",
                SqlFragmentType.Except => "EXCEPT",
                _ => fragmentType.ToString()
            };
        }

        /// <summary>
        /// Checks if a fragment modifies data (DML)
        /// </summary>
        public static bool IsDataModification(SqlFragmentType fragmentType)
        {
            return fragmentType is SqlFragmentType.Insert or SqlFragmentType.Update 
                or SqlFragmentType.Delete or SqlFragmentType.Merge;
        }

        /// <summary>
        /// Checks if a fragment retrieves data (DQL)
        /// </summary>
        public static bool IsDataRetrieval(SqlFragmentType fragmentType)
        {
            return fragmentType is SqlFragmentType.Select;
        }

        /// <summary>
        /// Checks if a fragment is a data definition (DDL)
        /// </summary>
        public static bool IsDataDefinition(SqlFragmentType fragmentType)
        {
            return fragmentType is SqlFragmentType.Create or SqlFragmentType.Alter 
                or SqlFragmentType.Drop;
        }

        /// <summary>
        /// Initializes the statement type to fragment type map
        /// </summary>
        private static Dictionary<string, SqlFragmentType> InitializeStatementTypeMap()
        {
            var map = new Dictionary<string, SqlFragmentType>(StringComparer.OrdinalIgnoreCase)
            {
                // Batches
                { "TSqlScript", SqlFragmentType.Batch },
                { "TSqlBatch", SqlFragmentType.Batch },
                
                // DML statements
                { "SelectStatement", SqlFragmentType.Select },
                { "InsertStatement", SqlFragmentType.Insert },
                { "UpdateStatement", SqlFragmentType.Update },
                { "DeleteStatement", SqlFragmentType.Delete },
                { "MergeStatement", SqlFragmentType.Merge },
                
                // DDL statements
                { "CreateTableStatement", SqlFragmentType.Create },
                { "CreateViewStatement", SqlFragmentType.View },
                { "CreateProcedureStatement", SqlFragmentType.Procedure },
                { "CreateFunctionStatement", SqlFragmentType.Function },
                { "CreateTriggerStatement", SqlFragmentType.Trigger },
                { "AlterTableStatement", SqlFragmentType.Alter },
                { "AlterViewStatement", SqlFragmentType.View },
                { "AlterProcedureStatement", SqlFragmentType.Procedure },
                { "AlterFunctionStatement", SqlFragmentType.Function },
                { "AlterTriggerStatement", SqlFragmentType.Trigger },
                { "DropTableStatement", SqlFragmentType.Drop },
                { "DropViewStatement", SqlFragmentType.View },
                { "DropProcedureStatement", SqlFragmentType.Procedure },
                { "DropFunctionStatement", SqlFragmentType.Function },
                { "DropTriggerStatement", SqlFragmentType.Trigger },
                
                // Variable statements
                { "DeclareTableStatement", SqlFragmentType.Declare },
                { "DeclareVariableStatement", SqlFragmentType.Declare },
                { "SetVariableStatement", SqlFragmentType.Set },
                
                // Control flow
                { "IfStatement", SqlFragmentType.If },
                { "WhileStatement", SqlFragmentType.While },
                { "BeginEndBlockStatement", SqlFragmentType.Begin },
                { "TryCatchStatement", SqlFragmentType.Try },
                { "ExecuteStatement", SqlFragmentType.Execute },
                
                // Other elements
                { "CommonTableExpression", SqlFragmentType.CTE },
                { "SubqueryExpression", SqlFragmentType.Subquery },
                { "QueryExpression", SqlFragmentType.Expression },
                { "BooleanExpression", SqlFragmentType.Expression },
                { "CaseExpression", SqlFragmentType.Case },
                { "QualifiedJoin", SqlFragmentType.Join },
                { "CommentStatement", SqlFragmentType.Comment },
                { "WhitespaceToken", SqlFragmentType.Whitespace }
            };

            return map;
        }

        /// <summary>
        /// Gets a join type description
        /// </summary>
        public static string GetJoinTypeDescription(QualifiedJoin join)
        {
            return join.QualifiedJoinType switch
            {
                QualifiedJoinType.Inner => "INNER JOIN",
                QualifiedJoinType.FullOuter => "FULL OUTER JOIN",
                QualifiedJoinType.LeftOuter => "LEFT OUTER JOIN",
                QualifiedJoinType.RightOuter => "RIGHT OUTER JOIN",
                QualifiedJoinType.CrossApply => "CROSS APPLY",
                QualifiedJoinType.OuterApply => "OUTER APPLY",
                _ => "JOIN"
            };
        }

        /// <summary>
        /// Gets a column reference type from TSqlFragment
        /// </summary>
        public static ColumnReferenceType GetColumnReferenceType(TSqlFragment fragment)
        {
            return fragment switch
            {
                ColumnReferenceExpression => ColumnReferenceType.Regular,
                StarColumnReference => ColumnReferenceType.Wildcard,
                VariableReference => ColumnReferenceType.Variable,
                ParameterReference => ColumnReferenceType.Parameter,
                _ => ColumnReferenceType.Expression
            };
        }

        /// <summary>
        /// Gets a table reference type from TSqlFragment
        /// </summary>
        public static TableReferenceType GetTableReferenceType(TSqlFragment fragment)
        {
            return fragment switch
            {
                NamedTableReference => TableReferenceType.Table,
                TableValuedFunctionReference => TableReferenceType.Function,
                TemporaryTableReference => TableReferenceType.TemporaryTable,
                CommonTableExpression => TableReferenceType.CTE,
                DerivedTable => TableReferenceType.Derived,
                TableVariableReference => TableReferenceType.Variable,
                _ => TableReferenceType.Table
            };
        }
    }
}
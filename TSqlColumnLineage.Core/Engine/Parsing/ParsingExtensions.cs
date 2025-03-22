using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Domain.Context;
using TSqlColumnLineage.Core.Domain.Graph;
using TSqlColumnLineage.Core.Engine.Parsing.Models;


using ModelTable = TSqlColumnLineage.Core.Engine.Parsing.Models.TableReference;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// Extension methods for parsing and analyzing T-SQL scripts.
    /// Simplifies common operations and provides a fluent API.
    /// </summary>
    public static class ParsingExtensions
    {
        /// <summary>
        /// Extracts all table references from a parsed script
        /// </summary>
        public static List<ModelTable> ExtractAllTableReferences(this ParsedScript parsedScript)
        {
            if (parsedScript == null)
                return [];

            var allTables = new List<ModelTable>();
            
            foreach (var fragment in parsedScript.Fragments)
            {
                allTables.AddRange(fragment.TableReferences);
            }
            
            return allTables;
        }

        /// <summary>
        /// Extracts all column references from a parsed script
        /// </summary>
        public static List<ColumnReference> ExtractAllColumnReferences(this ParsedScript parsedScript)
        {
            if (parsedScript == null)
                return [];

            var allColumns = new List<ColumnReference>();
            
            foreach (var fragment in parsedScript.Fragments)
            {
                allColumns.AddRange(fragment.ColumnReferences);
            }
            
            return allColumns;
        }

        /// <summary>
        /// Gets all source columns (columns being read) from a parsed script
        /// </summary>
        public static List<ColumnReference> GetSourceColumns(this ParsedScript parsedScript)
        {
            if (parsedScript == null)
                return [];

            var sourceColumns = new List<ColumnReference>();
            
            foreach (var fragment in parsedScript.Fragments)
            {
                sourceColumns.AddRange(fragment.ColumnReferences.Where(c => c.IsSource));
            }
            
            return sourceColumns;
        }

        /// <summary>
        /// Gets all target columns (columns being written) from a parsed script
        /// </summary>
        public static List<ColumnReference> GetTargetColumns(this ParsedScript parsedScript)
        {
            if (parsedScript == null)
                return [];

            var targetColumns = new List<ColumnReference>();
            
            foreach (var fragment in parsedScript.Fragments)
            {
                targetColumns.AddRange(fragment.ColumnReferences.Where(c => c.IsTarget));
            }
            
            return targetColumns;
        }

        /// <summary>
        /// Gets all source tables (tables being read) from a parsed script
        /// </summary>
        public static List<string> GetSourceTables(this ParsedScript parsedScript)
        {
            if (parsedScript == null)
                return [];

            var sourceTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var column in parsedScript.GetSourceColumns())
            {
                if (!string.IsNullOrEmpty(column.TableName))
                {
                    sourceTables.Add(column.TableName);
                }
            }
            
            return sourceTables.ToList();
        }

        /// <summary>
        /// Gets all target tables (tables being written) from a parsed script
        /// </summary>
        public static List<string> GetTargetTables(this ParsedScript parsedScript)
        {
            if (parsedScript == null)
                return [];

            var targetTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var column in parsedScript.GetTargetColumns())
            {
                if (!string.IsNullOrEmpty(column.TableName))
                {
                    targetTables.Add(column.TableName);
                }
            }
            
            return targetTables.ToList();
        }

        /// <summary>
        /// Finds all fragments of a specific type
        /// </summary>
        public static List<SqlFragment> FindFragmentsByType(this ParsedScript parsedScript, SqlFragmentType type)
        {
            if (parsedScript == null)
                return [];

            return parsedScript.Fragments.Where(f => f.FragmentType == type).ToList();
        }

        /// <summary>
        /// Finds all fragments containing a specific table reference
        /// </summary>
        public static List<SqlFragment> FindFragmentsByTable(this ParsedScript parsedScript, string tableName)
        {
            if (parsedScript == null || string.IsNullOrEmpty(tableName))
                return [];

            return parsedScript.Fragments.Where(f => 
                f.TableReferences.Any(t => 
                    string.Equals(t.TableName, tableName, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(t.Alias, tableName, StringComparison.OrdinalIgnoreCase))).ToList();
        }

        /// <summary>
        /// Finds all fragments containing a specific column reference
        /// </summary>
        public static List<SqlFragment> FindFragmentsByColumn(this ParsedScript parsedScript, string tableName, string columnName)
        {
            if (parsedScript == null || string.IsNullOrEmpty(columnName))
                return [];

            return parsedScript.Fragments.Where(f => 
                f.ColumnReferences.Any(c => 
                    string.Equals(c.ColumnName, columnName, StringComparison.OrdinalIgnoreCase) &&
                    (string.IsNullOrEmpty(tableName) || string.Equals(c.TableName, tableName, StringComparison.OrdinalIgnoreCase)))).ToList();
        }

        /// <summary>
        /// Gets a nicely formatted error report for a parsed script
        /// </summary>
        public static string GetErrorReport(this ParsedScript parsedScript)
        {
            if (parsedScript == null || parsedScript.Errors.Count == 0)
                return "No errors found.";

            var sb = new StringBuilder();
            sb.AppendLine($"Found {parsedScript.Errors.Count} parsing errors:");
            sb.AppendLine();

            foreach (var error in parsedScript.Errors)
            {
                sb.AppendLine($"Error at line {error.Line}, column {error.Column}: {error.Message}");
                
                // Get source excerpt
                string excerpt = parsedScript.GetSourceExcerpt(error.StartOffset, error.EndOffset);
                if (!string.IsNullOrEmpty(excerpt))
                {
                    sb.AppendLine("Context:");
                    sb.AppendLine(excerpt);
                    sb.AppendLine();
                }
            }

            return sb.ToString();
        }

        /// <summary>
        /// Gets a summary of the parsed script
        /// </summary>
        public static string GetSummary(this ParsedScript parsedScript)
        {
            if (parsedScript == null)
                return "No script to summarize.";

            var sb = new StringBuilder();
            sb.AppendLine($"T-SQL Script Analysis Summary");
            sb.AppendLine($"Source: {parsedScript.Source}");
            sb.AppendLine($"Status: {(parsedScript.IsValid ? "Valid" : "Contains Errors")}");
            sb.AppendLine($"Total Batches: {parsedScript.Batches.Count}");
            sb.AppendLine($"Total Fragments: {parsedScript.TotalFragmentCount}");
            sb.AppendLine();

            // Count statements by type
            var statementTypes = parsedScript.Fragments
                .GroupBy(f => f.FragmentType)
                .Select(g => new { Type = g.Key, Count = g.Count() })
                .OrderByDescending(g => g.Count);

            sb.AppendLine("Statement Types:");
            foreach (var type in statementTypes)
            {
                if (type.Type != SqlFragmentType.Batch)
                {
                    sb.AppendLine($"  {type.Type}: {type.Count}");
                }
            }
            sb.AppendLine();

            // Table references
            var tableReferences = parsedScript.ExtractAllTableReferences()
                .GroupBy(t => t.TableName)
                .Select(g => new { TableName = g.Key, Count = g.Count() })
                .OrderByDescending(g => g.Count);

            sb.AppendLine("Referenced Tables:");
            foreach (var table in tableReferences)
            {
                sb.AppendLine($"  {table.TableName}: {table.Count} references");
            }
            sb.AppendLine();

            // Source and target tables
            sb.AppendLine("Source Tables (tables being read):");
            foreach (var table in parsedScript.GetSourceTables())
            {
                sb.AppendLine($"  {table}");
            }
            sb.AppendLine();

            sb.AppendLine("Target Tables (tables being written):");
            foreach (var table in parsedScript.GetTargetTables())
            {
                sb.AppendLine($"  {table}");
            }
            sb.AppendLine();

            // Errors if any
            if (parsedScript.Errors.Count > 0)
            {
                sb.AppendLine($"Parsing Errors: {parsedScript.Errors.Count}");
                for (int i = 0; i < Math.Min(5, parsedScript.Errors.Count); i++)
                {
                    var error = parsedScript.Errors[i];
                    sb.AppendLine($"  Line {error.Line}: {error.Message}");
                }
                
                if (parsedScript.Errors.Count > 5)
                {
                    sb.AppendLine($"  ... and {parsedScript.Errors.Count - 5} more errors");
                }
            }

            return sb.ToString();
        }

        /// <summary>
        /// Converts a parsed script to a lineage graph
        /// </summary>
        public static async Task<LineageGraph> ToLineageGraphAsync(this ParsedScript parsedScript, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(parsedScript);

            // Create lineage graph
            var graph = new LineageGraph();
            
            // Create context manager
            using var contextManager = new ContextManager(graph);
            
            // Process lineage using the parsing service
            var parsingService = new ParsingService();
            await parsingService.ProcessLineageAsync(parsedScript, graph, contextManager, cancellationToken);
            
            return graph;
        }

        /// <summary>
        /// Creates a script context for analyzing lineage
        /// </summary>
        public static QueryContext CreateScriptContext(this ParsedScript parsedScript, LineageGraph graph)
        {
            if (parsedScript == null || graph == null)
                throw new ArgumentNullException(nameof(parsedScript));

            // Create context manager
            var contextManager = new ContextManager(graph);
            
            // Create script context
            var scriptContext = new QueryContext(contextManager, parsedScript.ScriptText);
            
            return scriptContext;
        }

        /// <summary>
        /// Saves the parsed script to a JSON file
        /// </summary>
        public static void SaveToJson(this ParsedScript parsedScript, string filePath)
        {
            ArgumentNullException.ThrowIfNull(parsedScript);

            // This would require implementing JSON serialization
            // Left as an exercise for the implementer
            throw new NotImplementedException("JSON serialization not implemented");
        }

        /// <summary>
        /// Gets a formatted representation of a SQL fragment
        /// </summary>
        public static string GetFormattedSql(this SqlFragment fragment)
        {
            if (fragment == null)
                return string.Empty;

            // Format SQL using the ScriptDom formatter
            if (fragment.ParsedFragment != null)
            {
                var generator = new Sql150ScriptGenerator(new SqlScriptGeneratorOptions
                {
                    KeywordCasing = KeywordCasing.Uppercase,
                    IncludeSemicolons = true,
                    NewLineBeforeFromClause = true,
                    NewLineBeforeOrderByClause = true,
                    NewLineBeforeWhereClause = true,
                    AlignClauseBodies = true
                });

                generator.GenerateScript(fragment.ParsedFragment, out string formattedSql);
                return formattedSql;
            }

            return fragment.SqlText;
        }

        /// <summary>
        /// Gets a compact fragment description
        /// </summary>
        public static string GetDescriptorSummary(this SqlFragment fragment)
        {
            if (fragment == null)
                return string.Empty;

            StringBuilder sb = new();
            sb.Append(fragment.FragmentType.ToString());
            
            // Add key table references
            if (fragment.TableReferences.Count > 0)
            {
                sb.Append(": ");
                
                int count = Math.Min(3, fragment.TableReferences.Count);
                for (int i = 0; i < count; i++)
                {
                    if (i > 0) sb.Append(", ");
                    sb.Append(fragment.TableReferences[i].TableName);
                }
                
                if (fragment.TableReferences.Count > count)
                {
                    sb.Append($", +{fragment.TableReferences.Count - count} more");
                }
            }
            
            sb.Append($" [line {fragment.LineNumber}]");
            
            return sb.ToString();
        }
    }
}
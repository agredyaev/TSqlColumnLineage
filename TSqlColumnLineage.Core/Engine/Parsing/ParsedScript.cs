using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Engine.Parsing.Models;

using ModelTable = TSqlColumnLineage.Core.Engine.Parsing.Models.TableReference;
using ScriptDomTable = Microsoft.SqlServer.TransactSql.ScriptDom.TableReference;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// Represents a parsed T-SQL script optimized for column lineage analysis.
    /// Implements data-oriented design for efficient memory usage.
    /// </summary>
    public class ParsedScript
    {
        /// <summary>
        /// Gets or sets the original script text
        /// </summary>
        public string ScriptText { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the source identifier (e.g., file name)
        /// </summary>
        public string Source { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the list of parsed fragments
        /// </summary>
        public List<SqlFragment> Fragments { get; set; } = [];

        /// <summary>
        /// Gets or sets the root batch fragments
        /// </summary>
        public List<SqlFragment> Batches { get; set; } = [];

        /// <summary>
        /// Gets or sets the table references
        /// </summary>
        public Dictionary<string, List<ModelTable>> TableReferences { get; set; } = new Dictionary<string, List<ModelTable>>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Gets or sets the column references 
        /// </summary>
        public Dictionary<string, List<ColumnReference>> ColumnReferences { get; set; } = new Dictionary<string, List<ColumnReference>>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Gets or sets the list of parsing errors
        /// </summary>
        public List<ParseError> Errors { get; set; } = [];

        /// <summary>
        /// Gets or sets whether parsing was successful
        /// </summary>
        public bool IsValid => Errors.Count == 0;

        /// <summary>
        /// Gets or sets the parsed AST (may be null if streaming parser is used)
        /// </summary>
        public TSqlFragment ScriptAst { get; set; }

        /// <summary>
        /// Gets or sets token stream
        /// </summary>
        public IList<TSqlParserToken> TokenStream { get; set; }

        /// <summary>
        /// Gets or sets the custom metadata
        /// </summary>
        public Dictionary<string, object> Metadata { get; set; } = [];

        /// <summary>
        /// Gets the total count of all SQL fragments
        /// </summary>
        public int TotalFragmentCount => Fragments.Count;

        /// <summary>
        /// Gets all fragments of a specific type
        /// </summary>
        public List<SqlFragment> GetFragmentsByType(SqlFragmentType type)
        {
            return Fragments.Where(f => f.FragmentType == type).ToList();
        }

        /// <summary>
        /// Gets table references by table name
        /// </summary>
        public List<ModelTable> GetTableReferences(string tableName)
        {
            if (string.IsNullOrEmpty(tableName) || !TableReferences.TryGetValue(tableName, out var references))
                return [];

            return references;
        }

        /// <summary>
        /// Gets column references by table and column name
        /// </summary>
        public List<ColumnReference> GetColumnReferences(string tableName, string columnName)
        {
            string key = string.IsNullOrEmpty(tableName) ? columnName : $"{tableName}.{columnName}";

            if (!ColumnReferences.TryGetValue(key, out var references))
                return [];

            return references;
        }

        /// <summary>
        /// Gets a source excerpt from the script
        /// </summary>
        public string GetSourceExcerpt(int startOffset, int endOffset, int contextLines = 1)
        {
            if (string.IsNullOrEmpty(ScriptText) || startOffset < 0 || endOffset <= startOffset || endOffset > ScriptText.Length)
                return string.Empty;

            // Find line start positions
            List<int> lineStarts = [0];

            for (int i = 0; i < ScriptText.Length; i++)
            {
                if (ScriptText[i] == '\n')
                {
                    lineStarts.Add(i + 1);
                }
            }

            // Find the start line
            int startLineIdx = 0;
            while (startLineIdx < lineStarts.Count && lineStarts[startLineIdx] <= startOffset)
            {
                startLineIdx++;
            }
            startLineIdx = Math.Max(0, startLineIdx - 1);

            // Find the end line
            int endLineIdx = startLineIdx;
            while (endLineIdx < lineStarts.Count && lineStarts[endLineIdx] <= endOffset)
            {
                endLineIdx++;
            }
            endLineIdx = Math.Max(0, endLineIdx - 1);

            // Apply context
            startLineIdx = Math.Max(0, startLineIdx - contextLines);
            endLineIdx = Math.Min(lineStarts.Count - 1, endLineIdx + contextLines);

            // Extract text
            _ = lineStarts[startLineIdx];
            _ = (endLineIdx + 1 < lineStarts.Count) ? lineStarts[endLineIdx + 1] - 1 : ScriptText.Length;

            StringBuilder sb = new();
            
            for (int i = startLineIdx; i <= endLineIdx; i++)
            {
                int lineStart = lineStarts[i];
                int lineEnd = (i + 1 < lineStarts.Count) ? lineStarts[i + 1] - 1 : ScriptText.Length;
                
                // Add line number
                sb.Append($"{i + 1,4}: ");
                
                // Add line content
                string line = ScriptText[lineStart..lineEnd];
                sb.AppendLine(line);
                
                // Add highlight for the referenced fragment
                if (startOffset >= lineStart && startOffset <= lineEnd)
                {
                    int highlightStart = startOffset - lineStart;
                    int highlightLength = Math.Min(endOffset - startOffset, lineEnd - startOffset);
                    sb.Append("     ");
                    sb.Append(' ', highlightStart);
                    sb.AppendLine(new string('^', highlightLength));
                }
            }
            
            return sb.ToString();
        }

        /// <summary>
        /// Creates a new ParsedScript from a list of batches
        /// </summary>
        public static ParsedScript FromBatches(List<SqlFragment> batches, string scriptText, string source)
        {
            var script = new ParsedScript
            {
                ScriptText = scriptText,
                Source = source,
                Batches = batches,
                Fragments = [],
                TableReferences = new Dictionary<string, List<ModelTable>>(StringComparer.OrdinalIgnoreCase),
                ColumnReferences = new Dictionary<string, List<ColumnReference>>(StringComparer.OrdinalIgnoreCase)
            };

            // Collect all fragments recursively
            foreach (var batch in batches)
            {
                CollectFragmentsRecursive(batch, script.Fragments);
            }

            // Collect table and column references
            foreach (var fragment in script.Fragments)
            {
                // Index table references
                foreach (var tableRef in fragment.TableReferences)
                {
                    if (!script.TableReferences.TryGetValue(tableRef.TableName, out var tableRefs))
                    {
                        tableRefs = [];
                        script.TableReferences[tableRef.TableName] = tableRefs;
                    }
                    
                    tableRefs.Add(tableRef);
                }

                // Index column references
                foreach (var colRef in fragment.ColumnReferences)
                {
                    string key = string.IsNullOrEmpty(colRef.TableName) ? colRef.ColumnName : $"{colRef.TableName}.{colRef.ColumnName}";
                    
                    if (!script.ColumnReferences.TryGetValue(key, out var colRefs))
                    {
                        colRefs = [];
                        script.ColumnReferences[key] = colRefs;
                    }
                    
                    colRefs.Add(colRef);
                }
            }

            return script;
        }

        /// <summary>
        /// Recursively collects all fragments from a hierarchy
        /// </summary>
        private static void CollectFragmentsRecursive(SqlFragment fragment, List<SqlFragment> allFragments)
        {
            allFragments.Add(fragment);
            
            foreach (var child in fragment.Children)
            {
                CollectFragmentsRecursive(child, allFragments);
            }
        }
    }

    /// <summary>
    /// Represents a SQL parsing error
    /// </summary>
    public class ParseError
    {
        /// <summary>
        /// Gets or sets the error message
        /// </summary>
        public string Message { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the error code
        /// </summary>
        public int ErrorCode { get; set; }

        /// <summary>
        /// Gets or sets the line number
        /// </summary>
        public int Line { get; set; }

        /// <summary>
        /// Gets or sets the column number
        /// </summary>
        public int Column { get; set; }

        /// <summary>
        /// Gets or sets the start offset in the script
        /// </summary>
        public int StartOffset { get; set; }
        
        /// <summary>
        /// Gets or sets the end offset in the script
        /// </summary>
        public int EndOffset { get; set; }

        /// <summary>
        /// Returns a string representation of this error
        /// </summary>
        public override string ToString()
        {
            return $"Error {ErrorCode} at line {Line}, column {Column}: {Message}";
        }
    }
}
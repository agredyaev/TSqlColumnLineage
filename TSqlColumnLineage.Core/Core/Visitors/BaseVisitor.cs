using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace TSqlColumnLineage.Core.Visitors
{
    /// <summary>
    /// Base visitor for traversing the T-SQL AST
    /// </summary>
    public abstract class BaseVisitor : TSqlFragmentVisitor
    {
        // Track unique fragments by their hash code to prevent infinite recursion
        private readonly HashSet<int> _visitedFragments = new HashSet<int>();
        // Track current path depth
        private int _recursionDepth = 0;
        // Maximum recursion depth allowed (safety valve)
        private const int MaxRecursionDepth = 500;
        
        // Simple tracking of timestamp to prevent excessive time in AST traversal
        private readonly DateTime _startTime = DateTime.Now;
        private const int MaxExecutionTimeInSeconds = 30;

        /// <summary>
        /// Lineage graph
        /// </summary>
        protected LineageGraph Graph { get; }

        /// <summary>
        /// Execution context
        /// </summary>
        protected LineageContext Context { get; }

        /// <summary>
        /// Logger
        /// </summary>
        protected ILogger? Logger { get; }

        /// <summary>
        /// Constructor for the base visitor
        /// </summary>
        protected BaseVisitor(LineageGraph graph, LineageContext context, ILogger? logger = null)
        {
            Graph = graph ?? throw new ArgumentNullException(nameof(graph));
            Context = context ?? throw new ArgumentNullException(nameof(context));
            Logger = logger;
        }

        /// <summary>
        /// Starts visiting the AST fragment
        /// </summary>
        public override void Visit(TSqlFragment fragment)
        {
            if (fragment == null)
                return;
            
            // Emergency timeout to prevent infinite processing
            if ((DateTime.Now - _startTime).TotalSeconds > MaxExecutionTimeInSeconds)
            {
                Logger?.LogWarning($"AST traversal exceeded maximum execution time of {MaxExecutionTimeInSeconds} seconds");
                return;
            }

            // Prevent stack overflow by limiting recursion depth
            _recursionDepth++;
            
            if (_recursionDepth > MaxRecursionDepth)
            {
                _recursionDepth--;
                Logger?.LogWarning($"Maximum recursion depth ({MaxRecursionDepth}) reached for {fragment.GetType().Name}");
                return;
            }

            // Use the object's hash code as a simple way to identify already visited fragments
            // This prevents cycles in the traversal
            int fragmentHash = RuntimeHelpers.GetHashCode(fragment);
            if (_visitedFragments.Contains(fragmentHash))
            {
                _recursionDepth--;
                return;
            }
            
            try
            {
                _visitedFragments.Add(fragmentHash);
                fragment.Accept(this);
            }
            catch (Exception ex)
            {
                Logger?.LogError(ex, $"Error visiting {fragment.GetType().Name} at Line {fragment.StartLine}, Column {fragment.StartColumn}");
                throw;
            }
            finally
            {
                _recursionDepth--;
                
                // Clean up state if we're back at the root
                if (_recursionDepth == 0)
                {
                    _visitedFragments.Clear();
                }
            }
        }

        /// <summary>
        /// Creates a unique identifier for a node
        /// </summary>
        public string CreateNodeId(string prefix, string name)
        {
            return $"{prefix}_{Guid.NewGuid():N}_{name.Replace(".", "_")}";
        }

        /// <summary>
        /// Creates a random identifier
        /// </summary>
        public string CreateRandomId()
        {
            return Guid.NewGuid().ToString("N");
        }

        /// <summary>
        /// Gets the SQL text of a fragment
        /// </summary>
        public string GetSqlText(TSqlFragment fragment)
        {
            return Parsing.ScriptDomUtils.GetFragmentSql(fragment);
        }

        /// <summary>
        /// Logs an error message
        /// </summary>
        protected void LogError(string message, Exception? ex = null)
        {
            Logger?.LogError(ex, message);
        }

        /// <summary>
        /// Logs an information message
        /// </summary>
        protected void LogInfo(string message)
        {
            Logger?.LogInformation(message);
        }

        /// <summary>
        /// Logs a debug message
        /// </summary>
        protected void LogDebug(string message)
        {
            Logger?.LogDebug(message);
        }

        /// <summary>
        /// Logs a warning message
        /// </summary>
        protected void LogWarning(string message)
        {
            Logger?.LogWarning(message);
        }

        /// <summary>
        /// Attempts to resolve the column ID from a column reference
        /// </summary>
        public string? ResolveColumnId(ColumnReferenceExpression columnRef)
        {
            if (columnRef == null || columnRef.MultiPartIdentifier == null)
                return null;

            var identifiers = columnRef.MultiPartIdentifier.Identifiers;
            if (identifiers.Count == 0)
                return null;

            string? tableName = null;
            string columnName;

            if (identifiers.Count > 1)
            {
                tableName = identifiers[0].Value;
                columnName = identifiers[1].Value;
            }
            else
            {
                columnName = identifiers[0].Value;
            }

            // Table lookup
            TableNode? table = null;
            if (!string.IsNullOrEmpty(tableName))
            {
                table = Context.GetTable(tableName);
            }
            else if (Context.Tables.Count == 1)
            {
                table = Context.Tables.Values.First();
            }

            if (table == null)
                return null;

            // Search for the column by name among existing nodes
            var columnNode = Graph.Nodes
                .OfType<ColumnNode>()
                .FirstOrDefault(c => c.TableOwner == table.Name &&
                                     c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));

            return columnNode?.Id;
        }

        /// <summary>
        /// Gets the table and column name from a column reference
        /// </summary>
        public (string TableName, string ColumnName) GetTableAndColumnName(ColumnReferenceExpression columnRef)
        {
            if (columnRef == null || columnRef.MultiPartIdentifier == null)
                return (string.Empty, string.Empty);

            var identifiers = columnRef.MultiPartIdentifier.Identifiers;
            if (identifiers.Count == 0)
                return (string.Empty, string.Empty);

            string? tableName = null;
            string columnName;

            if (identifiers.Count > 1)
            {
                tableName = identifiers[0].Value;
                columnName = identifiers.Last().Value;
            }
            else
            {
                columnName = identifiers[0].Value;
            }

            // If the table is not explicitly specified, try to find it in the context
            if (string.IsNullOrEmpty(tableName) && Context.Tables.Count == 1)
            {
                tableName = Context.Tables.Values.First().Name;
            }

            return (tableName ?? string.Empty, columnName);
        }
    }
}

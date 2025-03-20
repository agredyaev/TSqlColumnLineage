using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Visitors.Base
{
    /// <summary>
    /// Provides context for SQL fragment visitors
    /// </summary>
    public class VisitorContext
    {
        /// <summary>
        /// The lineage context containing tables, metadata, etc.
        /// </summary>
        public LineageContext LineageContext { get; }
        
        /// <summary>
        /// The parent fragment being processed
        /// </summary>
        public TSqlFragment CurrentFragment { get; private set; }
        
        /// <summary>
        /// Visitor state flags
        /// </summary>
        public Dictionary<string, object> State { get; } = new();
        
        /// <summary>
        /// Stack of fragments being processed
        /// </summary>
        private readonly Stack<TSqlFragment> _fragmentStack = new();
        
        /// <summary>
        /// Stack of state dictionaries
        /// </summary>
        private readonly Stack<Dictionary<string, object>> _stateStack = new();
        
        /// <summary>
        /// Flag indicating if processing should stop (e.g., timeout or limit reached)
        /// </summary>
        public bool ShouldStop { get; set; }
        
        /// <summary>
        /// Processing start time for timeout detection
        /// </summary>
        public DateTime StartTime { get; } = DateTime.UtcNow;
        
        /// <summary>
        /// Maximum processing time in milliseconds (default: 30 seconds)
        /// </summary>
        public int MaxProcessingTimeMs { get; set; } = 30000;
        
        /// <summary>
        /// Fragment visit counter to detect excessive processing
        /// </summary>
        public int FragmentVisitCount { get; set; }
        
        /// <summary>
        /// Maximum number of fragments to visit (default: 50,000)
        /// </summary>
        public int MaxFragmentVisitCount { get; set; } = 50000;
        
        /// <summary>
        /// Creates a new visitor context
        /// </summary>
        /// <param name="lineageContext">The lineage context</param>
        public VisitorContext(LineageContext lineageContext)
        {
            LineageContext = lineageContext ?? throw new ArgumentNullException(nameof(lineageContext));
        }
        
        /// <summary>
        /// Pushes a new fragment onto the stack
        /// </summary>
        /// <param name="fragment">The fragment to push</param>
        public void PushFragment(TSqlFragment fragment)
        {
            if (fragment == null) return;
            
            // Save current state
            _fragmentStack.Push(CurrentFragment);
            _stateStack.Push(new Dictionary<string, object>(State));
            
            // Update current fragment
            CurrentFragment = fragment;
            
            // Clear state for new fragment
            State.Clear();
            
            // Check if we should stop processing
            FragmentVisitCount++;
            CheckProcessingLimits();
        }
        
        /// <summary>
        /// Pops a fragment from the stack
        /// </summary>
        public void PopFragment()
        {
            if (_fragmentStack.Count == 0) return;
            
            // Restore previous fragment and state
            CurrentFragment = _fragmentStack.Pop();
            State.Clear();
            
            // Restore previous state
            if (_stateStack.Count > 0)
            {
                var previousState = _stateStack.Pop();
                foreach (var (key, value) in previousState)
                {
                    State[key] = value;
                }
            }
        }
        
        /// <summary>
        /// Checks if processing limits have been reached
        /// </summary>
        public void CheckProcessingLimits()
        {
            // Check time limit
            if ((DateTime.UtcNow - StartTime).TotalMilliseconds > MaxProcessingTimeMs)
            {
                ShouldStop = true;
                return;
            }
            
            // Check fragment count limit
            if (FragmentVisitCount > MaxFragmentVisitCount)
            {
                ShouldStop = true;
                return;
            }
        }
        
        /// <summary>
        /// Gets the current execution path as a string
        /// </summary>
        public string GetExecutionPath()
        {
            var fragments = new List<TSqlFragment>(_fragmentStack);
            fragments.Add(CurrentFragment);
            
            return string.Join(" -> ", fragments.Select(f => f?.GetType().Name ?? "null"));
        }
        
        /// <summary>
        /// Creates a column node for the given table and column
        /// </summary>
        public ColumnNode GetOrCreateColumnNode(string tableName, string columnName, string dataType = "unknown")
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(columnName))
                return null;
                
            // Try to find existing column
            var column = LineageContext.Graph.GetColumnNode(tableName, columnName);
            if (column != null)
                return column;
                
            // Create new column node
            var columnNode = new ColumnNode
            {
                Id = Guid.NewGuid().ToString(),
                Name = columnName,
                ObjectName = columnName,
                TableOwner = tableName,
                DataType = dataType
            };
            
            // Add to graph
            LineageContext.Graph.AddNode(columnNode);
            
            return columnNode;
        }
        
        /// <summary>
        /// Gets or creates a table node
        /// </summary>
        public TableNode GetOrCreateTableNode(string tableName, string tableType = "Table", string schema = "dbo")
        {
            if (string.IsNullOrEmpty(tableName))
                return null;
                
            // Try to find existing table
            var table = LineageContext.GetTable(tableName);
            if (table != null)
                return table;
                
            // Create new table node
            var tableNode = new TableNode
            {
                Id = Guid.NewGuid().ToString(),
                Name = tableName,
                ObjectName = tableName,
                SchemaName = schema,
                TableType = tableType
            };
            
            // Add to graph and context
            LineageContext.Graph.AddNode(tableNode);
            LineageContext.AddTable(tableNode);
            
            return tableNode;
        }
    }
}
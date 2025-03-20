using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Analysis.Context;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Visitors.Base
{
    /// <summary>
    /// Provides optimized context for SQL fragment visitors with improved state management and performance
    /// </summary>
    public sealed class VisitorContext
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
        private readonly ConcurrentDictionary<string, object> _state = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Public accessor for state
        /// </summary>
        public IReadOnlyDictionary<string, object> State => _state;
        
        /// <summary>
        /// Stack of fragments being processed
        /// </summary>
        private readonly Stack<TSqlFragment> _fragmentStack = new Stack<TSqlFragment>();
        
        /// <summary>
        /// Stack of state dictionaries
        /// </summary>
        private readonly Stack<Dictionary<string, object>> _stateStack = new Stack<Dictionary<string, object>>();
        
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
        /// String pool for optimizing memory usage
        /// </summary>
        private readonly StringPool _stringPool;
        
        /// <summary>
        /// Logger instance
        /// </summary>
        private readonly ILogger _logger;
        
        /// <summary>
        /// Thread-local fragment stack for parallel processing
        /// </summary>
        private readonly ThreadLocal<Stack<TSqlFragment>> _threadLocalFragmentStack;
        
        /// <summary>
        /// Thread-local state stack for parallel processing
        /// </summary>
        private readonly ThreadLocal<Stack<Dictionary<string, object>>> _threadLocalStateStack;
        
        /// <summary>
        /// Creates a new visitor context
        /// </summary>
        /// <param name="lineageContext">The lineage context</param>
        /// <param name="stringPool">String pool for memory optimization</param>
        /// <param name="logger">Logger for diagnostic information</param>
        public VisitorContext(LineageContext lineageContext, StringPool stringPool, ILogger logger = null)
        {
            LineageContext = lineageContext ?? throw new ArgumentNullException(nameof(lineageContext));
            _stringPool = stringPool ?? throw new ArgumentNullException(nameof(stringPool));
            _logger = logger;
            
            // Initialize thread-local stacks for parallel processing
            _threadLocalFragmentStack = new ThreadLocal<Stack<TSqlFragment>>(() => new Stack<TSqlFragment>());
            _threadLocalStateStack = new ThreadLocal<Stack<Dictionary<string, object>>>(() => new Stack<Dictionary<string, object>>());
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
            
            // Get thread-local state stack
            var stateStack = _threadLocalStateStack.Value;
            
            // Save current state
            var currentState = new Dictionary<string, object>(_state);
            stateStack.Push(currentState);
            
            // Update current fragment
            CurrentFragment = fragment;
            
            // Clear state for new fragment
            _state.Clear();
            
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
            
            // Restore previous fragment
            CurrentFragment = _fragmentStack.Pop();
            
            // Get thread-local state stack
            var stateStack = _threadLocalStateStack.Value;
            
            // Restore previous state
            if (stateStack.Count > 0)
            {
                var previousState = stateStack.Pop();
                _state.Clear();
                foreach (var (key, value) in previousState)
                {
                    _state[key] = value;
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
                _logger?.LogWarning($"Processing time limit reached: {MaxProcessingTimeMs}ms");
                ShouldStop = true;
                return;
            }
            
            // Check fragment count limit
            if (FragmentVisitCount > MaxFragmentVisitCount)
            {
                _logger?.LogWarning($"Fragment visit limit reached: {MaxFragmentVisitCount}");
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
        /// Sets a state value
        /// </summary>
        /// <param name="key">State key</param>
        /// <param name="value">State value</param>
        public void SetState(string key, object value)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", nameof(key));
                
            key = _stringPool.Intern(key);
            
            if (value == null)
            {
                _state.TryRemove(key, out _);
            }
            else
            {
                _state[key] = value;
                
                // Intern string values
                if (value is string strValue)
                {
                    _state[key] = _stringPool.Intern(strValue);
                }
            }
        }
        
        /// <summary>
        /// Gets a state value
        /// </summary>
        /// <param name="key">State key</param>
        /// <returns>State value or null if not found</returns>
        public object GetState(string key)
        {
            if (string.IsNullOrEmpty(key))
                return null;
                
            _state.TryGetValue(key, out var value);
            return value;
        }
        
        /// <summary>
        /// Gets a state value with type conversion
        /// </summary>
        /// <typeparam name="T">Type to convert to</typeparam>
        /// <param name="key">State key</param>
        /// <param name="defaultValue">Default value if not found or not convertible</param>
        /// <returns>Converted state value or default</returns>
        public T GetState<T>(string key, T defaultValue = default)
        {
            var value = GetState(key);
            
            if (value == null)
                return defaultValue;
                
            try
            {
                return (T)Convert.ChangeType(value, typeof(T));
            }
            catch
            {
                return defaultValue;
            }
        }
        
        /// <summary>
        /// Gets a column node from a context or creates one if it doesn't exist
        /// </summary>
        /// <param name="tableName">Table name</param>
        /// <param name="columnName">Column name</param>
        /// <param name="dataType">Data type (optional)</param>
        /// <returns>Column node</returns>
        public ColumnNode GetOrCreateColumnNode(string tableName, string columnName, string dataType = "unknown")
        {
            return LineageContext.GetOrCreateColumnNode(tableName, columnName, dataType);
        }
        
        /// <summary>
        /// Gets or creates a table node
        /// </summary>
        /// <param name="tableName">Table name</param>
        /// <param name="tableType">Table type</param>
        /// <param name="schema">Schema name</param>
        /// <returns>Table node</returns>
        public TableNode GetOrCreateTableNode(string tableName, string tableType = "Table", string schema = "dbo")
        {
            return LineageContext.GetOrCreateTableNode(tableName, tableType, schema);
        }
        
        /// <summary>
        /// Gets SQL text for a fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <returns>SQL text</returns>
        public string GetSqlText(TSqlFragment fragment)
        {
            if (fragment == null)
                return string.Empty;

            var generator = new Sql160ScriptGenerator();
            var builder = new System.Text.StringBuilder();

            using (var writer = new System.IO.StringWriter(builder))
            {
                generator.GenerateScript(fragment, writer);
            }

            return _stringPool.Intern(builder.ToString());
        }
        
        /// <summary>
        /// Creates a scope that automatically pops the fragment when disposed
        /// </summary>
        /// <param name="fragment">Fragment to push</param>
        /// <returns>Disposable scope</returns>
        public IDisposable CreateFragmentScope(TSqlFragment fragment)
        {
            PushFragment(fragment);
            return new FragmentScope(this);
        }
        
        /// <summary>
        /// Disposable scope for fragment processing
        /// </summary>
        private class FragmentScope : IDisposable
        {
            private readonly VisitorContext _context;
            
            public FragmentScope(VisitorContext context)
            {
                _context = context;
            }
            
            public void Dispose()
            {
                _context.PopFragment();
            }
        }
    }
}
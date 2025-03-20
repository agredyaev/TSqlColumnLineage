using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TSqlColumnLineage.Domain.Graph;

namespace TSqlColumnLineage.Domain.Context
{
    /// <summary>
    /// Maintains context for a specific SQL query during lineage analysis.
    /// Optimized for memory efficiency using data-oriented design principles.
    /// </summary>
    public sealed class QueryContext
    {
        // The parent context manager
        private readonly ContextManager _contextManager;
        
        // Query-specific data
        private string _queryText;
        private DateTime _startTime;
        private int _fragmentCount;
        private int _maxFragments;
        private TimeSpan _maxDuration;
        private readonly CancellationTokenSource _cancellationTokenSource;
        
        // Query column context (source/target columns)
        private readonly Dictionary<string, List<int>> _outputColumns = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, List<int>> _inputTables = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
        
        // Query table alias tracking
        private readonly Dictionary<string, string> _localAliases = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        
        // Current state
        private readonly Dictionary<string, object> _queryState = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Gets the lineage graph
        /// </summary>
        public LineageGraph Graph => _contextManager.Graph;
        
        /// <summary>
        /// Gets the query text
        /// </summary>
        public string QueryText => _queryText;
        
        /// <summary>
        /// Gets a flag indicating whether the query should stop processing
        /// </summary>
        public bool ShouldStop
        {
            get
            {
                // Stop if parent context is stopped
                if (_contextManager.ShouldStop)
                    return true;
                    
                // Stop if this query is canceled
                if (_cancellationTokenSource.IsCancellationRequested)
                    return true;
                    
                // Stop if max fragments exceeded
                if (_maxFragments > 0 && _fragmentCount > _maxFragments)
                    return true;
                    
                // Stop if max duration exceeded
                if (_maxDuration > TimeSpan.Zero && DateTime.UtcNow - _startTime > _maxDuration)
                    return true;
                    
                return false;
            }
        }
        
        /// <summary>
        /// Creates a new query context
        /// </summary>
        public QueryContext(ContextManager contextManager, string queryText)
        {
            _contextManager = contextManager ?? throw new ArgumentNullException(nameof(contextManager));
            _queryText = queryText;
            _startTime = DateTime.UtcNow;
            _maxFragments = 0;
            _maxDuration = TimeSpan.Zero;
            _cancellationTokenSource = new CancellationTokenSource();
        }
        
        /// <summary>
        /// Creates a new query context with limits
        /// </summary>
        public QueryContext(ContextManager contextManager, string queryText, int maxFragments, TimeSpan maxDuration)
            : this(contextManager, queryText)
        {
            _maxFragments = maxFragments;
            _maxDuration = maxDuration;
        }
        
        /// <summary>
        /// Creates a query scope that will be popped when disposed
        /// </summary>
        public IDisposable CreateQueryScope(string name = null)
        {
            return _contextManager.CreateScope(ScopeType.Query, name);
        }
        
        /// <summary>
        /// Adds an output column for the query
        /// </summary>
        public void AddOutputColumn(string tableName, int columnId)
        {
            if (string.IsNullOrEmpty(tableName))
                return;
                
            if (!_outputColumns.TryGetValue(tableName, out var columns))
            {
                columns = new List<int>();
                _outputColumns[tableName] = columns;
            }
            
            if (!columns.Contains(columnId))
            {
                columns.Add(columnId);
            }
        }
        
        /// <summary>
        /// Adds an input table for the query
        /// </summary>
        public void AddInputTable(string tableName, int tableId)
        {
            if (string.IsNullOrEmpty(tableName))
                return;
                
            if (!_inputTables.TryGetValue(tableName, out var tables))
            {
                tables = new List<int>();
                _inputTables[tableName] = tables;
            }
            
            if (!tables.Contains(tableId))
            {
                tables.Add(tableId);
            }
        }
        
        /// <summary>
        /// Adds a local table alias
        /// </summary>
        public void AddLocalAlias(string alias, string tableName)
        {
            if (string.IsNullOrEmpty(alias) || string.IsNullOrEmpty(tableName))
                return;
                
            _localAliases[alias] = tableName;
            
            // Also add to global aliases
            _contextManager.AddTableAlias(alias, tableName);
        }
        
        /// <summary>
        /// Resolves a table name from a local alias
        /// </summary>
        public string ResolveLocalAlias(string nameOrAlias)
        {
            if (string.IsNullOrEmpty(nameOrAlias))
                return null;
                
            // Check local aliases first
            if (_localAliases.TryGetValue(nameOrAlias, out var tableName))
            {
                return tableName;
            }
            
            // Fall back to global aliases
            return _contextManager.ResolveTableAlias(nameOrAlias);
        }
        
        /// <summary>
        /// Gets a table ID
        /// </summary>
        public int GetTableId(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return -1;
                
            // Resolve alias
            tableName = ResolveLocalAlias(tableName);
            
            return _contextManager.GetTableId(tableName);
        }
        
        /// <summary>
        /// Gets a column node
        /// </summary>
        public int GetColumnNode(string tableName, string columnName)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(columnName))
                return -1;
                
            // Resolve alias
            tableName = ResolveLocalAlias(tableName);
            
            return _contextManager.GetColumnNode(tableName, columnName);
        }
        
        /// <summary>
        /// Sets a query state value
        /// </summary>
        public void SetQueryState(string key, object value)
        {
            if (string.IsNullOrEmpty(key))
                return;
                
            if (value == null)
            {
                _queryState.Remove(key);
            }
            else
            {
                _queryState[key] = value;
            }
        }
        
        /// <summary>
        /// Gets a query state value
        /// </summary>
        public object GetQueryState(string key)
        {
            if (string.IsNullOrEmpty(key))
                return null;
                
            if (_queryState.TryGetValue(key, out var value))
            {
                return value;
            }
            
            return null;
        }
        
        /// <summary>
        /// Gets a boolean query state value
        /// </summary>
        public bool GetBoolQueryState(string key, bool defaultValue = false)
        {
            var value = GetQueryState(key);
            
            if (value == null)
                return defaultValue;
                
            if (value is bool boolValue)
                return boolValue;
                
            if (value is string strValue)
                return !string.IsNullOrEmpty(strValue) && strValue.Equals("true", StringComparison.OrdinalIgnoreCase);
                
            return defaultValue;
        }
        
        /// <summary>
        /// Increments the fragment count
        /// </summary>
        public void IncrementFragmentCount()
        {
            Interlocked.Increment(ref _fragmentCount);
        }
        
        /// <summary>
        /// Gets all output columns
        /// </summary>
        public Dictionary<string, List<int>> GetOutputColumns()
        {
            var result = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var kvp in _outputColumns)
            {
                result[kvp.Key] = new List<int>(kvp.Value);
            }
            
            return result;
        }
        
        /// <summary>
        /// Gets all input tables
        /// </summary>
        public Dictionary<string, List<int>> GetInputTables()
        {
            var result = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var kvp in _inputTables)
            {
                result[kvp.Key] = new List<int>(kvp.Value);
            }
            
            return result;
        }
        
        /// <summary>
        /// Returns query statistics
        /// </summary>
        public QueryStats GetStatistics()
        {
            int outputColumnCount = _outputColumns.Values.Sum(v => v.Count);
            int inputTableCount = _inputTables.Count;
            TimeSpan duration = DateTime.UtcNow - _startTime;
            
            return new QueryStats
            {
                FragmentCount = _fragmentCount,
                OutputColumnCount = outputColumnCount,
                InputTableCount = inputTableCount,
                DurationMs = (int)duration.TotalMilliseconds,
                Stopped = ShouldStop
            };
        }
        
        /// <summary>
        /// Cancels the query processing
        /// </summary>
        public void Cancel()
        {
            _cancellationTokenSource.Cancel();
        }
        
        /// <summary>
        /// Disposes resources
        /// </summary>
        public void Dispose()
        {
            _cancellationTokenSource.Dispose();
        }
    }
    
    /// <summary>
    /// Query statistics
    /// </summary>
    public class QueryStats
    {
        public int FragmentCount { get; set; }
        public int OutputColumnCount { get; set; }
        public int InputTableCount { get; set; }
        public int DurationMs { get; set; }
        public bool Stopped { get; set; }
        
        public override string ToString()
        {
            return $"Processed {FragmentCount} fragments in {DurationMs}ms, " +
                   $"output: {OutputColumnCount} columns, input: {InputTableCount} tables" +
                   (Stopped ? " (stopped)" : "");
        }
    }
}
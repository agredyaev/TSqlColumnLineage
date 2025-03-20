using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Context
{
    /// <summary>
    /// Provides context for lineage tracking operations with optimized state management
    /// </summary>
    public sealed class LineageContext
    {
        /// <summary>
        /// The lineage graph being constructed
        /// </summary>
        public LineageGraph Graph { get; }

        /// <summary>
        /// Utility objects for memory optimization
        /// </summary>
        private readonly StringPool _stringPool;
        private readonly IdGenerator _idGenerator;
        
        /// <summary>
        /// Collection of tables in the current context
        /// </summary>
        private readonly ConcurrentDictionary<string, TableNode> _tables = new ConcurrentDictionary<string, TableNode>(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Table alias mappings (alias -> table name)
        /// </summary>
        private readonly ConcurrentDictionary<string, string> _tableAliases = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Temporary tables in the current context
        /// </summary>
        private readonly ConcurrentDictionary<string, TableNode> _tempTables = new ConcurrentDictionary<string, TableNode>(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Table variables in the current context
        /// </summary>
        private readonly ConcurrentDictionary<string, TableNode> _tableVariables = new ConcurrentDictionary<string, TableNode>(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Column context for tracking current column context when processing
        /// </summary>
        private readonly ConcurrentDictionary<string, ColumnNode> _columnContext = new ConcurrentDictionary<string, ColumnNode>(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// General-purpose metadata storage
        /// </summary>
        private readonly ConcurrentDictionary<string, object> _metadata = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Stack of operation contexts for nested processing
        /// </summary>
        private readonly AsyncLocal<Stack<Dictionary<string, object>>> _contextStack = new AsyncLocal<Stack<Dictionary<string, object>>>();
        
        /// <summary>
        /// The current table context stack
        /// </summary>
        private readonly AsyncLocal<Stack<TableNode>> _tableContextStack = new AsyncLocal<Stack<TableNode>>();
        
        /// <summary>
        /// Access to tables collection
        /// </summary>
        public IReadOnlyDictionary<string, TableNode> Tables => _tables;
        
        /// <summary>
        /// Access to table aliases collection
        /// </summary>
        public IReadOnlyDictionary<string, string> TableAliases => _tableAliases;
        
        /// <summary>
        /// Access to temp tables collection
        /// </summary>
        public IReadOnlyDictionary<string, TableNode> TempTables => _tempTables;
        
        /// <summary>
        /// Access to table variables collection
        /// </summary>
        public IReadOnlyDictionary<string, TableNode> TableVariables => _tableVariables;
        
        /// <summary>
        /// Access to metadata collection
        /// </summary>
        public IReadOnlyDictionary<string, object> Metadata => _metadata;

        /// <summary>
        /// Creates a new instance of LineageContext
        /// </summary>
        /// <param name="graph">The lineage graph to populate</param>
        /// <param name="stringPool">String pool for memory optimization</param>
        /// <param name="idGenerator">ID generator for creating nodes and edges</param>
        public LineageContext(LineageGraph graph, StringPool stringPool, IdGenerator idGenerator)
        {
            Graph = graph ?? throw new ArgumentNullException(nameof(graph));
            _stringPool = stringPool ?? throw new ArgumentNullException(nameof(stringPool));
            _idGenerator = idGenerator ?? throw new ArgumentNullException(nameof(idGenerator));
            
            // Initialize context stack
            _contextStack.Value = new Stack<Dictionary<string, object>>();
            _tableContextStack.Value = new Stack<TableNode>();
        }
        
        /// <summary>
        /// Gets the current table context stack
        /// </summary>
        public Stack<TableNode> CurrentTableContext
        {
            get
            {
                if (_tableContextStack.Value == null)
                {
                    _tableContextStack.Value = new Stack<TableNode>();
                }
                return _tableContextStack.Value;
            }
        }
        
        /// <summary>
        /// Gets the column context for the specified key
        /// </summary>
        /// <param name="key">Context key</param>
        /// <returns>Column node or null if not found</returns>
        public ColumnNode GetColumnContext(string key)
        {
            return _columnContext.TryGetValue(key, out var column) ? column : null;
        }
        
        /// <summary>
        /// Sets the column context for the specified key
        /// </summary>
        /// <param name="key">Context key</param>
        /// <param name="column">Column node</param>
        public void SetColumnContext(string key, ColumnNode column)
        {
            if (column == null)
            {
                _columnContext.TryRemove(key, out _);
            }
            else
            {
                _columnContext[key] = column;
            }
        }

        /// <summary>
        /// Adds a table to the context
        /// </summary>
        /// <param name="table">Table node to add</param>
        public void AddTable(TableNode table)
        {
            if (table == null) throw new ArgumentNullException(nameof(table));
            
            // Intern strings
            table.InternStrings(_stringPool);
            
            // Add to the appropriate collection based on table type
            if (table.TableType.Equals("TempTable", StringComparison.OrdinalIgnoreCase) ||
                table.Name.StartsWith("#", StringComparison.Ordinal))
            {
                _tempTables[table.Name] = table;
            }
            else if (table.TableType.Equals("TableVariable", StringComparison.OrdinalIgnoreCase) ||
                     table.Name.StartsWith("@", StringComparison.Ordinal))
            {
                _tableVariables[table.Name] = table;
            }
            
            // Always add to main Tables collection
            _tables[table.Name] = table;
        }

        /// <summary>
        /// Adds a table alias to the context
        /// </summary>
        /// <param name="alias">Table alias</param>
        /// <param name="tableName">Actual table name</param>
        public void AddTableAlias(string alias, string tableName)
        {
            if (string.IsNullOrEmpty(alias)) throw new ArgumentException("Alias cannot be null or empty", nameof(alias));
            if (string.IsNullOrEmpty(tableName)) throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
            
            _tableAliases[_stringPool.Intern(alias)] = _stringPool.Intern(tableName);
        }

        /// <summary>
        /// Gets a table by name or alias
        /// </summary>
        /// <param name="nameOrAlias">Table name or alias</param>
        /// <returns>Table node or null if not found</returns>
        public TableNode GetTable(string nameOrAlias)
        {
            if (string.IsNullOrEmpty(nameOrAlias))
                return null;
                
            // Direct table lookup
            if (_tables.TryGetValue(nameOrAlias, out var table))
                return table;
            
            // Look for temp table
            if (_tempTables.TryGetValue(nameOrAlias, out var tempTable))
                return tempTable;
                
            // Look for table variable
            if (_tableVariables.TryGetValue(nameOrAlias, out var tableVar))
                return tableVar;
            
            // Look up by alias
            if (_tableAliases.TryGetValue(nameOrAlias, out var actualName) && 
                _tables.TryGetValue(actualName, out var aliasedTable))
                return aliasedTable;
            
            return null;
        }
        
        /// <summary>
        /// Creates a column node for the given table and column
        /// </summary>
        /// <param name="tableName">Table name</param>
        /// <param name="columnName">Column name</param>
        /// <param name="dataType">Data type</param>
        /// <returns>The created or retrieved column node</returns>
        public ColumnNode GetOrCreateColumnNode(string tableName, string columnName, string dataType = "unknown")
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(columnName))
                return null;
                
            tableName = _stringPool.Intern(tableName);
            columnName = _stringPool.Intern(columnName);
            dataType = _stringPool.Intern(dataType);
                
            // Try to find existing column
            var column = Graph.GetColumnNode(tableName, columnName);
            if (column != null)
                return column;
                
            // Create new column node with interned strings
            var id = _idGenerator.CreateNodeId("COLUMN", $"{tableName}.{columnName}");
            var columnNode = new ColumnNode(
                id,
                columnName,
                tableName,
                dataType);
            
            // Add to graph
            Graph.AddNode(columnNode);
            
            return columnNode;
        }
        
        /// <summary>
        /// Gets or creates a table node
        /// </summary>
        /// <param name="tableName">Table name</param>
        /// <param name="tableType">Table type</param>
        /// <param name="schema">Schema name</param>
        /// <returns>The created or retrieved table node</returns>
        public TableNode GetOrCreateTableNode(string tableName, string tableType = "Table", string schema = "dbo")
        {
            if (string.IsNullOrEmpty(tableName))
                return null;
                
            tableName = _stringPool.Intern(tableName);
            tableType = _stringPool.Intern(tableType);
            schema = _stringPool.Intern(schema);
                
            // Try to find existing table
            var table = GetTable(tableName);
            if (table != null)
                return table;
                
            // Create new table node with interned strings
            var id = _idGenerator.CreateNodeId("TABLE", tableName);
            var tableNode = new TableNode(
                id,
                tableName,
                tableType,
                objectName: tableName,
                schemaName: schema);
            
            // Add to graph and context
            Graph.AddNode(tableNode);
            AddTable(tableNode);
            
            return tableNode;
        }
        
        /// <summary>
        /// Sets a metadata value
        /// </summary>
        /// <param name="key">Metadata key</param>
        /// <param name="value">Metadata value</param>
        public void SetMetadata(string key, object value)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", nameof(key));
                
            key = _stringPool.Intern(key);
            
            if (value == null)
            {
                _metadata.TryRemove(key, out _);
            }
            else
            {
                _metadata[key] = value;
                
                // Intern string values
                if (value is string strValue)
                {
                    _metadata[key] = _stringPool.Intern(strValue);
                }
            }
        }
        
        /// <summary>
        /// Gets a metadata value
        /// </summary>
        /// <param name="key">Metadata key</param>
        /// <returns>Metadata value or null if not found</returns>
        public object GetMetadata(string key)
        {
            if (string.IsNullOrEmpty(key))
                return null;
                
            _metadata.TryGetValue(key, out var value);
            return value;
        }
        
        /// <summary>
        /// Gets a metadata value with type conversion
        /// </summary>
        /// <typeparam name="T">Type to convert to</typeparam>
        /// <param name="key">Metadata key</param>
        /// <param name="defaultValue">Default value if not found or not convertible</param>
        /// <returns>Converted metadata value or default</returns>
        public T GetMetadata<T>(string key, T defaultValue = default)
        {
            var value = GetMetadata(key);
            
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
        /// Pushes the current context onto the stack and creates a new context
        /// </summary>
        public void PushContext()
        {
            // Make sure stack is initialized
            if (_contextStack.Value == null)
            {
                _contextStack.Value = new Stack<Dictionary<string, object>>();
            }
            
            // Save current metadata
            var currentContext = new Dictionary<string, object>();
            foreach (var kvp in _metadata)
            {
                currentContext[kvp.Key] = kvp.Value;
            }
            
            // Push onto stack
            _contextStack.Value.Push(currentContext);
            
            // Clear current metadata for new context
            _metadata.Clear();
        }
        
        /// <summary>
        /// Pops the context from the stack, restoring the previous context
        /// </summary>
        public void PopContext()
        {
            // Make sure stack is initialized
            if (_contextStack.Value == null || _contextStack.Value.Count == 0)
                return;
                
            // Clear current metadata
            _metadata.Clear();
            
            // Restore previous context
            var previousContext = _contextStack.Value.Pop();
            foreach (var (key, value) in previousContext)
            {
                _metadata[key] = value;
            }
        }
        
        /// <summary>
        /// Creates a scoped context that automatically restores the previous context when disposed
        /// </summary>
        /// <returns>A disposable scope object</returns>
        public IDisposable CreateScope()
        {
            return new ContextScope(this);
        }
        
        /// <summary>
        /// Clones this context with a new graph
        /// </summary>
        /// <param name="newGraph">New graph to use (optional)</param>
        /// <returns>A new LineageContext instance</returns>
        public LineageContext Clone(LineageGraph newGraph = null)
        {
            var graph = newGraph ?? Graph.Clone();
            var clone = new LineageContext(graph, _stringPool, _idGenerator);
            
            // Clone collections
            foreach (var (key, value) in _tables)
            {
                clone._tables[key] = value;
            }
            
            foreach (var (key, value) in _tableAliases)
            {
                clone._tableAliases[key] = value;
            }
            
            foreach (var (key, value) in _metadata)
            {
                clone._metadata[key] = value;
            }
            
            foreach (var (key, value) in _tempTables)
            {
                clone._tempTables[key] = value;
            }
            
            foreach (var (key, value) in _tableVariables)
            {
                clone._tableVariables[key] = value;
            }
            
            foreach (var (key, value) in _columnContext)
            {
                clone._columnContext[key] = value;
            }
            
            return clone;
        }
        
        /// <summary>
        /// Disposable class for scoped context operations
        /// </summary>
        private class ContextScope : IDisposable
        {
            private readonly LineageContext _context;
            
            public ContextScope(LineageContext context)
            {
                _context = context;
                _context.PushContext();
            }
            
            public void Dispose()
            {
                _context.PopContext();
            }
        }
    }
}
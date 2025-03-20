using System;
using System.Collections.Generic;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Models.Graph
{
    /// <summary>
    /// Provides context for lineage tracking operations
    /// </summary>
    public class LineageContext
    {
        /// <summary>
        /// The lineage graph being constructed
        /// </summary>
        public LineageGraph Graph { get; }

        /// <summary>
        /// Collection of tables in the current context
        /// </summary>
        public Dictionary<string, TableNode> Tables { get; } = new(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Table alias mappings (alias -> table name)
        /// </summary>
        public Dictionary<string, string> TableAliases { get; } = new(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// General-purpose metadata storage
        /// </summary>
        public Dictionary<string, object> Metadata { get; } = new();
        
        /// <summary>
        /// Temporary tables in the current context
        /// </summary>
        public Dictionary<string, TableNode> TempTables { get; } = new(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Table variables in the current context
        /// </summary>
        public Dictionary<string, TableNode> TableVariables { get; } = new(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Column context for tracking current column context when processing
        /// </summary>
        private readonly Dictionary<string, ColumnNode> _columnContext = new();
        
        /// <summary>
        /// Stack of operation contexts to track nested processing
        /// </summary>
        private readonly Stack<Dictionary<string, object>> _contextStack = new();

        /// <summary>
        /// Creates a new instance of LineageContext
        /// </summary>
        /// <param name="graph">The lineage graph to populate</param>
        public LineageContext(LineageGraph graph)
        {
            Graph = graph ?? throw new ArgumentNullException(nameof(graph));
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
                _columnContext.Remove(key);
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
            
            // Add to the appropriate collection based on table type
            if (table.TableType.Equals("TempTable", StringComparison.OrdinalIgnoreCase) ||
                table.Name.StartsWith("#", StringComparison.Ordinal))
            {
                TempTables[table.Name] = table;
            }
            else if (table.TableType.Equals("TableVariable", StringComparison.OrdinalIgnoreCase) ||
                     table.Name.StartsWith("@", StringComparison.Ordinal))
            {
                TableVariables[table.Name] = table;
            }
            
            // Always add to main Tables collection
            Tables[table.Name] = table;
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
            
            TableAliases[alias] = tableName;
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
            if (Tables.TryGetValue(nameOrAlias, out var table))
                return table;
            
            // Look for temp table
            if (TempTables.TryGetValue(nameOrAlias, out var tempTable))
                return tempTable;
                
            // Look for table variable
            if (TableVariables.TryGetValue(nameOrAlias, out var tableVar))
                return tableVar;
            
            // Look up by alias
            if (TableAliases.TryGetValue(nameOrAlias, out var actualName) && 
                Tables.TryGetValue(actualName, out var aliasedTable))
                return aliasedTable;
            
            return null;
        }
        
        /// <summary>
        /// Pushes the current context onto the stack and creates a new context
        /// </summary>
        public void PushContext()
        {
            var currentContext = new Dictionary<string, object>(Metadata);
            _contextStack.Push(currentContext);
            Metadata.Clear();
        }
        
        /// <summary>
        /// Pops the context from the stack, restoring the previous context
        /// </summary>
        public void PopContext()
        {
            if (_contextStack.Count > 0)
            {
                Metadata.Clear();
                var previousContext = _contextStack.Pop();
                foreach (var (key, value) in previousContext)
                {
                    Metadata[key] = value;
                }
            }
        }
        
        /// <summary>
        /// Creates a clone of this context with a new graph
        /// </summary>
        public LineageContext Clone(LineageGraph newGraph = null)
        {
            var graph = newGraph ?? Graph.Clone();
            var clone = new LineageContext(graph);
            
            // Clone collections
            foreach (var (key, value) in Tables)
            {
                clone.Tables[key] = value;
            }
            
            foreach (var (key, value) in TableAliases)
            {
                clone.TableAliases[key] = value;
            }
            
            foreach (var (key, value) in Metadata)
            {
                clone.Metadata[key] = value;
            }
            
            foreach (var (key, value) in TempTables)
            {
                clone.TempTables[key] = value;
            }
            
            foreach (var (key, value) in TableVariables)
            {
                clone.TableVariables[key] = value;
            }
            
            foreach (var (key, value) in _columnContext)
            {
                clone._columnContext[key] = value;
            }
            
            return clone;
        }
    }
}
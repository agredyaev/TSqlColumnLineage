using System;
using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Models
{
    public class LineageContext
    {
        public LineageGraph Graph { get; }

        public Dictionary<string, TableNode> Tables { get; } = new();
        public Dictionary<string, string> TableAliases { get; } = new();
        public Dictionary<string, object> Metadata { get; } = new();
        
        // Support for temporary tables
        public Dictionary<string, TableNode> TempTables { get; } = new();
        
        // Support for table variables
        public Dictionary<string, TableNode> TableVariables { get; } = new();
        
        // Column context for tracking current column context when processing
        private readonly Dictionary<string, ColumnNode> _columnContext = new();

        public LineageContext(LineageGraph graph)
        {
            Graph = graph ?? throw new ArgumentNullException(nameof(graph));
        }
        
        /// <summary>
        /// Gets the column context for the specified key
        /// </summary>
        /// <param name="key">Context key</param>
        /// <returns>Column node or null if not found</returns>
        public ColumnNode? GetColumnContext(string key)
        {
            return _columnContext.TryGetValue(key, out var column) ? column : null;
        }
        
        /// <summary>
        /// Sets the column context for the specified key
        /// </summary>
        /// <param name="key">Context key</param>
        /// <param name="column">Column node</param>
        public void SetColumnContext(string key, ColumnNode? column)
        {
            _columnContext[key] = column;
        }

        public void AddTable(TableNode table)
        {
            Tables[table.Name] = table;
        }

        public void AddTableAlias(string alias, string tableName)
        {
            TableAliases[alias] = tableName;
        }

        public TableNode? GetTable(string nameOrAlias)
        {
            if (Tables.ContainsKey(nameOrAlias))
                return Tables[nameOrAlias];
            
            if (TableAliases.ContainsKey(nameOrAlias) && 
                Tables.ContainsKey(TableAliases[nameOrAlias]))
                return Tables[TableAliases[nameOrAlias]];
            
            return null;
        }
    }
}

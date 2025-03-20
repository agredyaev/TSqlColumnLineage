using System;

namespace TSqlColumnLineage.Core.Models
{
    /// <summary>
    /// Factory for creating lineage nodes
    /// </summary>
    public class LineageNodeFactory
    {
        /// <summary>
        /// Creates a new table node
        /// </summary>
        /// <param name="schema">Schema name</param>
        /// <param name="name">Table name</param>
        /// <returns>New table node</returns>
        public TableNode CreateTableNode(string schema, string name) => 
            new TableNode 
            { 
                SchemaName = schema, 
                Name = name,
                ObjectName = name,
                Id = Guid.NewGuid().ToString() 
            };

        /// <summary>
        /// Creates a new column node
        /// </summary>
        /// <param name="name">Column name</param>
        /// <param name="table">Owner table</param>
        /// <returns>New column node</returns>
        public ColumnNode CreateColumnNode(string name, TableNode table) => 
            new ColumnNode 
            { 
                Name = name, 
                TableOwner = table.Name,
                SchemaName = table.SchemaName,
                ObjectName = name,
                Id = Guid.NewGuid().ToString()
            };
            
        /// <summary>
        /// Creates a new expression node
        /// </summary>
        /// <param name="name">Expression name or description</param>
        /// <param name="expression">SQL expression</param>
        /// <returns>New expression node</returns>
        public ExpressionNode CreateExpressionNode(string name, string expression) =>
            new ExpressionNode
            {
                Name = name,
                Expression = expression,
                ObjectName = name,
                Id = Guid.NewGuid().ToString()
            };
    }
}

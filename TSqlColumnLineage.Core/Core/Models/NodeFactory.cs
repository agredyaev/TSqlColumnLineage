using System;
using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Models
{
    /// <summary>
    /// Factory for creating different types of lineage nodes
    /// </summary>
    public class NodeFactory : INodeFactory
    {
        private readonly LineageNodeFactory _nodeIdFactory;
        
        /// <summary>
        /// Initializes a new instance of the NodeFactory class
        /// </summary>
        /// <param name="nodeIdFactory">Factory for generating node IDs</param>
        public NodeFactory(LineageNodeFactory nodeIdFactory)
        {
            _nodeIdFactory = nodeIdFactory ?? throw new ArgumentNullException(nameof(nodeIdFactory));
        }
        
        /// <summary>
        /// Creates a table node
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="tableType">Type of table (e.g., Table, View, CTE, TempTable)</param>
        /// <param name="schemaName">Optional schema name</param>
        /// <param name="databaseName">Optional database name</param>
        /// <param name="alias">Optional table alias</param>
        /// <param name="definition">Optional SQL definition</param>
        /// <returns>A new TableNode instance</returns>
        public TableNode CreateTableNode(
            string tableName, 
            string tableType,
            string schemaName = "",
            string databaseName = "",
            string alias = "",
            string definition = "")
        {
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(nameof(tableName), "Table name cannot be null or empty");
                
            if (string.IsNullOrEmpty(tableType))
                throw new ArgumentNullException(nameof(tableType), "Table type cannot be null or empty");
                
            // Generate a unique ID for the table
            string nodeId = Guid.NewGuid().ToString();
            
            var tableNode = new TableNode
            {
                Id = nodeId,
                Name = tableName,
                ObjectName = tableName,
                SchemaName = schemaName ?? string.Empty,
                DatabaseName = databaseName ?? string.Empty,
                Type = "Table",
                TableType = tableType,
                Alias = alias ?? string.Empty,
                Definition = definition ?? string.Empty
            };
            
            return tableNode;
        }
        
        /// <summary>
        /// Creates a column node
        /// </summary>
        /// <param name="columnName">Name of the column</param>
        /// <param name="tableOwner">Name of the table that owns this column</param>
        /// <param name="dataType">Data type of the column</param>
        /// <param name="isNullable">Whether the column is nullable</param>
        /// <param name="schemaName">Optional schema name</param>
        /// <param name="databaseName">Optional database name</param>
        /// <returns>A new ColumnNode instance</returns>
        public ColumnNode CreateColumnNode(
            string columnName,
            string tableOwner,
            string dataType = "unknown",
            bool isNullable = true,
            string schemaName = "",
            string databaseName = "")
        {
            if (string.IsNullOrEmpty(columnName))
                throw new ArgumentNullException(nameof(columnName), "Column name cannot be null or empty");
                
            if (string.IsNullOrEmpty(tableOwner))
                throw new ArgumentNullException(nameof(tableOwner), "Table owner cannot be null or empty");
                
            // Generate a unique ID for the column
            string nodeId = Guid.NewGuid().ToString();
            
            var columnNode = new ColumnNode
            {
                Id = nodeId,
                Name = columnName,
                ObjectName = columnName,
                SchemaName = schemaName ?? string.Empty,
                DatabaseName = databaseName ?? string.Empty,
                Type = "Column",
                TableOwner = tableOwner,
                DataType = dataType ?? "unknown",
                IsNullable = isNullable
            };
            
            return columnNode;
        }
        
        /// <summary>
        /// Creates an expression node
        /// </summary>
        /// <param name="expression">SQL expression</param>
        /// <param name="expressionType">Type of expression (e.g., Function, Case, Calculation)</param>
        /// <param name="alias">Optional expression alias</param>
        /// <returns>A new ExpressionNode instance</returns>
        public ExpressionNode CreateExpressionNode(
            string expression,
            string expressionType,
            string alias = "")
        {
            if (string.IsNullOrEmpty(expression))
                throw new ArgumentNullException(nameof(expression), "Expression cannot be null or empty");
                
            if (string.IsNullOrEmpty(expressionType))
                throw new ArgumentNullException(nameof(expressionType), "Expression type cannot be null or empty");
                
            // Generate a unique ID for the expression
            string nodeId = Guid.NewGuid().ToString();
            
            var expressionNode = new ExpressionNode
            {
                Id = nodeId,
                Name = alias ?? $"Expr_{Guid.NewGuid().ToString("N").Substring(0, 8)}",
                ObjectName = expression,
                Type = "Expression",
                ExpressionType = expressionType,
                Expression = expression,
                TableOwner = string.Empty
            };
            
            // Store the alias in metadata if provided
            if (!string.IsNullOrEmpty(alias))
            {
                expressionNode.Metadata["Alias"] = alias;
            }
            
            return expressionNode;
        }
    }
}

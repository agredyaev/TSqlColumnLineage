namespace TSqlColumnLineage.Core.Models
{
    /// <summary>
    /// Interface for factory creating lineage nodes
    /// </summary>
    public interface INodeFactory
    {
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
        TableNode CreateTableNode(
            string tableName, 
            string tableType,
            string schemaName = "",
            string databaseName = "",
            string alias = "",
            string definition = "");
        
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
        ColumnNode CreateColumnNode(
            string columnName,
            string tableOwner,
            string dataType = "unknown",
            bool isNullable = true,
            string schemaName = "",
            string databaseName = "");
        
        /// <summary>
        /// Creates an expression node
        /// </summary>
        /// <param name="expression">SQL expression</param>
        /// <param name="expressionType">Type of expression (e.g., Function, Case, Calculation)</param>
        /// <param name="alias">Optional expression alias</param>
        /// <returns>A new ExpressionNode instance</returns>
        ExpressionNode CreateExpressionNode(
            string expression,
            string expressionType,
            string alias = "");
    }
}

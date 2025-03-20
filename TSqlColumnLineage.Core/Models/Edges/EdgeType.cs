namespace TSqlColumnLineage.Core.Models.Edges
{
    /// <summary>
    /// Defines the types of relationships between nodes in the lineage graph
    /// </summary>
    public enum EdgeType
    {
        /// <summary>
        /// Direct one-to-one mapping between columns (e.g., SELECT a FROM t)
        /// </summary>
        Direct,
        
        /// <summary>
        /// Indirect relationship through transformations (e.g., functions, calculations)
        /// </summary>
        Indirect,
        
        /// <summary>
        /// Column used in a join condition
        /// </summary>
        Join,
        
        /// <summary>
        /// Column used in a filter condition
        /// </summary>
        Filter,
        
        /// <summary>
        /// Parameter mapping (e.g., stored procedure parameter)
        /// </summary>
        Parameter,
        
        /// <summary>
        /// Columns used in grouping operations
        /// </summary>
        GroupBy,
        
        /// <summary>
        /// Edge representing a window function relationship
        /// </summary>
        Window
    }
}
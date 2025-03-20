using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Models
{
    /// <summary>
    /// Interface for all lineage nodes in the graph
    /// </summary>
    public interface ILineageNode
    {
        /// <summary>
        /// Gets or sets the unique identifier for the node
        /// </summary>
        string Id { get; set; }
        
        /// <summary>
        /// Gets or sets the name of the node
        /// </summary>
        string Name { get; set; }
        
        /// <summary>
        /// Gets or sets the original object name in the SQL
        /// </summary>
        string ObjectName { get; set; }
        
        /// <summary>
        /// Gets or sets the node type (table, column, expression)
        /// </summary>
        string NodeType { get; }
        
        /// <summary>
        /// Gets or sets additional metadata properties
        /// </summary>
        Dictionary<string, object> Metadata { get; }
    }
}

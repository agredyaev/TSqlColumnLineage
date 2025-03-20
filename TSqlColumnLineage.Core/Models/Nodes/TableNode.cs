using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;

namespace TSqlColumnLineage.Core.Models.Nodes
{    
    /// <summary>
    /// Represents a table in the lineage graph (includes tables, views, CTEs, temp tables)
    /// </summary>
    public sealed class TableNode : LineageNode
    {
        /// <summary>
        /// Initialize a new table node with default properties
        /// </summary>
        public TableNode()
        {
            Type = "Table";
        }

        /// <summary>
        /// Type of the table (Base Table, View, CTE, Temp Table, etc.)
        /// </summary>
        [JsonProperty("tableType")]
        public string TableType { get; init; } = "Unknown";

        /// <summary>
        /// List of column node IDs belonging to this table
        /// </summary>
        [JsonProperty("columns")]
        public HashSet<string> Columns { get; init; } = new HashSet<string>();

        /// <summary>
        /// Table alias in the query, if any
        /// </summary>
        [JsonProperty("alias")]
        public string Alias { get; init; } = string.Empty;

        /// <summary>
        /// Definition for CTEs or temporary tables
        /// </summary>
        [JsonProperty("definition")]
        public string Definition { get; init; } = string.Empty;
        
        /// <summary>
        /// Creates a deep clone of this node
        /// </summary>
        /// <returns>A new instance with the same properties</returns>
        public override ILineageNode Clone()
        {
            var clone = (TableNode)base.Clone();
            
            // Create a new hashset with the same column references
            clone.Columns = new HashSet<string>(Columns);
            
            return clone;
        }
    }
}
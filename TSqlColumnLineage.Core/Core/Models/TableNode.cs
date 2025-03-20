using Newtonsoft.Json;
using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Models
{    
    /// <summary>
    /// Immutable record representing a table in the lineage graph
    /// </summary>
    public sealed class TableNode : LineageNode
    {
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
        /// List of table columns
        /// </summary>
        [JsonProperty("columns")]
        public List<string> Columns { get; init; } = new List<string>();

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
    }
}

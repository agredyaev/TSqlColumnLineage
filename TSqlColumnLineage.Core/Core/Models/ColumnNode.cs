using Newtonsoft.Json;

namespace TSqlColumnLineage.Core.Models
{
    /// <summary>
    /// Immutable record representing a column in the lineage graph
    /// </summary>
    public sealed class ColumnNode : LineageNode
    {
        public ColumnNode()
        {
            Type = "Column";
        }

        /// <summary>
        /// Data type of the column
        /// </summary>
        [JsonProperty("dataType")]
        public string DataType { get; init; } = "unknown";

        /// <summary>
        /// Name of the owning table
        /// </summary>
        [JsonProperty("tableOwner")]
        public string TableOwner { get; init; } = string.Empty;

        /// <summary>
        /// Indicates if the column can contain NULL values
        /// </summary>
        [JsonProperty("isNullable")]
        public bool IsNullable { get; init; }

        /// <summary>
        /// Indicates if the column is computed
        /// </summary>
        [JsonProperty("isComputed")]
        public bool IsComputed { get; init; }
    }
}

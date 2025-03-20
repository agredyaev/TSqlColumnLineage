using Newtonsoft.Json;

namespace TSqlColumnLineage.Core.Models
{
    /// <summary>
    /// Class representing a relationship between nodes in the lineage graph
    /// </summary>
    public class LineageEdge
    {
        /// <summary>
        /// Unique identifier of the edge
        /// </summary>
        [JsonProperty("id")]
        public string Id { get; set; } = string.Empty;

        /// <summary>
        /// Identifier of the source node
        /// </summary>
        [JsonProperty("sourceId")]
        public string SourceId { get; set; } = string.Empty;

        /// <summary>
        /// Identifier of the target node
        /// </summary>
        [JsonProperty("targetId")]
        public string TargetId { get; set; } = string.Empty;

        /// <summary>
        /// Type of relationship (Direct, Filter, Join, Window, Transform, etc.)
        /// </summary>
        [JsonProperty("type")]
        public string Type { get; set; } = string.Empty;

        /// <summary>
        /// Description of the operation
        /// </summary>
        [JsonProperty("operation")]
        public string Operation { get; set; } = string.Empty;

        /// <summary>
        /// SQL expression that creates this relationship
        /// </summary>
        [JsonProperty("sqlExpression")]
        public string SqlExpression { get; set; } = string.Empty;

        public override string ToString()
        {
            return $"{SourceId} --[{Type}:{Operation}]--> {TargetId}";
        }
    }
}

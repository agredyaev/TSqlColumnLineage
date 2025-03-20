using Newtonsoft.Json;
using System;
using TSqlColumnLineage.Core.Models.Edges;

namespace TSqlColumnLineage.Core.Models.Edges
{
    /// <summary>
    /// Represents a relationship between nodes in the lineage graph
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
        /// Type of relationship (Direct, Indirect, Join, etc.)
        /// </summary>
        [JsonProperty("type")]
        public string Type { get; set; } = string.Empty;
        
        /// <summary>
        /// Type of relationship as an enum (for type-safe operations)
        /// </summary>
        [JsonIgnore]
        public EdgeType TypeEnum 
        { 
            get
            {
                if (Enum.TryParse<EdgeType>(Type, true, out var result))
                    return result;
                return EdgeType.Indirect; // Default to indirect if unknown
            }
            set
            {
                Type = value.ToString();
            }
        }

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

        /// <summary>
        /// Creates a unique key for this edge based on source, target and type
        /// </summary>
        [JsonIgnore]
        public string Key => $"{SourceId}-{TargetId}-{Type}";
        
        /// <summary>
        /// Creates a clone of this edge
        /// </summary>
        public LineageEdge Clone()
        {
            return new LineageEdge
            {
                Id = Id,
                SourceId = SourceId,
                TargetId = TargetId,
                Type = Type,
                Operation = Operation,
                SqlExpression = SqlExpression
            };
        }

        public override string ToString()
        {
            return $"{SourceId} --[{Type}:{Operation}]--> {TargetId}";
        }
    }
}
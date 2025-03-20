using Newtonsoft.Json;
using System;
using TSqlColumnLineage.Core.Common.Utils;

namespace TSqlColumnLineage.Core.Models.Edges
{
    /// <summary>
    /// Represents a relationship between nodes in the lineage graph with optimized memory usage
    /// </summary>
    public sealed class LineageEdge
    {
        // Strings interned by StringPool
        
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
        /// Efficient constructor to create a fully initialized edge with interned strings
        /// </summary>
        /// <param name="id">Edge ID</param>
        /// <param name="sourceId">Source node ID</param>
        /// <param name="targetId">Target node ID</param>
        /// <param name="type">Edge type</param>
        /// <param name="operation">Edge operation</param>
        /// <param name="sqlExpression">SQL expression for this edge</param>
        public LineageEdge(
            string id, 
            string sourceId, 
            string targetId, 
            string type, 
            string operation, 
            string sqlExpression = "")
        {
            Id = id;
            SourceId = sourceId;
            TargetId = targetId;
            Type = type;
            Operation = operation;
            SqlExpression = sqlExpression ?? string.Empty;
        }
        
        /// <summary>
        /// Default constructor for deserialization
        /// </summary>
        public LineageEdge() { }
        
        /// <summary>
        /// Returns a clone of this edge
        /// </summary>
        public LineageEdge Clone()
        {
            return new LineageEdge(Id, SourceId, TargetId, Type, Operation, SqlExpression);
        }

        /// <summary>
        /// Returns a string representation of the edge
        /// </summary>
        public override string ToString()
        {
            return $"{SourceId} --[{Type}:{Operation}]--> {TargetId}";
        }
        
        /// <summary>
        /// Helper method to update the strings in this edge using the provided StringPool
        /// </summary>
        /// <param name="stringPool">StringPool to intern strings</param>
        public void InternStrings(StringPool stringPool)
        {
            if (stringPool == null) return;
            
            Id = stringPool.Intern(Id);
            SourceId = stringPool.Intern(SourceId);
            TargetId = stringPool.Intern(TargetId);
            Type = stringPool.Intern(Type);
            Operation = stringPool.Intern(Operation);
            SqlExpression = stringPool.Intern(SqlExpression);
        }
    }
}
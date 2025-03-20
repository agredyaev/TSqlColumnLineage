using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Models.Nodes
{
    /// <summary>
    /// Base class for all lineage graph nodes
    /// </summary>
    public class LineageNode : ILineageNode
    {
        /// <summary>
        /// Unique node identifier
        /// </summary>
        [JsonProperty("id")]
        public string Id { get; set; } = string.Empty;

        /// <summary>
        /// Display name of the node
        /// </summary>
        [JsonProperty("name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Node type (Column, Table, Expression, etc.)
        /// </summary>
        [JsonProperty("type")]
        public string Type { get; set; } = string.Empty;
        
        /// <summary>
        /// Gets the node type for interface implementation
        /// </summary>
        [JsonIgnore]
        public string NodeType => Type;

        /// <summary>
        /// Full object name
        /// </summary>
        [JsonProperty("objectName")]
        public string ObjectName { get; set; } = string.Empty;

        /// <summary>
        /// Schema name
        /// </summary>
        [JsonProperty("schemaName")]
        public string SchemaName { get; set; } = string.Empty;

        /// <summary>
        /// Database name
        /// </summary>
        [JsonProperty("databaseName")]
        public string DatabaseName { get; set; } = string.Empty;

        /// <summary>
        /// Additional node metadata
        /// </summary>
        [JsonProperty("metadata")]
        public Dictionary<string, object> Metadata { get; set; } = new Dictionary<string, object>();
        
        /// <summary>
        /// Creates a deep clone of this node
        /// </summary>
        /// <returns>A new instance with the same properties</returns>
        public virtual ILineageNode Clone()
        {
            var clone = (LineageNode)MemberwiseClone();
            
            // Create deep copies of reference types
            clone.Metadata = new Dictionary<string, object>(Metadata);
            
            return clone;
        }

        public override string ToString()
        {
            return $"{Type}:{(string.IsNullOrEmpty(DatabaseName) ? "" : $"{DatabaseName}.")}" +
                   $"{(string.IsNullOrEmpty(SchemaName) ? "" : $"{SchemaName}.")}{ObjectName}";
        }
    }
}
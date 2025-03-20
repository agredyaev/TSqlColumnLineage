using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace TSqlColumnLineage.Core.Models
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

        public override string ToString()
        {
            return $"{Type}:{(string.IsNullOrEmpty(DatabaseName) ? "" : $"{DatabaseName}.")}" +
                   $"{(string.IsNullOrEmpty(SchemaName) ? "" : $"{SchemaName}.")}{ObjectName}";
        }
    }
}

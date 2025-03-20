using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using TSqlColumnLineage.Core.Common.Utils;

namespace TSqlColumnLineage.Core.Models.Nodes
{
    /// <summary>
    /// Base class for all lineage graph nodes with optimized memory usage
    /// </summary>
    public abstract class LineageNode : ILineageNode
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
        /// Default constructor
        /// </summary>
        protected LineageNode()
        {
        }

        /// <summary>
        /// Creates a new node with the specified parameters and interned strings
        /// </summary>
        /// <param name="id">Node ID</param>
        /// <param name="name">Node name</param>
        /// <param name="type">Node type</param>
        /// <param name="objectName">Object name</param>
        /// <param name="schemaName">Schema name</param>
        /// <param name="databaseName">Database name</param>
        protected LineageNode(
            string id,
            string name,
            string type,
            string objectName,
            string schemaName = "",
            string databaseName = "")
        {
            Id = id;
            Name = name;
            Type = type;
            ObjectName = objectName;
            SchemaName = schemaName ?? string.Empty;
            DatabaseName = databaseName ?? string.Empty;
        }
        
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
        
        /// <summary>
        /// Helper method to update the strings in this node using the provided StringPool
        /// </summary>
        /// <param name="stringPool">StringPool to intern strings</param>
        public void InternStrings(StringPool stringPool)
        {
            if (stringPool == null) return;
            
            Id = stringPool.Intern(Id);
            Name = stringPool.Intern(Name);
            Type = stringPool.Intern(Type);
            ObjectName = stringPool.Intern(ObjectName);
            SchemaName = stringPool.Intern(SchemaName);
            DatabaseName = stringPool.Intern(DatabaseName);
            
            // Intern strings in metadata
            var keys = new List<string>(Metadata.Keys);
            foreach (var key in keys)
            {
                var internedKey = stringPool.Intern(key);
                if (internedKey != key)
                {
                    var value = Metadata[key];
                    Metadata.Remove(key);
                    Metadata[internedKey] = value;
                }
                
                // Also intern string values
                if (Metadata[internedKey] is string strValue)
                {
                    Metadata[internedKey] = stringPool.Intern(strValue);
                }
            }
        }

        /// <summary>
        /// Returns a string representation of the node
        /// </summary>
        public override string ToString()
        {
            return $"{Type}:{(string.IsNullOrEmpty(DatabaseName) ? "" : $"{DatabaseName}.")}" +
                   $"{(string.IsNullOrEmpty(SchemaName) ? "" : $"{SchemaName}.")}{ObjectName}";
        }
    }
}
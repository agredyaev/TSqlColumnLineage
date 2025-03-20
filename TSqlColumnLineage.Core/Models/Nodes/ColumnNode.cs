using Newtonsoft.Json;
using TSqlColumnLineage.Core.Common.Utils;

namespace TSqlColumnLineage.Core.Models.Nodes
{
    /// <summary>
    /// Represents a column in the lineage graph with optimized memory usage
    /// </summary>
    public sealed class ColumnNode : LineageNode
    {
        /// <summary>
        /// Data type of the column
        /// </summary>
        [JsonProperty("dataType")]
        public string DataType { get; set; } = "unknown";

        /// <summary>
        /// Name of the owning table
        /// </summary>
        [JsonProperty("tableOwner")]
        public string TableOwner { get; set; } = string.Empty;

        /// <summary>
        /// Indicates if the column can contain NULL values
        /// </summary>
        [JsonProperty("isNullable")]
        public bool IsNullable { get; set; }

        /// <summary>
        /// Indicates if the column is computed
        /// </summary>
        [JsonProperty("isComputed")]
        public bool IsComputed { get; set; }
        
        /// <summary>
        /// Creates a fully qualified name for this column
        /// </summary>
        [JsonIgnore]
        public string FullyQualifiedName => $"{TableOwner}.{Name}";
        
        /// <summary>
        /// Default constructor needed for serialization
        /// </summary>
        public ColumnNode() : base()
        {
            Type = "Column";
        }
        
        /// <summary>
        /// Creates a new column node with the specified parameters
        /// </summary>
        /// <param name="id">Node ID</param>
        /// <param name="name">Column name</param>
        /// <param name="tableOwner">Owner table name</param>
        /// <param name="dataType">Data type</param>
        /// <param name="isNullable">Whether the column is nullable</param>
        /// <param name="isComputed">Whether the column is computed</param>
        /// <param name="objectName">Object name (defaults to name if not specified)</param>
        /// <param name="schemaName">Schema name</param>
        /// <param name="databaseName">Database name</param>
        public ColumnNode(
            string id,
            string name,
            string tableOwner,
            string dataType = "unknown",
            bool isNullable = false,
            bool isComputed = false,
            string objectName = null,
            string schemaName = "",
            string databaseName = "") 
            : base(id, name, "Column", objectName ?? name, schemaName, databaseName)
        {
            TableOwner = tableOwner;
            DataType = dataType;
            IsNullable = isNullable;
            IsComputed = isComputed;
        }
        
        /// <summary>
        /// Helper method to update the strings in this column node using the provided StringPool
        /// </summary>
        /// <param name="stringPool">StringPool to intern strings</param>
        public new void InternStrings(StringPool stringPool)
        {
            if (stringPool == null) return;
            
            // First, intern strings from the base class
            base.InternStrings(stringPool);
            
            // Then, intern the column-specific strings
            DataType = stringPool.Intern(DataType);
            TableOwner = stringPool.Intern(TableOwner);
        }
        
        /// <summary>
        /// Creates a deep clone of this node
        /// </summary>
        /// <returns>A new instance with the same properties</returns>
        public override ILineageNode Clone()
        {
            return (ColumnNode)base.Clone();
        }
    }
}
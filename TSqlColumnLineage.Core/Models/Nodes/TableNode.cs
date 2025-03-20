using Newtonsoft.Json;
using System.Collections.Generic;
using TSqlColumnLineage.Core.Common.Utils;

namespace TSqlColumnLineage.Core.Models.Nodes
{    
    /// <summary>
    /// Represents a table in the lineage graph (includes tables, views, CTEs, temp tables)
    /// with optimized memory usage
    /// </summary>
    public sealed class TableNode : LineageNode
    {
        /// <summary>
        /// Type of the table (Base Table, View, CTE, Temp Table, etc.)
        /// </summary>
        [JsonProperty("tableType")]
        public string TableType { get; set; } = "Unknown";

        /// <summary>
        /// List of column node IDs belonging to this table
        /// </summary>
        [JsonProperty("columns")]
        public HashSet<string> Columns { get; } = new HashSet<string>();

        /// <summary>
        /// Table alias in the query, if any
        /// </summary>
        [JsonProperty("alias")]
        public string Alias { get; set; } = string.Empty;

        /// <summary>
        /// Definition for CTEs or temporary tables
        /// </summary>
        [JsonProperty("definition")]
        public string Definition { get; set; } = string.Empty;
        
        /// <summary>
        /// Default constructor needed for serialization
        /// </summary>
        public TableNode() : base()
        {
            Type = "Table";
        }
        
        /// <summary>
        /// Creates a new table node with the specified parameters
        /// </summary>
        /// <param name="id">Node ID</param>
        /// <param name="name">Table name</param>
        /// <param name="tableType">Table type</param>
        /// <param name="alias">Table alias</param>
        /// <param name="definition">Table definition</param>
        /// <param name="objectName">Object name (defaults to name if not specified)</param>
        /// <param name="schemaName">Schema name</param>
        /// <param name="databaseName">Database name</param>
        public TableNode(
            string id,
            string name,
            string tableType = "Table",
            string alias = "",
            string definition = "",
            string objectName = null,
            string schemaName = "",
            string databaseName = "") 
            : base(id, name, "Table", objectName ?? name, schemaName, databaseName)
        {
            TableType = tableType;
            Alias = alias ?? string.Empty;
            Definition = definition ?? string.Empty;
        }
        
        /// <summary>
        /// Helper method to update the strings in this table node using the provided StringPool
        /// </summary>
        /// <param name="stringPool">StringPool to intern strings</param>
        public new void InternStrings(StringPool stringPool)
        {
            if (stringPool == null) return;
            
            // First, intern strings from the base class
            base.InternStrings(stringPool);
            
            // Then, intern the table-specific strings
            TableType = stringPool.Intern(TableType);
            Alias = stringPool.Intern(Alias);
            Definition = stringPool.Intern(Definition);
            
            // Intern column IDs
            var newColumns = new HashSet<string>();
            foreach (var column in Columns)
            {
                newColumns.Add(stringPool.Intern(column));
            }
            
            // Replace the old collection with the new one containing interned strings
            Columns.Clear();
            foreach (var column in newColumns)
            {
                Columns.Add(column);
            }
        }
        
        /// <summary>
        /// Creates a deep clone of this node
        /// </summary>
        /// <returns>A new instance with the same properties</returns>
        public override ILineageNode Clone()
        {
            var clone = (TableNode)base.Clone();
            
            // Create a new copy of the columns set
            clone.Columns.Clear();
            foreach (var column in Columns)
            {
                clone.Columns.Add(column);
            }
            
            return clone;
        }
    }
}
using Newtonsoft.Json;
using TSqlColumnLineage.Core.Common.Utils;

namespace TSqlColumnLineage.Core.Models.Nodes
{
    /// <summary>
    /// Represents an expression (function, calculation) in the lineage graph
    /// with optimized memory usage
    /// </summary>
    public sealed class ExpressionNode : LineageNode
    {
        /// <summary>
        /// Type of the expression (Function, Scalar, Aggregation, Case, etc.)
        /// </summary>
        [JsonProperty("expressionType")]
        public string ExpressionType { get; set; } = string.Empty;

        /// <summary>
        /// Original text of the expression
        /// </summary>
        [JsonProperty("expression")]
        public string Expression { get; set; } = string.Empty;

        /// <summary>
        /// Resulting data type of the expression
        /// </summary>
        [JsonProperty("resultType")]
        public string ResultType { get; set; } = string.Empty;
        
        /// <summary>
        /// Table owner for expression compatibility with column operations
        /// </summary>
        [JsonProperty("tableOwner")]
        public string TableOwner { get; set; } = string.Empty;
        
        /// <summary>
        /// Default constructor needed for serialization
        /// </summary>
        public ExpressionNode() : base()
        {
            Type = "Expression";
        }
        
        /// <summary>
        /// Creates a new expression node with the specified parameters
        /// </summary>
        /// <param name="id">Node ID</param>
        /// <param name="name">Expression name</param>
        /// <param name="expression">Expression text</param>
        /// <param name="expressionType">Expression type</param>
        /// <param name="resultType">Result data type</param>
        /// <param name="tableOwner">Owner table name</param>
        /// <param name="objectName">Object name (defaults to name if not specified)</param>
        /// <param name="schemaName">Schema name</param>
        /// <param name="databaseName">Database name</param>
        public ExpressionNode(
            string id,
            string name,
            string expression,
            string expressionType = "Expression",
            string resultType = "unknown",
            string tableOwner = "",
            string objectName = null,
            string schemaName = "",
            string databaseName = "")
            : base(id, name, "Expression", objectName ?? name, schemaName, databaseName)
        {
            Expression = expression ?? string.Empty;
            ExpressionType = expressionType ?? "Expression";
            ResultType = resultType ?? "unknown";
            TableOwner = tableOwner ?? string.Empty;
        }
        
        /// <summary>
        /// Helper method to update the strings in this expression node using the provided StringPool
        /// </summary>
        /// <param name="stringPool">StringPool to intern strings</param>
        public new void InternStrings(StringPool stringPool)
        {
            if (stringPool == null) return;
            
            // First, intern strings from the base class
            base.InternStrings(stringPool);
            
            // Then, intern the expression-specific strings
            ExpressionType = stringPool.Intern(ExpressionType);
            // Skip interning the Expression value as it can be large and unique
            ResultType = stringPool.Intern(ResultType);
            TableOwner = stringPool.Intern(TableOwner);
        }
        
        /// <summary>
        /// Creates a deep clone of this node
        /// </summary>
        /// <returns>A new instance with the same properties</returns>
        public override ILineageNode Clone()
        {
            return (ExpressionNode)base.Clone();
        }
    }
}
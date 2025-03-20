using System;
using Newtonsoft.Json;

namespace TSqlColumnLineage.Core.Models.Nodes
{
    /// <summary>
    /// Represents an expression (function, calculation) in the lineage graph
    /// </summary>
    public class ExpressionNode : LineageNode
    {
        /// <summary>
        /// Initialize a new expression node with default properties
        /// </summary>
        public ExpressionNode()
        {
            Type = "Expression";
        }

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
        /// Creates a deep clone of this node
        /// </summary>
        /// <returns>A new instance with the same properties</returns>
        public override ILineageNode Clone()
        {
            return (ExpressionNode)base.Clone();
        }
    }
}
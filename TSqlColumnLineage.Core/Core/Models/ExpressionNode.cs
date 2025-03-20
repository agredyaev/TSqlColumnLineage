using System;
using Newtonsoft.Json;

namespace TSqlColumnLineage.Core.Models
{
    /// <summary>
    /// Class representing an expression (function, calculation) in the lineage graph
    /// </summary>
    public class ExpressionNode : LineageNode
    {
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
        /// Provides explicit conversion from ExpressionNode to ColumnNode
        /// </summary>
        /// <param name="expression">Expression node to convert</param>
        public static explicit operator ColumnNode(ExpressionNode expression)
        {
            if (expression == null)
                return null!;
                
            return new ColumnNode
            {
                Id = expression.Id,
                Name = expression.Name,
                ObjectName = expression.ObjectName,
                SchemaName = expression.SchemaName,
                DatabaseName = expression.DatabaseName,
                TableOwner = expression.TableOwner,
                DataType = expression.ResultType ?? "unknown",
                IsComputed = true,
                Metadata = new Dictionary<string, object>(expression.Metadata)
                {
                    ["SourceExpression"] = expression.Expression,
                    ["ExpressionType"] = expression.ExpressionType
                }
            };
        }
    }
}

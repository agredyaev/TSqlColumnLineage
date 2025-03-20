using TSqlColumnLineage.Core.Models;
using System;

namespace TSqlColumnLineage.Core
{
    /// <summary>
    /// Factory for creating LineageEdge instances
    /// </summary>
    public class LineageEdgeFactory
    {
        /// <summary>
        /// Creates a new LineageEdge with a generated unique ID
        /// </summary>
        /// <param name="sourceId">Source node ID</param>
        /// <param name="targetId">Target node ID</param>
        /// <param name="type">Edge type (e.g. 'direct', 'indirect', 'parameter')</param>
        /// <param name="operation">Operation description (e.g. 'select', 'join', 'union')</param>
        /// <param name="sqlExpression">SQL expression (optional)</param>
        /// <returns>A new LineageEdge instance</returns>
        public static LineageEdge CreateEdge(string sourceId, string targetId, string type, string operation, string sqlExpression = "")
        {
            if (string.IsNullOrEmpty(sourceId))
                throw new ArgumentNullException(nameof(sourceId), "Source ID cannot be null or empty");
                
            if (string.IsNullOrEmpty(targetId))
                throw new ArgumentNullException(nameof(targetId), "Target ID cannot be null or empty");
                
            if (string.IsNullOrEmpty(type))
                throw new ArgumentNullException(nameof(type), "Edge type cannot be null or empty");
                
            if (string.IsNullOrEmpty(operation))
                throw new ArgumentNullException(nameof(operation), "Operation cannot be null or empty");
                
            return new LineageEdge
            {
                Id = Guid.NewGuid().ToString(),
                SourceId = sourceId,
                TargetId = targetId,
                Type = type,
                Operation = operation,
                SqlExpression = sqlExpression ?? string.Empty
            };
        }
        
        /// <summary>
        /// Creates a direct column-to-column edge
        /// </summary>
        /// <param name="sourceId">Source column ID</param>
        /// <param name="targetId">Target column ID</param>
        /// <param name="operation">Operation that created this relationship</param>
        /// <param name="sqlExpression">Optional SQL expression</param>
        /// <returns>A direct lineage edge</returns>
        public static LineageEdge CreateDirectEdge(string sourceId, string targetId, string operation, string sqlExpression = "")
        {
            return CreateEdge(sourceId, targetId, "direct", operation, sqlExpression);
        }
        
        /// <summary>
        /// Creates an indirect column-to-column edge (through expression or transformation)
        /// </summary>
        /// <param name="sourceId">Source column ID</param>
        /// <param name="targetId">Target column ID</param>
        /// <param name="operation">Operation that created this relationship</param>
        /// <param name="sqlExpression">Optional SQL expression</param>
        /// <returns>An indirect lineage edge</returns>
        public static LineageEdge CreateIndirectEdge(string sourceId, string targetId, string operation, string sqlExpression = "")
        {
            return CreateEdge(sourceId, targetId, "indirect", operation, sqlExpression);
        }
        
        /// <summary>
        /// Creates a parameter mapping edge
        /// </summary>
        /// <param name="sourceId">Source column or variable ID</param>
        /// <param name="targetId">Target parameter ID</param>
        /// <param name="sqlExpression">Optional SQL expression</param>
        /// <returns>A parameter lineage edge</returns>
        public static LineageEdge CreateParameterEdge(string sourceId, string targetId, string sqlExpression = "")
        {
            return CreateEdge(sourceId, targetId, "parameter", "map", sqlExpression);
        }
    }
}

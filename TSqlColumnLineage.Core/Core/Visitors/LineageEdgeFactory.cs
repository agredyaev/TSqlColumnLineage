// This file is deprecated.
// Please use TSqlColumnLineage.Core.LineageEdgeFactory instead.
// This file is kept temporarily to avoid breaking existing code.

using TSqlColumnLineage.Core.Models;
using System;

namespace TSqlColumnLineage.Core.Visitors
{
    /// <summary>
    /// Factory for creating LineageEdge instances
    /// </summary>
    [Obsolete("This class is deprecated. Please use TSqlColumnLineage.Core.LineageEdgeFactory instead.")]
    public class LineageEdgeFactory
    {
        /// <summary>
        /// Creates a new LineageEdge
        /// </summary>
        /// <param name="sourceId">Source node ID</param>
        /// <param name="targetId">Target node ID</param>
        /// <param name="type">Edge type</param>
        /// <param name="operation">Operation description</param>
        /// <param name="sqlExpression">SQL expression (optional)</param>
        /// <returns>A new LineageEdge instance</returns>
        public static LineageEdge CreateEdge(string sourceId, string targetId, string type, string operation, string sqlExpression = null)
        {
            // Delegate to the new implementation
            return Core.LineageEdgeFactory.CreateEdge(sourceId, targetId, type, operation, sqlExpression);
        }
    }
}

using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Visitors;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TSqlColumnLineage.Core.Handlers
{
    /// <summary>
    /// Handler for window functions (OVER clause)
    /// </summary>
    public class WindowFunctionHandler
    {
        private readonly ColumnLineageVisitor _visitor;
        private readonly LineageGraph _graph;
        private readonly LineageContext _context;
        private readonly ILogger _logger;

        public WindowFunctionHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger logger)
        {
            _visitor = visitor ?? throw new ArgumentNullException(nameof(visitor));
            _graph = graph ?? throw new ArgumentNullException(nameof(graph));
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _logger = logger;
        }

        /// <summary>
        /// Processes OVER clause for a window function
        /// </summary>
        /// <param name="node">OVER node</param>
        /// <param name="functionNode">Function node to which OVER applies</param>
        /// <param name="columnContext">Current column context</param>
        public void ProcessOverClause(OverClause node, ExpressionNode functionNode, ColumnLineageContext columnContext)
        {
            _logger?.LogDebug($"Processing OVER clause at Line {node.StartLine}");

            // Process PARTITION BY expressions
            if (node.Partitions != null && node.Partitions.Count > 0)
            {
                _logger?.LogDebug("Processing PARTITION BY expressions");

                foreach (var partition in node.Partitions)
                {
                    // Create temporary context for processing PARTITION BY expressions
                    var partitionContext = new ColumnLineageContext
                    {
                        TargetColumn = (ColumnNode)functionNode,
                        DependencyType = "partition"
                    };

                    ProcessExpressionWithContext(partition, partitionContext);
                }
            }

            // Process ORDER BY expressions
            if (node.OrderByClause != null && node.OrderByClause.OrderByElements != null)
            {
                _logger?.LogDebug("Processing ORDER BY expressions in OVER");

                foreach (var orderBy in node.OrderByClause.OrderByElements)
                {
                    // Create temporary context for processing ORDER BY expressions
                    var orderByContext = new ColumnLineageContext
                    {
                        TargetColumn = (ColumnNode)functionNode,
                        DependencyType = "order"
                    };

                    ProcessExpressionWithContext(orderBy.Expression, orderByContext);
                }
            }

            // Process window frame definition (ROWS/RANGE BETWEEN)
            if (node.WindowFrameClause != null)
            {
                _logger?.LogDebug("Processing window frame clause");

                // Record the window frame type in metadata
                functionNode.Metadata["windowFrameType"] = node.WindowFrameClause.GetType().Name;
                
                // Log that we've seen the window frame clause
                _logger?.LogDebug($"Processing window frame of type {node.WindowFrameClause.GetType().Name}");
                
                // We can't directly process the WindowFrameClause as it's not a ScalarExpression
                // Instead, extract any scalar expressions it might contain
                
                // The specific properties to access will depend on the version of SQL Server ScriptDom
                // This is a generic approach that should work for most cases
                try 
                {
                    // Try to extract any scalar expressions from the window frame clause
                    // through visiting the node directly
                    _visitor.Visit(node.WindowFrameClause);
                }
                catch (Exception ex)
                {
                    _logger?.LogDebug($"Error processing window frame: {ex.Message}");
                }
            }

            // After processing OVER, add metadata to the function node
            functionNode.Metadata["isWindowFunction"] = true;
            functionNode.Metadata["windowDefinition"] = _visitor.GetSqlText(node);
        }

        /// <summary>
        /// Processes an expression with a given context
        /// </summary>
        private void ProcessExpressionWithContext(ScalarExpression expression, ColumnLineageContext context)
        {
            // Save previous context
            string key = context?.TargetColumn?.Id ?? "default";
            var previousContext = _context.GetColumnContext(key);
            _context.SetColumnContext(key, context?.TargetColumn);

            try
            {
                // Process expression
                _visitor.Visit(expression);
            }
            finally
            {
                // Restore previous context
                _context.SetColumnContext(key, previousContext);
            }
        }
    }
}

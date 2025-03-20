using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Visitors;
using System;
using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Handlers
{
    /// <summary>
    /// Handler for CASE expressions
    /// </summary>
    public class CaseExpressionHandler
    {
        private readonly ColumnLineageVisitor _visitor;
        private readonly LineageGraph _graph;
        private readonly LineageContext _context;
        private readonly ILogger _logger;

        public CaseExpressionHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger logger)
        {
            _visitor = visitor ?? throw new ArgumentNullException(nameof(visitor));
            _graph = graph ?? throw new ArgumentNullException(nameof(graph));
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _logger = logger;
        }

        /// <summary>
        /// Processes CASE expression
        /// </summary>
        public void ProcessCaseExpression(SearchedCaseExpression node, ColumnLineageContext columnContext)
        {
            _logger?.LogDebug($"Processing CASE expression at Line {node.StartLine}");

            // Create node for the CASE expression
            var caseNode = new ExpressionNode
            {
                Id = _visitor.CreateNodeId("EXPR", "CASE"),
                Name = "CASE",
                ObjectName = "CASE",
                ExpressionType = "Case",
                Expression = _visitor.GetSqlText(node)
            };

            _graph.AddNode(caseNode);

            // If there is a target column context, create a relationship
            if (columnContext?.TargetColumn != null)
            {
                var edge = new LineageEdge
                {
                    Id = _visitor.CreateRandomId(),
                    SourceId = caseNode.Id,
                    TargetId = columnContext.TargetColumn.Id,
                    Type = "transform",
                    Operation = "CASE",
                    SqlExpression = _visitor.GetSqlText(node)
                };

                _graph.AddEdge(edge);
            }

            // Process WHEN clauses
            if (node.WhenClauses != null)
            {
                foreach (var whenClause in node.WhenClauses)
                {
                    // Process condition
                    if (whenClause.WhenExpression != null)
                    {
                        var whenContext = new ColumnLineageContext
                        {
                            TargetColumn = (ColumnNode)caseNode,
                            DependencyType = "case_condition"
                        };

                        ProcessExpressionWithContext(whenClause.WhenExpression, whenContext);
                    }

                    // Process result
                    if (whenClause.ThenExpression != null)
                    {
                        var thenContext = new ColumnLineageContext
                        {
                            TargetColumn = (ColumnNode)caseNode,
                            DependencyType = "case_result"
                        };

                        ProcessExpressionWithContext(whenClause.ThenExpression, thenContext);
                    }
                }
            }

            // Process ELSE clause
            if (node.ElseExpression != null)
            {
                var elseContext = new ColumnLineageContext
                {
                    TargetColumn = (ColumnNode)caseNode,
                    DependencyType = "case_else"
                };

                ProcessExpressionWithContext(node.ElseExpression, elseContext);
            }
        }

        /// <summary>
        /// Processes simple CASE expression
        /// </summary>
        public void ProcessSimpleCaseExpression(SimpleCaseExpression node, ColumnLineageContext columnContext)
        {
            _logger?.LogDebug($"Processing simple CASE expression at Line {node.StartLine}");

            // Create node for the CASE expression
            var caseNode = new ExpressionNode
            {
                Id = _visitor.CreateNodeId("EXPR", "CASE"),
                Name = "CASE",
                ObjectName = "CASE",
                ExpressionType = "Case",
                Expression = _visitor.GetSqlText(node)
            };

            _graph.AddNode(caseNode);

            // If there is a target column context, create a relationship
            if (columnContext?.TargetColumn != null)
            {
                var edge = new LineageEdge
                {
                    Id = _visitor.CreateRandomId(),
                    SourceId = caseNode.Id,
                    TargetId = columnContext.TargetColumn.Id,
                    Type = "transform",
                    Operation = "CASE",
                    SqlExpression = _visitor.GetSqlText(node)
                };

                _graph.AddEdge(edge);
            }

            // Process input expression
            if (node.InputExpression != null)
            {
                var inputContext = new ColumnLineageContext
                {
                    TargetColumn = (ColumnNode)caseNode,
                    DependencyType = "case_input"
                };

                ProcessExpressionWithContext(node.InputExpression, inputContext);
            }

            // Process WHEN clauses
            if (node.WhenClauses != null)
            {
                foreach (var whenClause in node.WhenClauses)
                {
                    // Process condition
                    if (whenClause.WhenExpression != null)
                    {
                        var whenContext = new ColumnLineageContext
                        {
                            TargetColumn = (ColumnNode)caseNode,
                            DependencyType = "case_condition"
                        };

                        ProcessExpressionWithContext(whenClause.WhenExpression, whenContext);
                    }

                    // Process result
                    if (whenClause.ThenExpression != null)
                    {
                        var thenContext = new ColumnLineageContext
                        {
                            TargetColumn = (ColumnNode)caseNode,
                            DependencyType = "case_result"
                        };

                        ProcessExpressionWithContext(whenClause.ThenExpression, thenContext);
                    }
                }
            }

            // Process ELSE clause
            if (node.ElseExpression != null)
            {
                var elseContext = new ColumnLineageContext
                {
                    TargetColumn = (ColumnNode)caseNode,
                    DependencyType = "case_else"
                };

                ProcessExpressionWithContext(node.ElseExpression, elseContext);
            }
        }

        /// <summary>
        /// Processes COALESCE expression
        /// </summary>
        public void ProcessCoalesceExpression(CoalesceExpression node, ColumnLineageContext columnContext)
        {
            _logger?.LogDebug($"Processing COALESCE expression at Line {node.StartLine}");

            // Create node for the COALESCE expression
            var coalesceNode = new ExpressionNode
            {
                Id = _visitor.CreateNodeId("EXPR", "COALESCE"),
                Name = "COALESCE",
                ObjectName = "COALESCE",
                ExpressionType = "Coalesce",
                Expression = _visitor.GetSqlText(node)
            };

            _graph.AddNode(coalesceNode);

            // If there is a target column context, create a relationship
            if (columnContext?.TargetColumn != null)
            {
                var edge = new LineageEdge
                {
                    Id = _visitor.CreateRandomId(),
                    SourceId = coalesceNode.Id,
                    TargetId = columnContext.TargetColumn.Id,
                    Type = "transform",
                    Operation = "COALESCE",
                    SqlExpression = _visitor.GetSqlText(node)
                };

                _graph.AddEdge(edge);
            }

            // Process COALESCE arguments
            if (node.Expressions != null)
            {
                foreach (var expr in node.Expressions)
                {
                    var exprContext = new ColumnLineageContext
                    {
                        TargetColumn = (ColumnNode)coalesceNode,
                        DependencyType = "coalesce_arg"
                    };

                    ProcessExpressionWithContext(expr, exprContext);
                }
            }
        }

        /// <summary>
        /// Processes NULLIF expression
        /// </summary>
        public void ProcessNullIfExpression(NullIfExpression node, ColumnLineageContext columnContext)
        {
            _logger?.LogDebug($"Processing NULLIF expression at Line {node.StartLine}");

            // Create node for the NULLIF expression
            var nullifNode = new ExpressionNode
            {
                Id = _visitor.CreateNodeId("EXPR", "NULLIF"),
                Name = "NULLIF",
                ObjectName = "NULLIF",
                ExpressionType = "NullIf",
                Expression = _visitor.GetSqlText(node)
            };

            _graph.AddNode(nullifNode);

            // If there is a target column context, create a relationship
            if (columnContext?.TargetColumn != null)
            {
                var edge = new LineageEdge
                {
                    Id = _visitor.CreateRandomId(),
                    SourceId = nullifNode.Id,
                    TargetId = columnContext.TargetColumn.Id,
                    Type = "transform",
                    Operation = "NULLIF",
                    SqlExpression = _visitor.GetSqlText(node)
                };

                _graph.AddEdge(edge);
            }

            // Process first argument
            if (node.FirstExpression != null)
            {
                var firstContext = new ColumnLineageContext
                {
                    TargetColumn = (ColumnNode)nullifNode,
                    DependencyType = "nullif_first"
                };

                ProcessExpressionWithContext(node.FirstExpression, firstContext);
            }

            // Process second argument
            if (node.SecondExpression != null)
            {
                var secondContext = new ColumnLineageContext
                {
                    TargetColumn = (ColumnNode)nullifNode,
                    DependencyType = "nullif_second"
                };

                ProcessExpressionWithContext(node.SecondExpression, secondContext);
            }
        }

        /// <summary>
        /// Processes an expression with a given context
        /// </summary>
        private void ProcessExpressionWithContext(TSqlFragment expression, ColumnLineageContext context)
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

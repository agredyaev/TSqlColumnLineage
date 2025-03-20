using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Visitors.Specialized
{
    /// <summary>
    /// Specialized visitor for handling expressions
    /// </summary>
    public class ExpressionVisitor : BaseVisitor
    {
        /// <summary>
        /// Creates a new expression visitor
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="logger">Logger (optional)</param>
        public ExpressionVisitor(VisitorContext context, ILogger logger = null)
            : base(context, logger)
        {
        }
        
        /// <summary>
        /// Process CASE expression
        /// </summary>
        public override void ExplicitVisit(SearchedCaseExpression node)
        {
            LogDebug($"Processing CASE expression at line {node.StartLine}");
            
            // Get current target column if available
            ColumnNode targetColumn = null;
            if (Context.State.TryGetValue("CurrentColumn", out var currentColumn) && 
                currentColumn is ColumnNode column)
            {
                targetColumn = column;
            }
            
            // Create an expression node for the CASE
            var expressionNode = new ExpressionNode
            {
                Id = CreateNodeId("EXPR", "CASE_" + Guid.NewGuid().ToString().Substring(0, 8)),
                Name = targetColumn?.Name ?? "CASE_Expression",
                ObjectName = GetSqlText(node),
                ExpressionType = "Case",
                Expression = GetSqlText(node),
                TableOwner = targetColumn?.TableOwner ?? string.Empty,
                ResultType = targetColumn?.DataType ?? "unknown"
            };
            
            Graph.AddNode(expressionNode);
            
            // Link to target column if available
            if (targetColumn != null)
            {
                var targetEdge = CreateDirectEdge(
                    expressionNode.Id,
                    targetColumn.Id,
                    "evaluate",
                    $"CASE expression result -> {targetColumn.TableOwner}.{targetColumn.Name}"
                );
                
                Graph.AddEdge(targetEdge);
            }
            
            // Process WHEN clauses
            if (node.WhenClauses != null)
            {
                foreach (var whenClause in node.WhenClauses)
                {
                    // Set context for condition processing
                    Context.State["ProcessingWhenCondition"] = true;
                    
                    try
                    {
                        // Process condition
                        if (whenClause.WhenExpression != null)
                        {
                            Visit(whenClause.WhenExpression);
                            
                            // Extract column references from the condition
                            var conditionRefs = ExtractBooleanConditionColumns(whenClause.WhenExpression);
                            foreach (var colRef in conditionRefs)
                            {
                                LinkColumnToExpression(colRef, expressionNode, "condition");
                            }
                        }
                    }
                    finally
                    {
                        Context.State.Remove("ProcessingWhenCondition");
                    }
                    
                    // Process result
                    if (whenClause.ThenExpression != null)
                    {
                        Visit(whenClause.ThenExpression);
                        
                        // Extract column references from the result
                        var resultRefs = new List<ColumnReferenceExpression>();
                        ExtractColumnReferences(whenClause.ThenExpression, resultRefs);
                        foreach (var colRef in resultRefs)
                        {
                            LinkColumnToExpression(colRef, expressionNode, "result");
                        }
                    }
                }
            }
            
            // Process ELSE clause
            if (node.ElseExpression != null)
            {
                Visit(node.ElseExpression);
                
                // Extract column references from the ELSE clause
                var elseRefs = new List<ColumnReferenceExpression>();
                ExtractColumnReferences(node.ElseExpression, elseRefs);
                foreach (var colRef in elseRefs)
                {
                    LinkColumnToExpression(colRef, expressionNode, "else");
                }
            }
        }
        
        /// <summary>
        /// Process Simple CASE expression
        /// </summary>
        public override void ExplicitVisit(SimpleCaseExpression node)
        {
            LogDebug($"Processing simple CASE expression at line {node.StartLine}");
            
            // Get current target column if available
            ColumnNode targetColumn = null;
            if (Context.State.TryGetValue("CurrentColumn", out var currentColumn) && 
                currentColumn is ColumnNode column)
            {
                targetColumn = column;
            }
            
            // Create an expression node for the CASE
            var expressionNode = new ExpressionNode
            {
                Id = CreateNodeId("EXPR", "CASE_" + Guid.NewGuid().ToString().Substring(0, 8)),
                Name = targetColumn?.Name ?? "CASE_Expression",
                ObjectName = GetSqlText(node),
                ExpressionType = "Case",
                Expression = GetSqlText(node),
                TableOwner = targetColumn?.TableOwner ?? string.Empty,
                ResultType = targetColumn?.DataType ?? "unknown"
            };
            
            Graph.AddNode(expressionNode);
            
            // Link to target column if available
            if (targetColumn != null)
            {
                var targetEdge = CreateDirectEdge(
                    expressionNode.Id,
                    targetColumn.Id,
                    "evaluate",
                    $"CASE expression result -> {targetColumn.TableOwner}.{targetColumn.Name}"
                );
                
                Graph.AddEdge(targetEdge);
            }
            
            // Process input expression (the value being compared in CASE)
            if (node.InputExpression != null)
            {
                Visit(node.InputExpression);
                
                // Extract column references from input
                var inputRefs = new List<ColumnReferenceExpression>();
                ExtractColumnReferences(node.InputExpression, inputRefs);
                foreach (var colRef in inputRefs)
                {
                    LinkColumnToExpression(colRef, expressionNode, "input");
                }
            }
            
            // Process WHEN clauses
            if (node.WhenClauses != null)
            {
                foreach (var whenClause in node.WhenClauses)
                {
                    // Process when expression (the value to compare with)
                    if (whenClause.WhenExpression != null)
                    {
                        Visit(whenClause.WhenExpression);
                        
                        // Extract column references from when value
                        var whenRefs = new List<ColumnReferenceExpression>();
                        ExtractColumnReferences(whenClause.WhenExpression, whenRefs);
                        foreach (var colRef in whenRefs)
                        {
                            LinkColumnToExpression(colRef, expressionNode, "when");
                        }
                    }
                    
                    // Process result
                    if (whenClause.ThenExpression != null)
                    {
                        Visit(whenClause.ThenExpression);
                        
                        // Extract column references from result
                        var resultRefs = new List<ColumnReferenceExpression>();
                        ExtractColumnReferences(whenClause.ThenExpression, resultRefs);
                        foreach (var colRef in resultRefs)
                        {
                            LinkColumnToExpression(colRef, expressionNode, "result");
                        }
                    }
                }
            }
            
            // Process ELSE clause
            if (node.ElseExpression != null)
            {
                Visit(node.ElseExpression);
                
                // Extract column references from ELSE
                var elseRefs = new List<ColumnReferenceExpression>();
                ExtractColumnReferences(node.ElseExpression, elseRefs);
                foreach (var colRef in elseRefs)
                {
                    LinkColumnToExpression(colRef, expressionNode, "else");
                }
            }
        }
        
        /// <summary>
        /// Process COALESCE expression
        /// </summary>
        public override void ExplicitVisit(CoalesceExpression node)
        {
            LogDebug($"Processing COALESCE expression at line {node.StartLine}");
            
            // Get current target column if available
            ColumnNode targetColumn = null;
            if (Context.State.TryGetValue("CurrentColumn", out var currentColumn) && 
                currentColumn is ColumnNode column)
            {
                targetColumn = column;
            }
            
            // Create an expression node for COALESCE
            var expressionNode = new ExpressionNode
            {
                Id = CreateNodeId("EXPR", "COALESCE_" + Guid.NewGuid().ToString().Substring(0, 8)),
                Name = targetColumn?.Name ?? "COALESCE_Expression",
                ObjectName = GetSqlText(node),
                ExpressionType = "Coalesce",
                Expression = GetSqlText(node),
                TableOwner = targetColumn?.TableOwner ?? string.Empty,
                ResultType = targetColumn?.DataType ?? "unknown"
            };
            
            Graph.AddNode(expressionNode);
            
            // Link to target column if available
            if (targetColumn != null)
            {
                var targetEdge = CreateDirectEdge(
                    expressionNode.Id,
                    targetColumn.Id,
                    "evaluate",
                    $"COALESCE result -> {targetColumn.TableOwner}.{targetColumn.Name}"
                );
                
                Graph.AddEdge(targetEdge);
            }
            
            // Process each argument
            if (node.Expressions != null)
            {
                foreach (var expr in node.Expressions)
                {
                    Visit(expr);
                    
                    // Extract column references
                    var refs = new List<ColumnReferenceExpression>();
                    ExtractColumnReferences(expr, refs);
                    foreach (var colRef in refs)
                    {
                        LinkColumnToExpression(colRef, expressionNode, "argument");
                    }
                }
            }
        }
        
        /// <summary>
        /// Process window function (OVER clause)
        /// </summary>
        public override void ExplicitVisit(OverClause node)
        {
            LogDebug($"Processing OVER clause at line {node.StartLine}");
            
            // Get current expression node if available (from parent function)
            if (Context.State.TryGetValue("CurrentFunction", out var currentFunc) && 
                currentFunc is ExpressionNode functionNode)
            {
                // Add window function info to the function node
                functionNode.Metadata["WindowFunction"] = true;
                functionNode.Metadata["WindowDefinition"] = GetSqlText(node);
                
                // Process PARTITION BY
                if (node.Partitions != null && node.Partitions.Count > 0)
                {
                    functionNode.Metadata["HasPartitionBy"] = true;
                    
                    // Process each partition column
                    foreach (var partition in node.Partitions)
                    {
                        Visit(partition);
                        
                        // Extract column references
                        var refs = new List<ColumnReferenceExpression>();
                        ExtractColumnReferences(partition, refs);
                        foreach (var colRef in refs)
                        {
                            LinkColumnToExpression(colRef, functionNode, "partition");
                        }
                    }
                }
                
                // Process ORDER BY
                if (node.OrderByClause != null)
                {
                    functionNode.Metadata["HasOrderBy"] = true;
                    Visit(node.OrderByClause);
                    
                    // Extract columns from ORDER BY
                    if (node.OrderByClause.OrderByElements != null)
                    {
                        foreach (var orderBy in node.OrderByClause.OrderByElements)
                        {
                            if (orderBy.Expression != null)
                            {
                                // Extract column references
                                var refs = new List<ColumnReferenceExpression>();
                                ExtractColumnReferences(orderBy.Expression, refs);
                                foreach (var colRef in refs)
                                {
                                    LinkColumnToExpression(colRef, functionNode, "order");
                                }
                            }
                        }
                    }
                }
                
                // Process window frame
                if (node.WindowFrameClause != null)
                {
                    functionNode.Metadata["HasWindowFrame"] = true;
                    functionNode.Metadata["WindowFrameType"] = node.WindowFrameClause.GetType().Name;
                    
                    // Extract any columns used in the window frame bounds
                    var startBound = node.WindowFrameClause.StartBound;
                    var endBound = node.WindowFrameClause.EndBound;
                    
                    if (startBound?.Expression != null)
                    {
                        var refs = new List<ColumnReferenceExpression>();
                        ExtractColumnReferences(startBound.Expression, refs);
                        foreach (var colRef in refs)
                        {
                            LinkColumnToExpression(colRef, functionNode, "windowFrame");
                        }
                    }
                    
                    if (endBound?.Expression != null)
                    {
                        var refs = new List<ColumnReferenceExpression>();
                        ExtractColumnReferences(endBound.Expression, refs);
                        foreach (var colRef in refs)
                        {
                            LinkColumnToExpression(colRef, functionNode, "windowFrame");
                        }
                    }
                }
            }
        }
        
        /// <summary>
        /// Process function call
        /// </summary>
        public override void ExplicitVisit(FunctionCall node)
        {
            LogDebug($"Processing function call {node.FunctionName?.Value} at line {node.StartLine}");
            
            // Get current target column if available
            ColumnNode targetColumn = null;
            if (Context.State.TryGetValue("CurrentColumn", out var currentColumn) && 
                currentColumn is ColumnNode column)
            {
                targetColumn = column;
            }
            
            // Create an expression node for the function
            var functionNode = new ExpressionNode
            {
                Id = CreateNodeId("EXPR", $"FUNC_{node.FunctionName?.Value ?? "Unknown"}_{Guid.NewGuid().ToString().Substring(0, 8)}"),
                Name = targetColumn?.Name ?? $"{node.FunctionName?.Value ?? "Function"}_Expression",
                ObjectName = GetSqlText(node),
                ExpressionType = "Function",
                Expression = GetSqlText(node),
                TableOwner = targetColumn?.TableOwner ?? string.Empty,
                ResultType = targetColumn?.DataType ?? "unknown"
            };
            
            // Set function specific metadata
            functionNode.Metadata["FunctionName"] = node.FunctionName?.Value;
            functionNode.Metadata["IsWindowFunction"] = node.OverClause != null;
            functionNode.Metadata["IsAggregate"] = IsPotentialAggregateFunction(node.FunctionName?.Value);
            
            Graph.AddNode(functionNode);
            
            // Link to target column if available
            if (targetColumn != null)
            {
                var targetEdge = CreateDirectEdge(
                    functionNode.Id,
                    targetColumn.Id,
                    "evaluate",
                    $"Function result -> {targetColumn.TableOwner}.{targetColumn.Name}"
                );
                
                Graph.AddEdge(targetEdge);
            }
            
            // Set current function context
            Context.State["CurrentFunction"] = functionNode;
            
            try
            {
                // Process function parameters
                if (node.Parameters != null)
                {
                    foreach (var param in node.Parameters)
                    {
                        if (param is ScalarExpression scalarParam)
                        {
                            Visit(scalarParam);
                            
                            // Extract column references
                            var refs = new List<ColumnReferenceExpression>();
                            ExtractColumnReferences(scalarParam, refs);
                            foreach (var colRef in refs)
                            {
                                LinkColumnToExpression(colRef, functionNode, "parameter");
                            }
                        }
                    }
                }
                
                // Process OVER clause if present
                if (node.OverClause != null)
                {
                    Visit(node.OverClause);
                }
            }
            finally
            {
                // Clear function context
                Context.State.Remove("CurrentFunction");
            }
        }
        
        /// <summary>
        /// Links a column reference to an expression node
        /// </summary>
        private void LinkColumnToExpression(ColumnReferenceExpression colRef, ExpressionNode expressionNode, string operation)
        {
            if (colRef == null || expressionNode == null) return;
            
            string columnName = colRef.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value;
            string tableName = null;
            
            // If there's a table specifier
            if (colRef.MultiPartIdentifier?.Identifiers.Count > 1)
            {
                tableName = colRef.MultiPartIdentifier.Identifiers[0].Value;
                
                // Resolve alias if needed
                if (LineageContext.TableAliases.TryGetValue(tableName, out var resolvedTable))
                {
                    tableName = resolvedTable;
                }
            }
            
            // Find the column node
            ColumnNode columnNode;
            
            if (!string.IsNullOrEmpty(tableName))
            {
                // Specific table referenced
                columnNode = Graph.GetColumnNode(tableName, columnName);
                
                if (columnNode == null)
                {
                    // Create it if it doesn't exist
                    columnNode = Context.GetOrCreateColumnNode(tableName, columnName);
                }
            }
            else
            {
                // Try to find the column in any table
                columnNode = LineageContext.Tables.Values
                    .Select(t => Graph.GetColumnNode(t.Name, columnName))
                    .FirstOrDefault(c => c != null);
                
                if (columnNode == null)
                {
                    // No matching column found
                    LogWarning($"Could not find column {columnName} referenced in expression");
                    return;
                }
            }
            
            // Create an indirect edge from column to expression
            var edge = CreateIndirectEdge(
                columnNode.Id,
                expressionNode.Id,
                operation,
                $"{columnNode.TableOwner}.{columnNode.Name} used in {expressionNode.ExpressionType} as {operation}"
            );
            
            Graph.AddEdge(edge);
            LogDebug($"Created indirect lineage edge: {columnNode.TableOwner}.{columnNode.Name} -> {expressionNode.Name} (as {operation})");
        }
        
        /// <summary>
        /// Extracts column references from an expression
        /// </summary>
        private void ExtractColumnReferences(ScalarExpression expr, List<ColumnReferenceExpression> columnRefs)
        {
            if (expr == null) return;
            
            // Direct column reference
            if (expr is ColumnReferenceExpression colRef)
            {
                columnRefs.Add(colRef);
                return;
            }
            
            // Binary expressions (e.g., a + b)
            if (expr is BinaryExpression binaryExpr)
            {
                ExtractColumnReferences(binaryExpr.FirstExpression, columnRefs);
                ExtractColumnReferences(binaryExpr.SecondExpression, columnRefs);
                return;
            }
            
            // Function calls
            if (expr is FunctionCall functionCall)
            {
                foreach (var parameter in functionCall.Parameters)
                {
                    if (parameter is ScalarExpression scalarParam)
                    {
                        ExtractColumnReferences(scalarParam, columnRefs);
                    }
                }
                return;
            }
            
            // Parentheses
            if (expr is ParenthesisExpression parenExpr)
            {
                ExtractColumnReferences(parenExpr.Expression, columnRefs);
                return;
            }
            
            // Unary expressions
            if (expr is UnaryExpression unaryExpr)
            {
                ExtractColumnReferences(unaryExpr.Expression, columnRefs);
                return;
            }
            
            // CASE expressions
            if (expr is SearchedCaseExpression caseExpr)
            {
                foreach (var whenClause in caseExpr.WhenClauses)
                {
                    ExtractColumnReferencesFromBooleanExpression(whenClause.WhenExpression, columnRefs);
                    ExtractColumnReferences(whenClause.ThenExpression, columnRefs);
                }
                
                if (caseExpr.ElseExpression != null)
                {
                    ExtractColumnReferences(caseExpr.ElseExpression, columnRefs);
                }
                return;
            }
            
            // Simple CASE
            if (expr is SimpleCaseExpression simpleCaseExpr)
            {
                ExtractColumnReferences(simpleCaseExpr.InputExpression, columnRefs);
                
                foreach (var whenClause in simpleCaseExpr.WhenClauses)
                {
                    ExtractColumnReferences(whenClause.WhenExpression, columnRefs);
                    ExtractColumnReferences(whenClause.ThenExpression, columnRefs);
                }
                
                if (simpleCaseExpr.ElseExpression != null)
                {
                    ExtractColumnReferences(simpleCaseExpr.ElseExpression, columnRefs);
                }
                return;
            }
            
            // COALESCE
            if (expr is CoalesceExpression coalesceExpr)
            {
                foreach (var e in coalesceExpr.Expressions)
                {
                    ExtractColumnReferences(e, columnRefs);
                }
                return;
            }
            
            // NULLIF
            if (expr is NullIfExpression nullIfExpr)
            {
                ExtractColumnReferences(nullIfExpr.FirstExpression, columnRefs);
                ExtractColumnReferences(nullIfExpr.SecondExpression, columnRefs);
                return;
            }
            
            // CAST/CONVERT
            if (expr is CastCall castExpr)
            {
                ExtractColumnReferences(castExpr.Parameter, columnRefs);
                return;
            }
            
            if (expr is ConvertCall convertExpr)
            {
                ExtractColumnReferences(convertExpr.Parameter, columnRefs);
                return;
            }
        }
        
        /// <summary>
        /// Extract column references from boolean expressions
        /// </summary>
        private void ExtractColumnReferencesFromBooleanExpression(BooleanExpression expr, List<ColumnReferenceExpression> columnRefs)
        {
            if (expr == null) return;
            
            // Comparison (a = b)
            if (expr is BooleanComparisonExpression comparisonExpr)
            {
                ExtractColumnReferences(comparisonExpr.FirstExpression, columnRefs);
                ExtractColumnReferences(comparisonExpr.SecondExpression, columnRefs);
                return;
            }
            
            // Binary boolean (AND, OR)
            if (expr is BooleanBinaryExpression binaryExpr)
            {
                ExtractColumnReferencesFromBooleanExpression(binaryExpr.FirstExpression, columnRefs);
                ExtractColumnReferencesFromBooleanExpression(binaryExpr.SecondExpression, columnRefs);
                return;
            }
            
            // Parentheses
            if (expr is BooleanParenthesisExpression parenExpr)
            {
                ExtractColumnReferencesFromBooleanExpression(parenExpr.Expression, columnRefs);
                return;
            }
            
            // NOT
            if (expr is BooleanNotExpression notExpr)
            {
                ExtractColumnReferencesFromBooleanExpression(notExpr.Expression, columnRefs);
                return;
            }
            
            // IS NULL
            if (expr is BooleanIsNullExpression isNullExpr)
            {
                ExtractColumnReferences(isNullExpr.Expression, columnRefs);
                return;
            }
            
            // IN
            if (expr is InPredicate inExpr)
            {
                ExtractColumnReferences(inExpr.Expression, columnRefs);
                
                if (inExpr.Values != null)
                {
                    foreach (var value in inExpr.Values)
                    {
                        if (value is ScalarExpression valueExpr)
                        {
                            ExtractColumnReferences(valueExpr, columnRefs);
                        }
                    }
                }
                return;
            }
            
            // LIKE
            if (expr is LikePredicate likeExpr)
            {
                ExtractColumnReferences(likeExpr.FirstExpression, columnRefs);
                ExtractColumnReferences(likeExpr.SecondExpression, columnRefs);
                
                if (likeExpr.EscapeExpression != null)
                {
                    ExtractColumnReferences(likeExpr.EscapeExpression, columnRefs);
                }
                return;
            }
        }
        
        /// <summary>
        /// Extract column references from boolean conditions
        /// </summary>
        private List<ColumnReferenceExpression> ExtractBooleanConditionColumns(BooleanExpression expr)
        {
            var result = new List<ColumnReferenceExpression>();
            ExtractColumnReferencesFromBooleanExpression(expr, result);
            return result;
        }
        
        /// <summary>
        /// Determines if a function might be an aggregate function
        /// </summary>
        private bool IsPotentialAggregateFunction(string functionName)
        {
            if (string.IsNullOrEmpty(functionName))
                return false;
                
            // List of common aggregate functions
            string[] aggregateFunctions = {
                "SUM", "AVG", "MIN", "MAX", "COUNT", "STDEV", "STDEVP", 
                "VAR", "VARP", "CHECKSUM_AGG", "STRING_AGG"
            };
            
            return aggregateFunctions.Contains(functionName.ToUpperInvariant());
        }
    }
}
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Expressions
{
    public class WindowFunctionHandler : AbstractQueryHandler
    {
        public WindowFunctionHandler(
            VisitorContext context, 
            StringPool stringPool, 
            IdGenerator idGenerator, 
            ILogger logger = null) 
            : base(context, stringPool, idGenerator, logger)
        {
        }
        
        public override bool CanHandle(TSqlFragment fragment)
        {
            return fragment is WindowFunction;
        }
        
        public override bool Handle(TSqlFragment fragment, VisitorContext context)
        {
            if (fragment is WindowFunction windowFunc)
            {
                ExpressionNode expressionNode = null;
                
                // Try to get current expression node from context
                if (context.State.TryGetValue("CurrentExpression", out var currentExpr) && 
                    currentExpr is ExpressionNode exprNode)
                {
                    expressionNode = exprNode;
                }
                
                return Process(windowFunc, expressionNode);
            }
            
            return false;
        }

        public bool Process(WindowFunction windowFunc, ExpressionNode expressionNode)
        {
            LogDebug($"Processing WindowFunction: {GetSqlText(windowFunc)}");

            // Create an expression node if one wasn't provided
            if (expressionNode == null)
            {
                expressionNode = new ExpressionNode
                {
                    Id = CreateNodeId("EXPR", $"WINDOW_{System.Guid.NewGuid().ToString().Substring(0, 8)}"),
                    Name = "Window_Function",
                    ObjectName = GetSqlText(windowFunc),
                    Type = "WindowFunction",
                    ExpressionType = "WindowFunction",
                    Expression = GetSqlText(windowFunc)
                };
                
                Graph.AddNode(expressionNode);
            }

            // 1. Process Partition By Clause
            if (windowFunc.PartitionByClause != null)
            {
                foreach (var expression in windowFunc.PartitionByClause.PartitionExpressions)
                {
                    Context.Visit(expression);
                }
            }

            // 2. Process Order By Clause
            if (windowFunc.OrderByClause != null)
            {
                foreach (var orderByElement in windowFunc.OrderByClause.OrderByElements)
                {
                    Context.Visit(orderByElement.Expression);
                }
            }

            // 3. Process Windowing Clause (ROWS or RANGE)
            if (windowFunc.WindowClause != null)
            {
                if (windowFunc.WindowClause.WindowFrameType == WindowFrameType.Rows || windowFunc.WindowClause.WindowFrameType == WindowFrameType.Range)
                {
                    // Process start boundary
                    if (windowFunc.WindowClause.StartBoundary != null && windowFunc.WindowClause.StartBoundary.Expression != null)
                    {
                        Context.Visit(windowFunc.WindowClause.StartBoundary.Expression);
                    }

                    // Process end boundary
                    if (windowFunc.WindowClause.EndBoundary != null && windowFunc.WindowClause.EndBoundary.Expression != null)
                    {
                        Context.Visit(windowFunc.WindowClause.EndBoundary.Expression);
                    }
                }
            }
            
            // 4. Visit Function Arguments
            foreach (var arg in windowFunc.Parameters) 
            {
                Context.Visit(arg);
            }

            return true;
        }
    }
}
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Specialized;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Expressions
{
    public class WindowFunctionHandler : AbstractQueryHandler
    {
        public WindowFunctionHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger? logger) : base(visitor, graph, context, logger)
        {
        }

        public bool Process(WindowFunction windowFunc, ExpressionNode expressionNode)
        {
            LogDebug($"Processing WindowFunction: {Visitor.GetSqlText(windowFunc)}");

            // 1. Process Partition By Clause
            if (windowFunc.PartitionByClause != null)
            {
                foreach (var expression in windowFunc.PartitionByClause.PartitionExpressions)
                {
                    Visitor.Visit(expression);
                }
            }

            // 2. Process Order By Clause
            if (windowFunc.OrderByClause != null)
            {
                foreach (var orderByElement in windowFunc.OrderByClause.OrderByElements)
                {
                    Visitor.Visit(orderByElement.Expression);
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
                        Visitor.Visit(windowFunc.WindowClause.StartBoundary.Expression);
                    }

                    // Process end boundary
                    if (windowFunc.WindowClause.EndBoundary != null && windowFunc.WindowClause.EndBoundary.Expression != null)
                    {
                        Visitor.Visit(windowFunc.WindowClause.EndBoundary.Expression);
                    }
                }
            }
            
            // 4. Visit Function Arguments
            foreach (var arg in windowFunc.Parameters) 
            {
                Visitor.Visit(arg);
            }

            //Create Edge from input column to expression node
            if (expressionNode != null)
            {
                //TODO: create edges
            }


            return true;
        }
    }
}

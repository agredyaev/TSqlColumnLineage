using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Specialized;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Expressions
{
    public class CaseExpressionHandler : AbstractQueryHandler, IQueryHandler
    {
        public CaseExpressionHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger? logger) : base(visitor, graph, context, logger)
        {
        }
        
        public bool Process(dynamic fragment, ExpressionNode expressionNode)
        {
            if (fragment is SearchedCaseExpression searchedCaseExpression)
            {
                return HandleSearchedCaseExpression(searchedCaseExpression, expressionNode);
            }
            if (fragment is SimpleCaseExpression simpleCaseExpression)
            {
                return HandleSimpleCaseExpression(simpleCaseExpression, expressionNode);
            }

            return false;
        }

        public bool Process(dynamic fragment)
        {
            if (fragment is CaseExpression caseExpression)
            {
                // no expression node
                return Process(caseExpression, null);
            }
            return false;
        }


        private bool HandleSearchedCaseExpression(SearchedCaseExpression node, ExpressionNode? expressionNode)
        {
            LogDebug($"ExplicitVisit SearchedCaseExpression");
            string sql = Visitor.GetSqlText(node);
            LogDebug($"CASE Expression: {sql.Substring(0, Math.Min(100, sql.Length))}");
            
            if(expressionNode == null)
            {
                // Create an expression node for this CASE expression
                var expressionId = Visitor.CreateNodeId("EXPR", $"CASE_{System.Guid.NewGuid().ToString().Substring(0, 8)}");
                expressionNode = new ExpressionNode
                {
                    Id = expressionId,
                    Name = "CASE_Expression",
                    ObjectName = Visitor.GetSqlText(node),
                    Type = "CaseExpression",
                    Expression = Visitor.GetSqlText(node)
                };
                
                Graph.AddNode(expressionNode);
                LogDebug($"Created CASE expression node: {expressionNode.ObjectName}");
            }


            // Process the WHEN clauses
            foreach (var whenClause in node.WhenClauses)
            {
                //Condition
                Visitor.Visit(whenClause.WhenExpression);
                //Then Expression
                Visitor.Visit(whenClause.ThenExpression);
            }
            //ELSE Expression
            if(node.ElseExpression != null)
                Visitor.Visit(node.ElseExpression);


            return true;
        }

        private bool HandleSimpleCaseExpression(SimpleCaseExpression node, ExpressionNode? expressionNode)
        {
            LogDebug($"ExplicitVisit SimpleCaseExpression");
            string sql = Visitor.GetSqlText(node);
            LogDebug($"Simple CASE Expression: {sql.Substring(0, Math.Min(100, sql.Length))}");

            if (expressionNode == null)
            {
                // Create an expression node for this CASE expression
                var expressionId = Visitor.CreateNodeId("EXPR", $"SIMPLE_CASE_{System.Guid.NewGuid().ToString().Substring(0, 8)}");
                expressionNode = new ExpressionNode
                {
                    Id = expressionId,
                    Name = "CASE_Expression",
                    ObjectName = Visitor.GetSqlText(node),
                    Type = "CaseExpression",
                    Expression = Visitor.GetSqlText(node)
                };

                Graph.AddNode(expressionNode);
                LogDebug($"Created Simple CASE expression node: {expressionNode.ObjectName}");
            }

            // Process the input expression
            Visitor.Visit(node.InputExpression);
            
            // Process WHEN clauses
            foreach (var whenClause in node.WhenClauses)
            {
                //When Expression
                Visitor.Visit(whenClause.WhenExpression);
                //Then Expression
                Visitor.Visit(whenClause.ThenExpression);
            }

            //ELSE Expression
            if (node.ElseExpression != null)
                Visitor.Visit(node.ElseExpression);

            return true;
        }
    }
}

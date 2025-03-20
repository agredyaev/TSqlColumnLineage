using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Expressions
{
    public class CaseExpressionHandler : AbstractQueryHandler, IQueryHandler
    {
        public CaseExpressionHandler(
            VisitorContext context, 
            StringPool stringPool, 
            IdGenerator idGenerator, 
            ILogger logger = null) 
            : base(context, stringPool, idGenerator, logger)
        {
        }
        
        public override bool CanHandle(TSqlFragment fragment)
        {
            return fragment is SearchedCaseExpression || fragment is SimpleCaseExpression;
        }
        
        public override bool Handle(TSqlFragment fragment, VisitorContext context)
        {
            if (fragment is SearchedCaseExpression searchedCaseExpression)
            {
                return HandleSearchedCaseExpression(searchedCaseExpression, null);
            }
            if (fragment is SimpleCaseExpression simpleCaseExpression)
            {
                return HandleSimpleCaseExpression(simpleCaseExpression, null);
            }

            return false;
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

        private bool HandleSearchedCaseExpression(SearchedCaseExpression node, ExpressionNode expressionNode)
        {
            LogDebug($"Processing SearchedCaseExpression");
            string sql = GetSqlText(node);
            LogDebug($"CASE Expression: {sql.Substring(0, Math.Min(100, sql.Length))}");
            
            if(expressionNode == null)
            {
                // Create an expression node for this CASE expression
                var expressionId = CreateNodeId("EXPR", $"CASE_{System.Guid.NewGuid().ToString().Substring(0, 8)}");
                expressionNode = new ExpressionNode
                {
                    Id = expressionId,
                    Name = "CASE_Expression",
                    ObjectName = GetSqlText(node),
                    Type = "CaseExpression",
                    Expression = GetSqlText(node)
                };
                
                Graph.AddNode(expressionNode);
                LogDebug($"Created CASE expression node: {expressionNode.ObjectName}");
            }

            // Process the WHEN clauses
            foreach (var whenClause in node.WhenClauses)
            {
                //Condition - use Visit method from context
                Context.Visit(whenClause.WhenExpression);
                //Then Expression
                Context.Visit(whenClause.ThenExpression);
            }
            //ELSE Expression
            if(node.ElseExpression != null)
                Context.Visit(node.ElseExpression);

            return true;
        }

        private bool HandleSimpleCaseExpression(SimpleCaseExpression node, ExpressionNode expressionNode)
        {
            LogDebug($"Processing SimpleCaseExpression");
            string sql = GetSqlText(node);
            LogDebug($"Simple CASE Expression: {sql.Substring(0, Math.Min(100, sql.Length))}");

            if (expressionNode == null)
            {
                // Create an expression node for this CASE expression
                var expressionId = CreateNodeId("EXPR", $"SIMPLE_CASE_{System.Guid.NewGuid().ToString().Substring(0, 8)}");
                expressionNode = new ExpressionNode
                {
                    Id = expressionId,
                    Name = "CASE_Expression",
                    ObjectName = GetSqlText(node),
                    Type = "CaseExpression",
                    Expression = GetSqlText(node)
                };

                Graph.AddNode(expressionNode);
                LogDebug($"Created Simple CASE expression node: {expressionNode.ObjectName}");
            }

            // Process the input expression
            Context.Visit(node.InputExpression);
            
            // Process WHEN clauses
            foreach (var whenClause in node.WhenClauses)
            {
                //When Expression
                Context.Visit(whenClause.WhenExpression);
                //Then Expression
                Context.Visit(whenClause.ThenExpression);
            }

            //ELSE Expression
            if (node.ElseExpression != null)
                Context.Visit(node.ElseExpression);

            return true;
        }
    }
}
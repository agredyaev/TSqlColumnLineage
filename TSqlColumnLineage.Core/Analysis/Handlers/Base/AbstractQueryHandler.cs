using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Models.Edges;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Base
{
    /// <summary>
    /// Abstract base class for SQL query handlers with optimized performance and memory usage
    /// </summary>
    public abstract class AbstractQueryHandler : IQueryHandler
    {
        /// <summary>
        /// Visitor context
        /// </summary>
        protected readonly VisitorContext Context;
        
        /// <summary>
        /// Lineage graph
        /// </summary>
        protected LineageGraph Graph => Context.LineageContext.Graph;
        
        /// <summary>
        /// Lineage context
        /// </summary>
        protected LineageContext LineageContext => Context.LineageContext;
        
        /// <summary>
        /// Logger
        /// </summary>
        protected readonly ILogger Logger;
        
        /// <summary>
        /// String pool for memory optimization
        /// </summary>
        private readonly StringPool _stringPool;
        
        /// <summary>
        /// ID generator for creating node and edge IDs
        /// </summary>
        private readonly IdGenerator _idGenerator;
        
        /// <summary>
        /// Create a new query handler
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="stringPool">String pool for memory optimization</param>
        /// <param name="idGenerator">ID generator</param>
        /// <param name="logger">Logger (optional)</param>
        protected AbstractQueryHandler(
            VisitorContext context, 
            StringPool stringPool,
            IdGenerator idGenerator,
            ILogger logger = null)
        {
            Context = context ?? throw new ArgumentNullException(nameof(context));
            _stringPool = stringPool ?? throw new ArgumentNullException(nameof(stringPool));
            _idGenerator = idGenerator ?? throw new ArgumentNullException(nameof(idGenerator));
            Logger = logger;
        }

        /// <summary>
        /// Checks if this handler can process the specified fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <returns>True if the handler can process the fragment; otherwise, false</returns>
        public abstract bool CanHandle(TSqlFragment fragment);

        /// <summary>
        /// Processes the SQL fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <param name="context">Visitor context</param>
        /// <returns>True if the fragment was fully processed; otherwise, false</returns>
        public abstract bool Handle(TSqlFragment fragment, VisitorContext context);
        
        /// <summary>
        /// Gets the SQL text of a fragment
        /// </summary>
        protected string GetSqlText(TSqlFragment fragment)
        {
            return Context.GetSqlText(fragment);
        }
        
        /// <summary>
        /// Creates a unique ID for a node
        /// </summary>
        protected string CreateNodeId(string prefix, string name)
        {
            return _idGenerator.CreateNodeId(prefix, name);
        }
        
        /// <summary>
        /// Creates a random ID
        /// </summary>
        protected string CreateRandomId()
        {
            return _idGenerator.CreateGuidId("ID");
        }
        
        /// <summary>
        /// Creates a direct edge between two nodes
        /// </summary>
        protected LineageEdge CreateDirectEdge(string sourceId, string targetId, string operation, string sqlExpression = "")
        {
            var id = _idGenerator.CreateGuidId("EDGE");
            operation = _stringPool.Intern(operation);
            
            return new LineageEdge(
                id,
                sourceId,
                targetId,
                EdgeType.Direct.ToString(),
                operation,
                sqlExpression);
        }
        
        /// <summary>
        /// Creates an indirect edge between two nodes
        /// </summary>
        protected LineageEdge CreateIndirectEdge(string sourceId, string targetId, string operation, string sqlExpression = "")
        {
            var id = _idGenerator.CreateGuidId("EDGE");
            operation = _stringPool.Intern(operation);
            
            return new LineageEdge(
                id,
                sourceId,
                targetId,
                EdgeType.Indirect.ToString(),
                operation,
                sqlExpression);
        }
        
        /// <summary>
        /// Interns a string to reduce memory usage
        /// </summary>
        protected string InternString(string str)
        {
            return _stringPool.Intern(str);
        }
        
        /// <summary>
        /// Logs a debug message
        /// </summary>
        protected void LogDebug(string message)
        {
            Logger?.LogDebug(message);
        }
        
        /// <summary>
        /// Logs an information message
        /// </summary>
        protected void LogInfo(string message)
        {
            Logger?.LogInformation(message);
        }
        
        /// <summary>
        /// Logs a warning message
        /// </summary>
        protected void LogWarning(string message)
        {
            Logger?.LogWarning(message);
        }
        
        /// <summary>
        /// Logs an error message
        /// </summary>
        protected void LogError(string message, Exception ex = null)
        {
            Logger?.LogError(ex, message);
        }
        
        /// <summary>
        /// Extracts column references from an expression
        /// </summary>
        protected void ExtractColumnReferences(ScalarExpression expr, List<ColumnReferenceExpression> columnRefs)
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
            }
            
            // CONVERT
            if (expr is ConvertCall convertExpr)
            {
                ExtractColumnReferences(convertExpr.Parameter, columnRefs);
            }
            
            // CAST
            if (expr is CastCall castExpr)
            {
                ExtractColumnReferences(castExpr.Parameter, columnRefs);
            }
        }
        
        /// <summary>
        /// Extracts column references from a boolean expression
        /// </summary>
        protected void ExtractColumnReferencesFromBooleanExpression(BooleanExpression expr, List<ColumnReferenceExpression> columnRefs)
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
            
            // EXISTS
            if (expr is ExistsPredicate existsExpr && existsExpr.Subquery != null)
            {
                // We handle this differently - the visitor will visit the subquery
                return;
            }
        }
    }
}
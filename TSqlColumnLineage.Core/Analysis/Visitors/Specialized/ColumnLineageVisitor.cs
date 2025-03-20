using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Models.Edges;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Visitors.Specialized
{
    /// <summary>
    /// Specialized visitor for tracking column lineage in SQL queries
    /// </summary>
    public class ColumnLineageVisitor : BaseVisitor
    {
        private readonly IHandlerRegistry _handlerRegistry;
        
        // Track current query context
        private string _currentSelectAlias;
        private readonly Stack<TableNode> _tableContextStack = new();
        
        /// <summary>
        /// Creates a new column lineage visitor
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="handlerRegistry">Registry of specialized handlers</param>
        /// <param name="logger">Logger (optional)</param>
        public ColumnLineageVisitor(
            VisitorContext context, 
            IHandlerRegistry handlerRegistry,
            ILogger logger = null) 
            : base(context, logger)
        {
            _handlerRegistry = handlerRegistry ?? throw new ArgumentNullException(nameof(handlerRegistry));
        }
        
        /// <summary>
        /// Visits a fragment, attempting to use a specialized handler first
        /// </summary>
        public override void Visit(TSqlFragment fragment)
        {
            if (fragment == null) return;
            
            // Try to find a specialized handler
            var handler = _handlerRegistry.GetHandler(fragment);
            if (handler != null && handler.CanHandle(fragment))
            {
                LogDebug($"Using specialized handler {handler.GetType().Name} for {fragment.GetType().Name}");
                
                // Use the handler
                if (handler.Handle(fragment, Context))
                {
                    // Handler processed it completely, skip standard processing
                    return;
                }
                
                // Handler didn't fully process it, continue with standard processing
            }
            
            // Fall back to standard processing
            base.Visit(fragment);
        }
        
        #region Select Statement Processing
        
        /// <summary>
        /// Process SELECT statement
        /// </summary>
        public override void ExplicitVisit(SelectStatement node)
        {
            LogDebug($"Processing SELECT statement at line {node.StartLine}");
            
            // Save current context
            var previousAlias = _currentSelectAlias;
            
            try
            {
                // Generate a unique alias for this SELECT
                _currentSelectAlias = $"Select_{CreateRandomId().Substring(0, 8)}";
                Context.State["CurrentSelect"] = _currentSelectAlias;
                
                // Handle TOP clause
                if (node.QueryExpression is QuerySpecification qs && qs.TopRowFilter != null)
                {
                    Visit(qs.TopRowFilter);
                }
                
                // Process the INTO clause first if present
                if (node.Into != null)
                {
                    Context.State["HasIntoClause"] = true;
                    Visit(node.Into);
                }
                
                // Process the query expression
                if (node.QueryExpression != null)
                {
                    Visit(node.QueryExpression);
                }
                
                // Process ORDER BY
                if (node.OrderByClause != null)
                {
                    Visit(node.OrderByClause);
                }
                
                // Process OFFSET
                if (node.OffsetClause != null)
                {
                    Visit(node.OffsetClause);
                }
                
                // Process FOR clause
                if (node.ForClause != null)
                {
                    Visit(node.ForClause);
                }
            }
            finally
            {
                // Restore previous context
                _currentSelectAlias = previousAlias;
                Context.State.Remove("CurrentSelect");
                Context.State.Remove("HasIntoClause");
            }
        }
        
        /// <summary>
        /// Process query specification (SELECT clause)
        /// </summary>
        public override void ExplicitVisit(QuerySpecification node)
        {
            LogDebug($"Processing query specification at line {node.StartLine}");
            
            // Create result table node for this query if not part of INSERT...SELECT
            TableNode resultTable = null;
            if (!Context.State.ContainsKey("InsertTargetTable"))
            {
                resultTable = new TableNode
                {
                    Id = CreateNodeId("TABLE", _currentSelectAlias),
                    Name = _currentSelectAlias,
                    ObjectName = _currentSelectAlias,
                    TableType = "DerivedTable"
                };
                
                Graph.AddNode(resultTable);
                LineageContext.AddTable(resultTable);
                
                // Push this table as the current context
                _tableContextStack.Push(resultTable);
            }
            
            try
            {
                // First process the FROM clause to establish table context
                if (node.FromClause != null)
                {
                    Visit(node.FromClause);
                }
                
                // Then process WHERE, GROUP BY, HAVING
                if (node.WhereClause != null)
                {
                    Visit(node.WhereClause);
                }
                
                if (node.GroupByClause != null)
                {
                    Visit(node.GroupByClause);
                }
                
                if (node.HavingClause != null)
                {
                    Visit(node.HavingClause);
                }
                
                // Process SELECT elements last, now that we have the full context
                if (node.SelectElements != null)
                {
                    foreach (var element in node.SelectElements)
                    {
                        Visit(element);
                    }
                }
            }
            finally
            {
                // Pop table context if we pushed one
                if (resultTable != null && _tableContextStack.Count > 0)
                {
                    _tableContextStack.Pop();
                }
            }
        }
        
        /// <summary>
        /// Process a SELECT column
        /// </summary>
        public override void ExplicitVisit(SelectScalarExpression node)
        {
            LogDebug($"Processing select scalar expression at line {node.StartLine}");
            
            string columnName = node.ColumnName?.Value;
            string outputColumn = columnName;
            
            // If no column name specified, try to determine it from the expression
            if (string.IsNullOrEmpty(columnName) && node.Expression is ColumnReferenceExpression colRef)
            {
                outputColumn = colRef.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value;
            }
            else if (string.IsNullOrEmpty(columnName))
            {
                // Generate a name for expressions without a column name
                outputColumn = $"Col_{CreateRandomId().Substring(0, 8)}";
            }
            
            // Create target column node in result table or insert target
            ColumnNode targetColumn;
            string targetTable;
            
            if (Context.State.TryGetValue("InsertTargetTable", out var insertTable) && insertTable is string tableName)
            {
                // This is part of INSERT...SELECT
                targetTable = tableName;
                
                // Try to match with the target column list
                if (Context.State.TryGetValue("InsertTargetColumns", out var targetCols) && 
                    targetCols is List<string> targetColumns)
                {
                    // Find the corresponding target column based on position
                    int index = ((List<SelectElement>)node.Parent)?.IndexOf(node) ?? 0;
                    
                    if (index < targetColumns.Count)
                    {
                        // Use the specified target column
                        outputColumn = targetColumns[index];
                    }
                }
                
                targetColumn = Context.GetOrCreateColumnNode(targetTable, outputColumn);
            }
            else if (_tableContextStack.Count > 0)
            {
                // Regular SELECT
                targetTable = _tableContextStack.Peek().Name;
                targetColumn = Context.GetOrCreateColumnNode(targetTable, outputColumn);
            }
            else
            {
                // Fallback - create an "unknown" table
                targetTable = "Unknown";
                targetColumn = Context.GetOrCreateColumnNode(targetTable, outputColumn);
            }
            
            // Set current column context for lineage tracking
            Context.State["CurrentColumn"] = targetColumn;
            
            try
            {
                // Process the expression
                if (node.Expression is ColumnReferenceExpression colRefExpr)
                {
                    // Direct column reference - create a direct edge
                    ProcessColumnReference(colRefExpr, targetColumn);
                }
                else if (node.Expression != null)
                {
                    // Complex expression - process it
                    ProcessExpression(node.Expression, targetColumn);
                }
            }
            finally
            {
                // Clear the column context
                Context.State.Remove("CurrentColumn");
            }
        }
        
        /// <summary>
        /// Processes a direct column reference
        /// </summary>
        private void ProcessColumnReference(ColumnReferenceExpression colRef, ColumnNode targetColumn)
        {
            if (colRef == null || targetColumn == null) return;
            
            // Extract column name and table
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
            
            // Get or create the source column node
            ColumnNode sourceColumn;
            
            if (!string.IsNullOrEmpty(tableName))
            {
                // Specific table referenced
                sourceColumn = Context.GetOrCreateColumnNode(tableName, columnName);
            }
            else
            {
                // Try to infer the table from context
                sourceColumn = FindColumnInContext(columnName);
            }
            
            if (sourceColumn != null)
            {
                // Create a direct lineage edge
                var edge = CreateDirectEdge(
                    sourceColumn.Id, 
                    targetColumn.Id, 
                    "select", 
                    $"{sourceColumn.TableOwner}.{sourceColumn.Name} -> {targetColumn.TableOwner}.{targetColumn.Name}"
                );
                
                Graph.AddEdge(edge);
                LogDebug($"Created direct lineage edge: {sourceColumn.TableOwner}.{sourceColumn.Name} -> {targetColumn.TableOwner}.{targetColumn.Name}");
            }
            else
            {
                LogWarning($"Could not resolve source column for {columnName}");
            }
        }
        
        /// <summary>
        /// Finds a column in the current context
        /// </summary>
        private ColumnNode FindColumnInContext(string columnName)
        {
            if (string.IsNullOrEmpty(columnName)) return null;
            
            // Try each table in the context
            foreach (var table in LineageContext.Tables.Values)
            {
                var column = Graph.GetColumnNode(table.Name, columnName);
                if (column != null)
                    return column;
            }
            
            return null;
        }
        
        /// <summary>
        /// Processes a complex expression and tracks lineage
        /// </summary>
        private void ProcessExpression(ScalarExpression expr, ColumnNode targetColumn)
        {
            if (expr == null || targetColumn == null) return;
            
            // Create an expression node
            var expressionNode = new ExpressionNode
            {
                Id = CreateNodeId("EXPR", targetColumn.Name),
                Name = targetColumn.Name,
                ObjectName = GetSqlText(expr),
                Expression = GetSqlText(expr),
                ExpressionType = DetermineExpressionType(expr),
                ResultType = targetColumn.DataType,
                TableOwner = targetColumn.TableOwner
            };
            
            Graph.AddNode(expressionNode);
            
            // Create edge from expression to target column
            var targetEdge = CreateDirectEdge(
                expressionNode.Id,
                targetColumn.Id,
                "select",
                $"Expression -> {targetColumn.TableOwner}.{targetColumn.Name}"
            );
            
            Graph.AddEdge(targetEdge);
            
            // Extract column references from the expression
            var columnRefs = new List<ColumnReferenceExpression>();
            ExtractColumnReferences(expr, columnRefs);
            
            // Process each column reference
            foreach (var colRef in columnRefs)
            {
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
                
                // Get or create the source column node
                ColumnNode sourceColumn;
                
                if (!string.IsNullOrEmpty(tableName))
                {
                    // Specific table referenced
                    sourceColumn = Context.GetOrCreateColumnNode(tableName, columnName);
                }
                else
                {
                    // Try to infer the table from context
                    sourceColumn = FindColumnInContext(columnName);
                }
                
                if (sourceColumn != null)
                {
                    // Create an indirect lineage edge from source column to expression
                    var edge = CreateIndirectEdge(
                        sourceColumn.Id,
                        expressionNode.Id,
                        "reference",
                        $"{sourceColumn.TableOwner}.{sourceColumn.Name} used in {expressionNode.ExpressionType}"
                    );
                    
                    Graph.AddEdge(edge);
                    LogDebug($"Created indirect lineage edge: {sourceColumn.TableOwner}.{sourceColumn.Name} -> {expressionNode.Name} (Expression)");
                }
            }
        }
        
        /// <summary>
        /// Extracts all column references from an expression
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
            
            // Other expressions might have their own ways to reference columns
        }
        
        /// <summary>
        /// Extracts column references from boolean expressions (WHERE, JOIN conditions, etc.)
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
                
                if (inExpr.SubQuery != null)
                {
                    Visit(inExpr.SubQuery);
                }
                
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
        /// Determines the type of expression
        /// </summary>
        private string DetermineExpressionType(ScalarExpression expr)
        {
            if (expr is FunctionCall) return "Function";
            if (expr is SearchedCaseExpression || expr is SimpleCaseExpression) return "Case";
            if (expr is BinaryExpression) return "Calculation";
            if (expr is UnaryExpression) return "Unary";
            if (expr is ParenthesisExpression) return "Grouped";
            if (expr is CoalesceExpression) return "Coalesce";
            if (expr is NullIfExpression) return "NullIf";
            if (expr is CastCall) return "Cast";
            
            return "Expression";
        }
        
        #endregion
        
        #region FROM Clause Processing
        
        /// <summary>
        /// Process FROM clause
        /// </summary>
        public override void ExplicitVisit(FromClause node)
        {
            LogDebug($"Processing FROM clause at line {node.StartLine}");
            
            if (node.TableReferences != null)
            {
                foreach (var tableRef in node.TableReferences)
                {
                    Visit(tableRef);
                }
            }
        }
        
        /// <summary>
        /// Process named table reference
        /// </summary>
        public override void ExplicitVisit(NamedTableReference node)
        {
            if (node?.SchemaObject?.Identifiers == null) return;
            
            string tableName = string.Join(".", node.SchemaObject.Identifiers.Select(i => i.Value));
            string alias = node.Alias?.Value;
            
            LogDebug($"Processing table reference: {tableName}{(alias != null ? $" AS {alias}" : "")}");
            
            // Create table node
            string tableType = "Table";
            if (tableName.StartsWith("#")) tableType = "TempTable";
            
            var tableNode = Context.GetOrCreateTableNode(tableName, tableType);
            
            // Add alias if specified
            if (!string.IsNullOrEmpty(alias))
            {
                tableNode.Alias = alias;
                LineageContext.AddTableAlias(alias, tableName);
            }
        }
        
        /// <summary>
        /// Process JOIN
        /// </summary>
        public override void ExplicitVisit(QualifiedJoin node)
        {
            LogDebug($"Processing JOIN at line {node.StartLine}");
            
            // Process left side
            if (node.FirstTableReference != null)
            {
                Visit(node.FirstTableReference);
            }
            
            // Process right side
            if (node.SecondTableReference != null)
            {
                Visit(node.SecondTableReference);
            }
            
            // Process join condition
            if (node.SearchCondition != null)
            {
                Context.State["InJoinCondition"] = true;
                
                try
                {
                    Visit(node.SearchCondition);
                    
                    // Extract column references in join condition
                    var columnRefs = new List<ColumnReferenceExpression>();
                    ExtractJoinColumnReferences(node.SearchCondition, columnRefs);
                    
                    // Create join edges between columns
                    if (columnRefs.Count >= 2)
                    {
                        ProcessJoinColumns(columnRefs, node.QualifiedJoinType.ToString());
                    }
                }
                finally
                {
                    Context.State.Remove("InJoinCondition");
                }
            }
        }
        
        /// <summary>
        /// Extracts column references from join conditions
        /// </summary>
        private void ExtractJoinColumnReferences(BooleanExpression expr, List<ColumnReferenceExpression> columnRefs)
        {
            // Similar to ExtractColumnReferencesFromBooleanExpression but specialized for JOIN
            // focusing on equality conditions
            
            if (expr is BooleanComparisonExpression comparisonExpr && 
                comparisonExpr.ComparisonType == BooleanComparisonType.Equals)
            {
                if (comparisonExpr.FirstExpression is ColumnReferenceExpression leftCol &&
                    comparisonExpr.SecondExpression is ColumnReferenceExpression rightCol)
                {
                    columnRefs.Add(leftCol);
                    columnRefs.Add(rightCol);
                }
            }
            else if (expr is BooleanBinaryExpression binaryExpr &&
                     binaryExpr.BinaryExpressionType == BooleanBinaryExpressionType.And)
            {
                ExtractJoinColumnReferences(binaryExpr.FirstExpression, columnRefs);
                ExtractJoinColumnReferences(binaryExpr.SecondExpression, columnRefs);
            }
            else if (expr is BooleanParenthesisExpression parenExpr)
            {
                ExtractJoinColumnReferences(parenExpr.Expression, columnRefs);
            }
        }
        
        /// <summary>
        /// Processes join columns and creates lineage edges
        /// </summary>
        private void ProcessJoinColumns(List<ColumnReferenceExpression> columnRefs, string joinType)
        {
            // Group columns by table
            var columnsByTable = new Dictionary<string, List<ColumnReferenceExpression>>();
            
            foreach (var colRef in columnRefs)
            {
                string tableName = colRef.MultiPartIdentifier?.Identifiers.Count > 1 
                    ? colRef.MultiPartIdentifier.Identifiers[0].Value 
                    : null;
                    
                if (tableName == null) continue;
                
                // Resolve alias if needed
                if (LineageContext.TableAliases.TryGetValue(tableName, out var resolvedTable))
                {
                    tableName = resolvedTable;
                }
                
                if (!columnsByTable.TryGetValue(tableName, out var tableColumns))
                {
                    tableColumns = new List<ColumnReferenceExpression>();
                    columnsByTable[tableName] = tableColumns;
                }
                
                tableColumns.Add(colRef);
            }
            
            // If we have columns from at least 2 tables
            if (columnsByTable.Count >= 2)
            {
                // Create join edges between tables
                var tables = columnsByTable.Keys.ToList();
                
                for (int i = 0; i < tables.Count - 1; i++)
                {
                    for (int j = i + 1; j < tables.Count; j++)
                    {
                        var leftTableName = tables[i];
                        var rightTableName = tables[j];
                        
                        var leftColumns = columnsByTable[leftTableName];
                        var rightColumns = columnsByTable[rightTableName];
                        
                        // Find matching column pairs
                        for (int li = 0; li < leftColumns.Count; li++)
                        {
                            for (int ri = 0; ri < rightColumns.Count; ri++)
                            {
                                CreateJoinEdge(leftColumns[li], rightColumns[ri], joinType);
                            }
                        }
                    }
                }
            }
        }
        
        /// <summary>
        /// Creates a join edge between two columns
        /// </summary>
        private void CreateJoinEdge(ColumnReferenceExpression leftCol, ColumnReferenceExpression rightCol, string joinType)
        {
            string leftColumnName = leftCol.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value;
            string leftTableName = leftCol.MultiPartIdentifier?.Identifiers.Count > 1 
                ? leftCol.MultiPartIdentifier.Identifiers[0].Value 
                : null;
                
            string rightColumnName = rightCol.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value;
            string rightTableName = rightCol.MultiPartIdentifier?.Identifiers.Count > 1 
                ? rightCol.MultiPartIdentifier.Identifiers[0].Value 
                : null;
                
            if (leftTableName == null || rightTableName == null) return;
            
            // Resolve aliases if needed
            if (LineageContext.TableAliases.TryGetValue(leftTableName, out var resolvedLeftTable))
            {
                leftTableName = resolvedLeftTable;
            }
            
            if (LineageContext.TableAliases.TryGetValue(rightTableName, out var resolvedRightTable))
            {
                rightTableName = resolvedRightTable;
            }
            
            // Get or create column nodes
            var leftColumnNode = Context.GetOrCreateColumnNode(leftTableName, leftColumnName);
            var rightColumnNode = Context.GetOrCreateColumnNode(rightTableName, rightColumnName);
            
            if (leftColumnNode == null || rightColumnNode == null) return;
            
            // Create join edges in both directions
            var leftToRightEdge = new LineageEdge
            {
                Id = CreateRandomId(),
                SourceId = leftColumnNode.Id,
                TargetId = rightColumnNode.Id,
                Type = EdgeType.Join.ToString(),
                Operation = joinType.ToLowerInvariant(),
                SqlExpression = $"{leftTableName}.{leftColumnName} = {rightTableName}.{rightColumnName}"
            };
            
            var rightToLeftEdge = new LineageEdge
            {
                Id = CreateRandomId(),
                SourceId = rightColumnNode.Id,
                TargetId = leftColumnNode.Id,
                Type = EdgeType.Join.ToString(),
                Operation = joinType.ToLowerInvariant(),
                SqlExpression = $"{rightTableName}.{rightColumnName} = {leftTableName}.{leftColumnName}"
            };
            
            Graph.AddEdge(leftToRightEdge);
            Graph.AddEdge(rightToLeftEdge);
            
            LogDebug($"Created JOIN edges between {leftTableName}.{leftColumnName} and {rightTableName}.{rightColumnName}");
        }
        
        #endregion
    }
}
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Models.Edges;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Visitors.Specialized
{
    /// <summary>
    /// Specialized visitor for tracking column lineage in SQL queries with optimized performance
    /// </summary>
    public sealed class ColumnLineageVisitor : BaseVisitor
    {
        private readonly IHandlerRegistry _handlerRegistry;
        
        // Track current query context
        private string _currentSelectAlias;
        
        // Object pools for reducing allocations
        private readonly ObjectPool<List<ColumnReferenceExpression>> _columnListPool;
        private readonly ObjectPool<List<LineageEdge>> _edgeListPool;
        
        /// <summary>
        /// Creates a new column lineage visitor
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="stringPool">String pool for memory optimization</param>
        /// <param name="idGenerator">ID generator</param>
        /// <param name="handlerRegistry">Registry of specialized handlers</param>
        /// <param name="logger">Logger (optional)</param>
        /// <param name="cancellationToken">Cancellation token for stopping processing</param>
        public ColumnLineageVisitor(
            VisitorContext context,
            StringPool stringPool,
            IdGenerator idGenerator,
            IHandlerRegistry handlerRegistry,
            ILogger logger = null,
            CancellationToken cancellationToken = default) 
            : base(context, stringPool, idGenerator, logger, cancellationToken)
        {
            _handlerRegistry = handlerRegistry ?? throw new ArgumentNullException(nameof(handlerRegistry));
            
            // Initialize object pools
            _columnListPool = new ObjectPool<List<ColumnReferenceExpression>>(
                () => new List<ColumnReferenceExpression>(),
                list => list.Clear(),
                initialCount: 10,
                maxObjects: 100);
                
            _edgeListPool = new ObjectPool<List<LineageEdge>>(
                () => new List<LineageEdge>(),
                list => list.Clear(),
                initialCount: 10,
                maxObjects: 100);
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
            
            // Generate a unique alias for this SELECT
            _currentSelectAlias = InternString($"Select_{CreateRandomId().Substring(0, 8)}");
            Context.SetState("CurrentSelect", _currentSelectAlias);
            
            try
            {
                // Handle TOP clause
                if (node.QueryExpression is QuerySpecification qs && qs.TopRowFilter != null)
                {
                    Visit(qs.TopRowFilter);
                }
                
                // Process the INTO clause first if present
                if (node.Into != null)
                {
                    Context.SetState("HasIntoClause", true);
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
                Context.SetState("CurrentSelect", null);
                Context.SetState("HasIntoClause", null);
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
                var id = CreateNodeId("TABLE", _currentSelectAlias);
                resultTable = new TableNode(
                    id,
                    _currentSelectAlias,
                    "DerivedTable",
                    objectName: _currentSelectAlias
                );
                
                Graph.AddNode(resultTable);
                LineageContext.AddTable(resultTable);
                
                // Push this table as the current context
                LineageContext.CurrentTableContext.Push(resultTable);
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
                    Context.SetState("ProcessingWhereClause", true);
                    try
                    {
                        Visit(node.WhereClause);
                    }
                    finally
                    {
                        Context.SetState("ProcessingWhereClause", null);
                    }
                }
                
                if (node.GroupByClause != null)
                {
                    Context.SetState("ProcessingGroupByClause", true);
                    try
                    {
                        Visit(node.GroupByClause);
                    }
                    finally
                    {
                        Context.SetState("ProcessingGroupByClause", null);
                    }
                }
                
                if (node.HavingClause != null)
                {
                    Context.SetState("ProcessingHavingClause", true);
                    try
                    {
                        Visit(node.HavingClause);
                    }
                    finally
                    {
                        Context.SetState("ProcessingHavingClause", null);
                    }
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
                if (resultTable != null && LineageContext.CurrentTableContext.Count > 0)
                {
                    LineageContext.CurrentTableContext.Pop();
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
                outputColumn = InternString($"Col_{CreateRandomId().Substring(0, 8)}");
            }
            
            // Create target column node in result table or insert target
            ColumnNode targetColumn;
            string targetTable;
            
            if (Context.GetState("InsertTargetTable") is string tableName)
            {
                // This is part of INSERT...SELECT
                targetTable = tableName;
                
                // Try to match with the target column list
                if (Context.GetState("InsertTargetColumns") is List<string> targetColumns)
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
            else if (LineageContext.CurrentTableContext.Count > 0)
            {
                // Regular SELECT
                targetTable = LineageContext.CurrentTableContext.Peek().Name;
                targetColumn = Context.GetOrCreateColumnNode(targetTable, outputColumn);
            }
            else
            {
                // Fallback - create an "unknown" table
                targetTable = "Unknown";
                targetColumn = Context.GetOrCreateColumnNode(targetTable, outputColumn);
            }
            
            // Set current column context for lineage tracking
            Context.SetColumnContext("current", targetColumn);
            
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
                Context.SetColumnContext("current", null);
            }
        }
        
        /// <summary>
        /// Process a SELECT * expression
        /// </summary>
        public override void ExplicitVisit(SelectStarExpression node)
        {
            LogDebug($"Processing SELECT * expression at line {node.StartLine}");
            
            // Get qualifier if specified (table name)
            string tableQualifier = null;
            if (node.Qualifier != null && node.Qualifier.Identifiers.Count > 0)
            {
                tableQualifier = node.Qualifier.Identifiers[0].Value;
                LogDebug($"SELECT * with qualifier: {tableQualifier}");
            }
            
            // Create target table node for output
            TableNode targetTable;
            if (Context.GetState("InsertTargetTable") is string insertTargetName)
            {
                // This is INSERT...SELECT
                targetTable = LineageContext.GetTable(insertTargetName);
                if (targetTable == null)
                {
                    targetTable = Context.GetOrCreateTableNode(insertTargetName);
                }
            }
            else if (LineageContext.CurrentTableContext.Count > 0)
            {
                // Regular SELECT
                targetTable = LineageContext.CurrentTableContext.Peek();
            }
            else
            {
                // Fallback
                targetTable = Context.GetOrCreateTableNode("Unknown");
            }
            
            // Find source tables based on qualifier
            var sourceTables = new List<TableNode>();
            if (tableQualifier != null)
            {
                // If alias is specified, find the specific table
                var sourceTable = LineageContext.GetTable(tableQualifier);
                if (sourceTable != null)
                {
                    sourceTables.Add(sourceTable);
                }
            }
            else
            {
                // No qualifier, include all tables in scope
                foreach (var table in LineageContext.Tables.Values)
                {
                    // Skip the target table
                    if (table.Id != targetTable.Id)
                    {
                        sourceTables.Add(table);
                    }
                }
            }
            
            LogDebug($"SELECT * found {sourceTables.Count} source tables");
            
            // Create lineage edges from source columns to target
            foreach (var sourceTable in sourceTables)
            {
                // Create direct lineage edges for each column
                var sourceColumns = Graph.GetNodesOfType<ColumnNode>()
                    .Where(c => c.TableOwner == sourceTable.Name)
                    .ToList();
                
                LogDebug($"SELECT * processing {sourceColumns.Count} columns from {sourceTable.Name}");
                
                foreach (var sourceColumn in sourceColumns)
                {
                    // Find or create a corresponding target column
                    var targetColumn = Context.GetOrCreateColumnNode(
                        targetTable.Name,
                        sourceColumn.Name,
                        sourceColumn.DataType);
                    
                    // Create a direct lineage edge
                    var edge = CreateDirectEdge(
                        sourceColumn.Id,
                        targetColumn.Id,
                        "select",
                        $"SELECT * column: {sourceColumn.TableOwner}.{sourceColumn.Name} -> {targetColumn.TableOwner}.{targetColumn.Name}");
                    
                    Graph.AddEdge(edge);
                    LogDebug($"Created SELECT * lineage edge: {sourceColumn.TableOwner}.{sourceColumn.Name} -> {targetColumn.TableOwner}.{targetColumn.Name}");
                }
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
            var expressionId = CreateNodeId("EXPR", targetColumn.Name);
            var expressionNode = new ExpressionNode(
                expressionId,
                targetColumn.Name,
                GetSqlText(expr),
                DetermineExpressionType(expr),
                targetColumn.DataType,
                targetColumn.TableOwner
            );
            
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
            var columnRefs = _columnListPool.Get();
            try
            {
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
            finally
            {
                // Return the column list to the pool
                _columnListPool.Return(columnRefs);
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
            if (expr is ConvertCall) return "Convert";
            
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
                Context.SetState("InJoinCondition", true);
                
                try
                {
                    Visit(node.SearchCondition);
                    
                    // Extract column references in join condition
                    var columnRefs = _columnListPool.Get();
                    try
                    {
                        ExtractJoinColumnReferences(node.SearchCondition, columnRefs);
                        
                        // Create join edges between columns
                        if (columnRefs.Count >= 2)
                        {
                            ProcessJoinColumns(columnRefs, node.QualifiedJoinType.ToString());
                        }
                    }
                    finally
                    {
                        _columnListPool.Return(columnRefs);
                    }
                }
                finally
                {
                    Context.SetState("InJoinCondition", null);
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
                var edges = _edgeListPool.Get();
                
                try
                {
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
                                    CreateJoinEdge(leftColumns[li], rightColumns[ri], joinType, edges);
                                }
                            }
                        }
                    }
                    
                    // Add all accumulated edges to the graph
                    foreach (var edge in edges)
                    {
                        Graph.AddEdge(edge);
                    }
                }
                finally
                {
                    _edgeListPool.Return(edges);
                }
            }
        }
        
        /// <summary>
        /// Creates a join edge between two columns
        /// </summary>
        private void CreateJoinEdge(ColumnReferenceExpression leftCol, ColumnReferenceExpression rightCol, string joinType, List<LineageEdge> edges)
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
            
            joinType = InternString(joinType.ToLowerInvariant());
            
            // Create join edges in both directions
            edges.Add(new LineageEdge(
                _idGenerator.CreateGuidId("EDGE"),
                leftColumnNode.Id,
                rightColumnNode.Id,
                EdgeType.Join.ToString(),
                joinType,
                $"{leftTableName}.{leftColumnName} = {rightTableName}.{rightColumnName}"
            ));
            
            edges.Add(new LineageEdge(
                _idGenerator.CreateGuidId("EDGE"),
                rightColumnNode.Id,
                leftColumnNode.Id,
                EdgeType.Join.ToString(),
                joinType,
                $"{rightTableName}.{rightColumnName} = {leftTableName}.{leftColumnName}"
            ));
            
            LogDebug($"Created JOIN edges between {leftTableName}.{leftColumnName} and {rightTableName}.{rightColumnName}");
        }
        
        #endregion
    }
}
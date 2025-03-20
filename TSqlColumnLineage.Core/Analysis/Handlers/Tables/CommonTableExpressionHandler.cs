using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Tables
{
    /// <summary>
    /// Handler for Common Table Expressions (CTEs)
    /// </summary>
    public class CommonTableExpressionHandler : AbstractQueryHandler
    {
        /// <summary>
        /// Creates a new CTE handler
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="logger">Logger (optional)</param>
        public CommonTableExpressionHandler(VisitorContext context, ILogger logger = null)
            : base(context, logger)
        {
        }
        
        /// <summary>
        /// Checks if this handler can process the specified fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <returns>True if the handler can process the fragment; otherwise, false</returns>
        public override bool CanHandle(TSqlFragment fragment)
        {
            return fragment is CommonTableExpression ||
                   (fragment is NamedTableReference namedTable && IsCteReference(namedTable)) ||
                   IsWithClause(fragment);
        }
        
        /// <summary>
        /// Processes the SQL fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <param name="context">Visitor context</param>
        /// <returns>True if the fragment was fully processed; otherwise, false</returns>
        public override bool Handle(TSqlFragment fragment, VisitorContext context)
        {
            if (fragment is CommonTableExpression cte)
            {
                ProcessCTE(cte);
                return true;
            }
            else if (fragment is NamedTableReference namedTable && IsCteReference(namedTable))
            {
                ProcessCTEReference(namedTable);
                return true;
            }
            else if (IsWithClause(fragment))
            {
                ProcessWithClause(fragment);
                return true;
            }
            
            return false;
        }
        
        /// <summary>
        /// Checks if a named table reference is a CTE reference
        /// </summary>
        private bool IsCteReference(NamedTableReference namedTable)
        {
            if (namedTable?.SchemaObject?.Identifiers == null || namedTable.SchemaObject.Identifiers.Count == 0)
                return false;
                
            string tableName = namedTable.SchemaObject.Identifiers[0].Value;
            return LineageContext.Metadata.ContainsKey($"CTE:{tableName.ToLowerInvariant()}");
        }
        
        /// <summary>
        /// Checks if a fragment is a WITH clause
        /// </summary>
        private bool IsWithClause(TSqlFragment fragment)
        {
            try
            {
                // Try to get CommonTableExpressions or CTEs property using reflection
                var property = fragment.GetType().GetProperty("CommonTableExpressions") ?? 
                               fragment.GetType().GetProperty("CTEs") ??
                               fragment.GetType().GetProperty("Expressions");
                               
                if (property != null)
                {
                    var value = property.GetValue(fragment);
                    return value != null && value is IEnumerable<CommonTableExpression>;
                }
            }
            catch
            {
                // Ignore reflection errors
            }
            
            return false;
        }
        
        /// <summary>
        /// Processes a Common Table Expression
        /// </summary>
        private void ProcessCTE(CommonTableExpression cte)
        {
            if (cte == null || cte.ExpressionName == null)
                return;
                
            string cteName = cte.ExpressionName.Value;
            LogDebug($"Processing CTE: {cteName}");
            
            // Create a table node for the CTE
            var cteNode = new TableNode
            {
                Id = CreateNodeId("CTE", cteName),
                Name = cteName,
                ObjectName = cteName,
                SchemaName = string.Empty,
                TableType = "CTE",
                Definition = GetSqlText(cte)
            };
            
            Graph.AddNode(cteNode);
            LineageContext.AddTable(cteNode);
            
            // Register the CTE in metadata
            LineageContext.Metadata[$"CTE:{cteName.ToLowerInvariant()}"] = cteNode;
            
            // Process CTE column list
            List<string> columnNames = new List<string>();
            
            if (cte.Columns != null && cte.Columns.Count > 0)
            {
                // Use explicit column list
                foreach (var column in cte.Columns)
                {
                    if (column?.Value != null)
                    {
                        columnNames.Add(column.Value);
                    }
                }
            }
            else
            {
                // Try to infer column names from query
                columnNames = InferColumnsFromQuery(cte.QueryExpression);
            }
            
            // Create column nodes
            foreach (var columnName in columnNames)
            {
                var columnNode = new ColumnNode
                {
                    Id = CreateNodeId("COLUMN", $"{cteName}.{columnName}"),
                    Name = columnName,
                    ObjectName = columnName,
                    TableOwner = cteName,
                    SchemaName = string.Empty,
                    DataType = "unknown" // We don't know types yet
                };
                
                Graph.AddNode(columnNode);
                cteNode.Columns.Add(columnNode.Id);
            }
            
            // Save current context state
            var processingCTE = Context.State.ContainsKey("ProcessingCTE");
            var currentCTE = Context.State.ContainsKey("CurrentCTE") ? Context.State["CurrentCTE"] : null;
            
            // Update context for processing the query
            Context.State["ProcessingCTE"] = true;
            Context.State["CurrentCTE"] = cteName;
            
            try
            {
                // Process the query expression
                var visitor = Context.GetType().Assembly.CreateInstance(
                    "TSqlColumnLineage.Core.Analysis.Visitors.Specialized.ColumnLineageVisitor",
                    false,
                    System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
                    null,
                    new[] { Context, null, Logger },
                    null,
                    null) as TSqlFragmentVisitor;
                    
                if (visitor != null && cte.QueryExpression != null)
                {
                    cte.QueryExpression.Accept(visitor);
                    
                    // Now associate columns from the query with CTE columns
                    LinkQueryColumnsToCteTables(cte.QueryExpression, cteNode, columnNames);
                }
            }
            finally
            {
                // Restore previous context
                if (!processingCTE)
                {
                    Context.State.Remove("ProcessingCTE");
                }
                
                if (currentCTE != null)
                {
                    Context.State["CurrentCTE"] = currentCTE;
                }
                else
                {
                    Context.State.Remove("CurrentCTE");
                }
            }
        }
        
        /// <summary>
        /// Processes a WITH clause containing CTEs
        /// </summary>
        private void ProcessWithClause(TSqlFragment withClause)
        {
            if (withClause == null)
                return;
                
            LogDebug("Processing WITH clause");
            
            // Extract CTEs using reflection
            var ctes = ExtractCTEsFromWithClause(withClause);
            if (ctes.Count == 0)
                return;
            
            LogDebug($"Found {ctes.Count} CTEs");
            
            // Save current context state
            var processingWithClause = Context.State.ContainsKey("ProcessingWithClause");
            
            // Update context
            Context.State["ProcessingWithClause"] = true;
            
            try
            {
                // Process each CTE in order
                foreach (var cte in ctes)
                {
                    ProcessCTE(cte);
                }
            }
            finally
            {
                // Restore context
                if (!processingWithClause)
                {
                    Context.State.Remove("ProcessingWithClause");
                }
            }
        }
        
        /// <summary>
        /// Extracts CTEs from a WITH clause using reflection
        /// </summary>
        private List<CommonTableExpression> ExtractCTEsFromWithClause(TSqlFragment withClause)
        {
            var result = new List<CommonTableExpression>();
            
            try
            {
                // Try different property names for the CTE collection
                var ctesProp = withClause.GetType().GetProperty("CommonTableExpressions") ?? 
                               withClause.GetType().GetProperty("CTEs") ??
                               withClause.GetType().GetProperty("Expressions");
                               
                if (ctesProp != null)
                {
                    var value = ctesProp.GetValue(withClause);
                    
                    if (value is IEnumerable<CommonTableExpression> ctes)
                    {
                        result.AddRange(ctes);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"Error extracting CTEs from WITH clause: {ex.Message}", ex);
            }
            
            return result;
        }
        
        /// <summary>
        /// Processes a reference to a CTE
        /// </summary>
        private void ProcessCTEReference(NamedTableReference namedTable)
        {
            if (namedTable?.SchemaObject?.Identifiers == null || namedTable.SchemaObject.Identifiers.Count == 0)
                return;
                
            string tableName = namedTable.SchemaObject.Identifiers[0].Value;
            LogDebug($"Processing CTE reference: {tableName}");
            
            // Get the CTE from metadata
            if (LineageContext.Metadata.TryGetValue($"CTE:{tableName.ToLowerInvariant()}", out var cteObj) && 
                cteObj is TableNode cteNode)
            {
                // If the reference has an alias, register it
                if (!string.IsNullOrEmpty(namedTable.Alias?.Value))
                {
                    string alias = namedTable.Alias.Value;
                    LineageContext.AddTableAlias(alias, cteNode.Name);
                    LogDebug($"Added CTE alias: {alias} -> {cteNode.Name}");
                }
            }
        }
        
        /// <summary>
        /// Infers column names from a query expression
        /// </summary>
        private List<string> InferColumnsFromQuery(QueryExpression query)
        {
            var columns = new List<string>();
            
            if (query == null)
                return columns;
                
            // Process SELECT query
            if (query is QuerySpecification select && select.SelectElements != null)
            {
                foreach (var element in select.SelectElements)
                {
                    if (element is SelectScalarExpression scalarExpr)
                    {
                        // Column with alias
                        if (scalarExpr.ColumnName != null)
                        {
                            columns.Add(scalarExpr.ColumnName.Value);
                        }
                        // Direct column reference
                        else if (scalarExpr.Expression is ColumnReferenceExpression colRef &&
                                 colRef.MultiPartIdentifier?.Identifiers?.Count > 0)
                        {
                            columns.Add(colRef.MultiPartIdentifier.Identifiers.Last().Value);
                        }
                        // Expression without alias - use a placeholder
                        else
                        {
                            columns.Add($"Col{columns.Count + 1}");
                        }
                    }
                    else if (element is SelectStarExpression)
                    {
                        // We can't determine columns from * without knowing table schema
                        columns.Add($"Col{columns.Count + 1}");
                    }
                }
            }
            // UNION, INTERSECT, EXCEPT - use columns from first query
            else if (query is BinaryQueryExpression binaryQuery && binaryQuery.FirstQueryExpression != null)
            {
                columns = InferColumnsFromQuery(binaryQuery.FirstQueryExpression);
            }
            
            return columns;
        }
        
        /// <summary>
        /// Links columns from a query expression to CTE columns
        /// </summary>
        private void LinkQueryColumnsToCteTables(QueryExpression query, TableNode cteNode, List<string> cteColumns)
        {
            if (query == null || cteNode == null || cteColumns == null || cteColumns.Count == 0)
                return;
                
            // Handle different query types
            if (query is QuerySpecification select && select.SelectElements != null)
            {
                // Process each SELECT element
                for (int i = 0; i < Math.Min(select.SelectElements.Count, cteColumns.Count); i++)
                {
                    var element = select.SelectElements[i];
                    var cteColumnName = cteColumns[i];
                    
                    var cteColumn = Graph.GetColumnNode(cteNode.Name, cteColumnName);
                    if (cteColumn == null)
                        continue;
                        
                    if (element is SelectScalarExpression scalarExpr)
                    {
                        // Extract source column or expression
                        if (scalarExpr.Expression is ColumnReferenceExpression colRef)
                        {
                            // Direct column reference - find the source column
                            string columnName = colRef.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value;
                            string tableName = null;
                            
                            if (colRef.MultiPartIdentifier?.Identifiers.Count > 1)
                            {
                                tableName = colRef.MultiPartIdentifier.Identifiers[0].Value;
                                
                                // Resolve alias if needed
                                if (LineageContext.TableAliases.TryGetValue(tableName, out var resolvedTable))
                                {
                                    tableName = resolvedTable;
                                }
                            }
                            
                            // Find source column
                            ColumnNode sourceColumn = null;
                            
                            if (!string.IsNullOrEmpty(tableName))
                            {
                                sourceColumn = Graph.GetColumnNode(tableName, columnName);
                            }
                            else
                            {
                                // Try to find in any table
                                foreach (var table in LineageContext.Tables.Values)
                                {
                                    var col = Graph.GetColumnNode(table.Name, columnName);
                                    if (col != null)
                                    {
                                        sourceColumn = col;
                                        break;
                                    }
                                }
                            }
                            
                            if (sourceColumn != null)
                            {
                                // Create direct edge from source to CTE column
                                var edge = CreateDirectEdge(
                                    sourceColumn.Id,
                                    cteColumn.Id,
                                    "cte",
                                    $"Source column to CTE column: {sourceColumn.TableOwner}.{sourceColumn.Name} -> {cteColumn.TableOwner}.{cteColumn.Name}"
                                );
                                
                                Graph.AddEdge(edge);
                                LogDebug($"Created lineage edge: {sourceColumn.TableOwner}.{sourceColumn.Name} -> {cteColumn.TableOwner}.{cteColumn.Name}");
                                
                                // Update data type if known
                                if (cteColumn.DataType == "unknown" && sourceColumn.DataType != "unknown")
                                {
                                    cteColumn.DataType = sourceColumn.DataType;
                                }
                            }
                        }
                        else
                        {
                            // Complex expression - create an expression node
                            var expressionNode = new ExpressionNode
                            {
                                Id = CreateNodeId("EXPR", $"{cteNode.Name}_{cteColumnName}"),
                                Name = cteColumnName,
                                ObjectName = GetSqlText(scalarExpr.Expression),
                                ExpressionType = "CteExpression",
                                Expression = GetSqlText(scalarExpr.Expression),
                                TableOwner = cteNode.Name,
                                ResultType = "unknown"
                            };
                            
                            Graph.AddNode(expressionNode);
                            
                            // Create edge from expression to CTE column
                            var edge = CreateDirectEdge(
                                expressionNode.Id,
                                cteColumn.Id,
                                "cte",
                                $"Expression to CTE column: {expressionNode.Name} -> {cteColumn.TableOwner}.{cteColumn.Name}"
                            );
                            
                            Graph.AddEdge(edge);
                            LogDebug($"Created lineage edge: Expression -> {cteColumn.TableOwner}.{cteColumn.Name}");
                            
                            // Extract column references from the expression
                            var columnRefs = new List<ColumnReferenceExpression>();
                            ExtractColumnReferences(scalarExpr.Expression, columnRefs);
                            
                            foreach (var colRef in columnRefs)
                            {
                                // Extract source column info
                                string columnName = colRef.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value;
                                string tableName = null;
                                
                                if (colRef.MultiPartIdentifier?.Identifiers.Count > 1)
                                {
                                    tableName = colRef.MultiPartIdentifier.Identifiers[0].Value;
                                    
                                    // Resolve alias if needed
                                    if (LineageContext.TableAliases.TryGetValue(tableName, out var resolvedTable))
                                    {
                                        tableName = resolvedTable;
                                    }
                                }
                                
                                // Find source column
                                ColumnNode sourceColumn = null;
                                
                                if (!string.IsNullOrEmpty(tableName))
                                {
                                    sourceColumn = Graph.GetColumnNode(tableName, columnName);
                                }
                                else
                                {
                                    // Try to find in any table
                                    foreach (var table in LineageContext.Tables.Values)
                                    {
                                        var col = Graph.GetColumnNode(table.Name, columnName);
                                        if (col != null)
                                        {
                                            sourceColumn = col;
                                            break;
                                        }
                                    }
                                }
                                
                                if (sourceColumn != null)
                                {
                                    // Create edge from source column to expression
                                    var sourceEdge = CreateIndirectEdge(
                                        sourceColumn.Id,
                                        expressionNode.Id,
                                        "reference",
                                        $"Source column to expression: {sourceColumn.TableOwner}.{sourceColumn.Name} -> {expressionNode.Name}"
                                    );
                                    
                                    Graph.AddEdge(sourceEdge);
                                    LogDebug($"Created lineage edge: {sourceColumn.TableOwner}.{sourceColumn.Name} -> Expression");
                                }
                            }
                        }
                    }
                }
            }
            // UNION, INTERSECT, EXCEPT - process both sides
            else if (query is BinaryQueryExpression binaryQuery)
            {
                // Process first query
                if (binaryQuery.FirstQueryExpression != null)
                {
                    LinkQueryColumnsToCteTables(binaryQuery.FirstQueryExpression, cteNode, cteColumns);
                }
                
                // Process second query
                if (binaryQuery.SecondQueryExpression != null)
                {
                    LinkQueryColumnsToCteTables(binaryQuery.SecondQueryExpression, cteNode, cteColumns);
                }
            }
        }
    }
}
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Visitors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace TSqlColumnLineage.Core.Handlers.QueryHandlers
{
    /// <summary>
    /// Handler for Common Table Expressions (CTEs)
    /// </summary>
    public class CommonTableExpressionHandler : AbstractQueryHandler
    {
        public CommonTableExpressionHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger logger)
            : base(visitor, graph, context, logger)
        {
        }

        /// <summary>
        /// Process a SQL fragment
        /// </summary>
        /// <param name="fragment">The SQL fragment to process</param>
        /// <returns>True if the handler processed the fragment; otherwise, false</returns>
        public override bool Process(TSqlFragment fragment)
        {
            if (fragment is CommonTableExpression cte)
            {
                ProcessCTE(cte);
                return true;
            }
            
            if (fragment is TSqlFragment withClause)
            {
                // Check if it's a WITH clause using reflection
                var property = withClause.GetType().GetProperty("CommonTableExpressions") ?? 
                               withClause.GetType().GetProperty("CTEs") ??
                               withClause.GetType().GetProperty("Expressions");
                               
                if (property != null)
                {
                    ProcessWithClause(withClause);
                    return true;
                }
            }
            
            if (fragment is NamedTableReference namedTable)
            {
                // Check if it's a CTE reference
                string tableName = string.Join(".", namedTable.SchemaObject.Identifiers.Select(i => i.Value));
                string cteKey = $"CTE:{tableName.ToLowerInvariant()}";
                
                if (Context.Metadata.ContainsKey(cteKey))
                {
                    ProcessCTEReference(namedTable);
                    return true;
                }
            }
            
            return false;
        }

        /// <summary>
        /// Process a Common Table Expression
        /// </summary>
        public void ProcessCTE(CommonTableExpression node)
        {
            if (node == null)
                return;

            string cteName = node.ExpressionName?.Value ?? "UnknownCTE";
            LogDebug($"Processing CTE: {cteName}");

            // Create a node for the CTE as if it were a table
            var cteNode = new TableNode
            {
                Id = CreateNodeId("CTE", cteName),
                Name = cteName,
                ObjectName = cteName,
                TableType = "CTE",
                Definition = GetSqlText(node)
            };

            Graph.AddNode(cteNode);
            Context.AddTable(cteNode);
            
            // Store the CTE in a special tracking collection for CTEs
            Context.Metadata[$"CTE:{cteName.ToLowerInvariant()}"] = cteNode;

            // Process column list if provided
            ProcessCTEColumnList(node, cteNode);

            // Save current context - we're going to need this to restore later
            var previousContext = Context.GetColumnContext("current");
            
            try
            {
                // Mark that we're processing this CTE
                Context.Metadata["ProcessingCTE"] = cteName;
                
                // Process the query expression
                if (node.QueryExpression != null)
                {
                    Visitor.Visit(node.QueryExpression);
                }
            }
            finally
            {
                // Clean up context
                Context.Metadata.Remove("ProcessingCTE");
                Context.SetColumnContext("current", previousContext);
            }
        }

        /// <summary>
        /// Process a WITH clause containing CTEs
        /// </summary>
        public void ProcessWithClause(TSqlFragment node)
        {
            if (node == null)
                return;
                
            LogDebug($"Processing WITH clause");
            
            // Extract CTEs using reflection to handle different ScriptDom versions
            var ctes = ExtractCTEsFromWithClause(node);
            if (ctes.Count == 0)
                return;
                
            LogDebug($"Found {ctes.Count} CTEs in WITH clause");

            // Mark the context that we're processing a WITH clause
            Context.Metadata["ProcessingWithClause"] = true;

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
                // Clean up context
                Context.Metadata.Remove("ProcessingWithClause");
            }
        }
        
        /// <summary>
        /// Extract CTEs from a WITH clause using reflection (version-agnostic)
        /// </summary>
        private List<CommonTableExpression> ExtractCTEsFromWithClause(TSqlFragment node)
        {
            var result = new List<CommonTableExpression>();
            
            try
            {
                // Try different property names that might contain the CTE collection
                var ctesProp = node.GetType().GetProperty("CommonTableExpressions") ?? 
                               node.GetType().GetProperty("CTEs") ??
                               node.GetType().GetProperty("Expressions");
                               
                if (ctesProp != null)
                {
                    var ctes = ctesProp.GetValue(node) as IEnumerable<CommonTableExpression>;
                    if (ctes != null)
                    {
                        result.AddRange(ctes);
                    }
                }
            }
            catch (Exception ex)
            {
                LogDebug($"Error extracting CTEs from WITH clause: {ex.Message}");
            }
            
            return result;
        }

        /// <summary>
        /// Process a CTE reference in a FROM clause
        /// </summary>
        public void ProcessCTEReference(NamedTableReference node)
        {
            if (node?.SchemaObject?.Identifiers == null || node.SchemaObject.Identifiers.Count == 0)
                return;

            string tableName = string.Join(".", node.SchemaObject.Identifiers.Select(i => i.Value));
            
            // Check if this references a CTE
            string cteKey = $"CTE:{tableName.ToLowerInvariant()}";
            if (!Context.Metadata.ContainsKey(cteKey))
                return;
                
            LogDebug($"Processing CTE reference: {tableName}");
            
            // Get the CTE node
            var cteNode = Context.Metadata[cteKey] as TableNode;
            if (cteNode == null)
                return;
                
            // Handle alias if present
            if (!string.IsNullOrEmpty(node.Alias?.Value))
            {
                Context.AddTableAlias(node.Alias.Value, cteNode.Name);
            }
        }

        /// <summary>
        /// Process the column list of a CTE
        /// </summary>
        private void ProcessCTEColumnList(CommonTableExpression node, TableNode cteNode)
        {
            var columnNames = new List<string>();

            // Extract column names from the explicit column list if defined
            if (node.Columns != null && node.Columns.Count > 0)
            {
                foreach (var column in node.Columns)
                {
                    if (column?.Value != null)
                    {
                        columnNames.Add(column.Value);
                    }
                }
            }
            else
            {
                // Otherwise try to extract columns from the query expression
                // This is more complex and depends on the query structure
                columnNames = ExtractColumnsFromQueryExpression(node.QueryExpression);
            }

            // Create column nodes for the CTE
            foreach (var columnName in columnNames)
            {
                var columnNode = new ColumnNode
                {
                    Id = CreateNodeId("COLUMN", $"{cteNode.Name}.{columnName}"),
                    Name = columnName,
                    ObjectName = columnName,
                    TableOwner = cteNode.Name,
                    DataType = "unknown" // We don't know the data type from the CTE definition
                };

                Graph.AddNode(columnNode);
                cteNode.Columns.Add(columnNode.Id);
            }
        }

        /// <summary>
        /// Attempt to extract column names from a query expression
        /// </summary>
        private List<string> ExtractColumnsFromQueryExpression(QueryExpression queryExpression)
        {
            var columns = new List<string>();

            try
            {
                if (queryExpression is QuerySpecification querySpec && querySpec.SelectElements != null)
                {
                    // Process SELECT elements to extract column names
                    foreach (var element in querySpec.SelectElements)
                    {
                        if (element is SelectScalarExpression scalarExpr)
                        {
                            // Named column (using AS)
                            if (scalarExpr.ColumnName?.Value != null)
                            {
                                columns.Add(scalarExpr.ColumnName.Value);
                            }
                            // Column reference without alias
                            else if (scalarExpr.Expression is ColumnReferenceExpression colRef &&
                                     colRef.MultiPartIdentifier?.Identifiers != null &&
                                     colRef.MultiPartIdentifier.Identifiers.Count > 0)
                            {
                                columns.Add(colRef.MultiPartIdentifier.Identifiers.Last().Value);
                            }
                            // Expression without alias (use a placeholder)
                            else
                            {
                                columns.Add($"Column{columns.Count + 1}");
                            }
                        }
                        else if (element is SelectStarExpression)
                        {
                            // Star expressions are complex as they depend on the tables referenced
                            // This is a simplified placeholder approach
                            columns.Add($"StarColumn{columns.Count + 1}");
                        }
                    }
                }
                else if (queryExpression is BinaryQueryExpression binaryQuery)
                {
                    // For UNION, INTERSECT, etc., get columns from the first query
                    // Both sides should have the same column structure
                    if (binaryQuery.FirstQueryExpression != null)
                    {
                        columns = ExtractColumnsFromQueryExpression(binaryQuery.FirstQueryExpression);
                    }
                }
            }
            catch (Exception ex)
            {
                LogDebug($"Error extracting columns from query expression: {ex.Message}");
            }

            return columns;
        }
    }
}

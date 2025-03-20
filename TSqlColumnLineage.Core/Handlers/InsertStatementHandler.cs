using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Visitors;
using TSqlColumnLineage.Core.Services;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TSqlColumnLineage.Core.Handlers
{
    /// <summary>
    /// Handler for INSERT statements to track lineage from source to target columns
    /// </summary>
    public class InsertStatementHandler : AbstractQueryHandler
    {
        public InsertStatementHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger logger)
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
            if (fragment is InsertStatement insertStmt)
            {
                ProcessInsertStatement(insertStmt);
                return true;
            }
            
            return false;
        }

        /// <summary>
        /// Process an INSERT statement and establish column lineage
        /// </summary>
        public void ProcessInsertStatement(InsertStatement node)
        {
            LogDebug($"===============================================");
            LogDebug($"ProcessInsertStatement called");
            
            if (node?.InsertSpecification?.Target == null || node.InsertSpecification.InsertSource == null)
            {
                LogDebug("Target or source is null, returning");
                return;
            }

            LogDebug($"Processing INSERT statement at Line {node.StartLine}");

            // Get target table name
            string targetTableName = ExtractTableName(node.InsertSpecification.Target);
            LogDebug($"Target table: {targetTableName}");

            // Process column list if available
            List<string> targetColumns = new List<string>();
            if (node.InsertSpecification.Columns != null && node.InsertSpecification.Columns.Count > 0)
            {
                foreach (var column in node.InsertSpecification.Columns)
                {
                    if (column is ColumnReferenceExpression colRef && colRef.MultiPartIdentifier?.Identifiers?.Count > 0)
                    {
                        string columnName = colRef.MultiPartIdentifier.Identifiers.Last().Value;
                        targetColumns.Add(columnName);
                        LogDebug($"Target column: {columnName}");
                    }
                }
            }
            else
            {
                // If no columns are specified, all columns are implicitly targeted
                var tableMetadata = Context.GetTable(targetTableName);
                if (tableMetadata != null && Context.Metadata.TryGetValue("MetadataService", out var service) && service is IMetadataService ms)
                {
                    // Get all columns for the target table from metadata
                    var columns = ms.GetTableColumnsMetadata(targetTableName);
                    targetColumns.AddRange(columns.Select(c => c.Name));
                    LogDebug($"Implicit target columns: {string.Join(", ", targetColumns)}");
                }
            }

            // Process insert source
            if (node.InsertSpecification.InsertSource is ValuesInsertSource valuesSource)
            {
                // INSERT INTO ... VALUES - no lineage to track for literal values
                LogDebug("INSERT VALUES source - no lineage to track for literal values");
            }
            else if (node.InsertSpecification.InsertSource is SelectInsertSource selectSource)
            {
                // INSERT INTO ... SELECT - track lineage from SELECT columns to target columns
                LogDebug($"Processing INSERT ... SELECT source for table {targetTableName} with {targetColumns.Count} columns");
                ProcessInsertSelect(selectSource.Select, targetTableName, targetColumns);
            }
            
            LogDebug($"===============================================");
        }

        /// <summary>
        /// Process an INSERT ... SELECT statement and establish column lineage
        /// </summary>
        private void ProcessInsertSelect(QueryExpression selectQuery, string targetTableName, List<string> targetColumns)
        {
            // Get all tables referenced in the SELECT before processing the query
            var originalTables = new Dictionary<string, TableNode>(Context.Tables);
            
            LogDebug($"ProcessInsertSelect: Beginning processing for {targetTableName}, Column count: {targetColumns.Count}");
            
            // Visit the SELECT query to populate the context with column nodes
            Visitor.Visit(selectQuery);
            
            LogDebug($"ProcessInsertSelect: After visiting SELECT, tables count: {Context.Tables.Count}");
            foreach (var table in Context.Tables.Values)
            {
                LogDebug($"Table in context: {table.Name}");
            }
            
            LogDebug($"ProcessInsertSelect: Target table={targetTableName}, Target columns={string.Join(", ", targetColumns)}");

            // After visiting the SELECT, lineage tracking can be established
            // Extract source columns from the SELECT clause
            if (selectQuery is QuerySpecification querySpec && querySpec.SelectElements != null)
            {
                LogDebug($"ProcessInsertSelect: Found QuerySpecification with {querySpec.SelectElements.Count} select elements");
                
                List<SourceColumnMapping> sourceColumns = ExtractSourceColumns(querySpec.SelectElements);
                LogDebug($"ProcessInsertSelect: Extracted {sourceColumns.Count} source column mappings");
                
                // Create lineage edges from source columns to target columns
                CreateLineageEdges(sourceColumns, targetTableName, targetColumns);
            }
            else
            {
                LogDebug($"ProcessInsertSelect: Query is not a QuerySpecification or has no select elements");
            }
        }

        /// <summary>
        /// Extract source columns from a SELECT statement
        /// </summary>
        private List<SourceColumnMapping> ExtractSourceColumns(IList<SelectElement> selectElements)
        {
            var sourceColumns = new List<SourceColumnMapping>();
            
            foreach (var element in selectElements)
            {
                if (element is SelectScalarExpression scalarExpr)
                {
                    LogDebug($"Processing SELECT element: {GetSqlText(scalarExpr)}");
                    
                    if (scalarExpr.Expression is ColumnReferenceExpression colRef)
                    {
                        // Direct column reference (e.g., TableName.ColumnName or ColumnName)
                        string columnName = colRef.MultiPartIdentifier?.Identifiers?.Last()?.Value;
                        string tableName = null;
                        
                        // Get the table name if specified
                        if (colRef.MultiPartIdentifier?.Identifiers?.Count > 1)
                        {
                            tableName = colRef.MultiPartIdentifier.Identifiers[0].Value;
                            
                            // Resolve table alias if needed
                            if (Context.TableAliases.TryGetValue(tableName, out string resolvedTable))
                            {
                                tableName = resolvedTable;
                            }
                            LogDebug($"Found multi-part identifier with table name: {tableName}.{columnName}");
                        }
                        else
                        {
                            // Try to infer table from context (only one table or explicit join)
                            if (Context.Tables.Count == 1)
                            {
                                tableName = Context.Tables.Values.First().Name;
                                LogDebug($"Inferred table name from single table context: {tableName}");
                            }
                            else
                            {
                                LogDebug($"Multiple tables in context, will search for column {columnName} across all tables");
                            }
                        }
                        
                        sourceColumns.Add(new SourceColumnMapping
                        {
                            ColumnName = columnName,
                            TableName = tableName,
                            Alias = scalarExpr.ColumnName?.Value,
                            Expression = colRef
                        });
                        
                        LogDebug($"Added source column mapping: Table={tableName}, Column={columnName}, Alias={scalarExpr.ColumnName?.Value}");
                    }
                    else if (scalarExpr.Expression is ScalarExpression expr)
                    {
                        // Complex expression (function, calculation, etc.)
                        sourceColumns.Add(new SourceColumnMapping
                        {
                            Expression = expr,
                            Alias = scalarExpr.ColumnName?.Value,
                            IsExpression = true
                        });
                        
                        LogDebug($"Added expression mapping: Expression={GetSqlText(expr)}, Alias={scalarExpr.ColumnName?.Value}");
                    }
                }
                else
                {
                    LogDebug($"Unsupported SELECT element type: {element.GetType().Name}");
                }
            }
            
            return sourceColumns;
        }
        
        /// <summary>
        /// Create lineage edges from source columns to target columns
        /// </summary>
        private void CreateLineageEdges(List<SourceColumnMapping> sourceColumns, string targetTableName, List<string> targetColumnNames)
        {
            // Match source columns to target columns and create edges
            LogDebug($"CreateLineageEdges: Creating edges from {sourceColumns.Count} source columns to {targetColumnNames.Count} target columns");
            LogDebug($"Target table: {targetTableName}, Target columns: {string.Join(", ", targetColumnNames)}");
            
            // Ensure we have target columns
            if (targetColumnNames.Count == 0)
            {
                LogDebug("No target columns specified, cannot create lineage edges");
                return;
            }
            
            // If source and target column counts don't match, log a warning
            if (sourceColumns.Count != targetColumnNames.Count)
            {
                LogDebug($"WARNING: Source column count ({sourceColumns.Count}) doesn't match target column count ({targetColumnNames.Count})");
            }
            
            // First, create a dictionary of all columns in the source and target tables
            Dictionary<string, ColumnNode> sourceTableColumns = new Dictionary<string, ColumnNode>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, ColumnNode> targetTableColumns = new Dictionary<string, ColumnNode>(StringComparer.OrdinalIgnoreCase);

            // Gather all columns from tables in context
            foreach (var tableNode in Context.Tables.Values)
            {
                foreach (var columnId in tableNode.Columns)
                {
                    if (Graph.GetNodeById(columnId) is ColumnNode columnNode)
                    {
                        if (columnNode.TableOwner.Equals(targetTableName, StringComparison.OrdinalIgnoreCase))
                        {
                            targetTableColumns[columnNode.Name] = columnNode;
                            LogDebug($"Found target column: {columnNode.TableOwner}.{columnNode.Name}");
                        }
                        else 
                        {
                            sourceTableColumns[columnNode.Name] = columnNode;
                            LogDebug($"Found source column: {columnNode.TableOwner}.{columnNode.Name}");
                        }
                    }
                }
            }

            // Try regular source to target mapping first
            for (int i = 0; i < Math.Min(sourceColumns.Count, targetColumnNames.Count); i++)
            {
                var sourceMapping = sourceColumns[i];
                string targetColumnName = targetColumnNames[i];
                
                LogDebug($"Processing pair {i}: Source={sourceMapping.TableName}.{sourceMapping.ColumnName}, Target={targetTableName}.{targetColumnName}, IsExpression={sourceMapping.IsExpression}");
                
                // Find the target column node
                ColumnNode targetColumn = null;
                if (targetTableColumns.TryGetValue(targetColumnName, out var foundTarget))
                {
                    targetColumn = foundTarget;
                }
                else
                {
                    targetColumn = FindColumnNode(targetTableName, targetColumnName);
                }

                if (targetColumn == null)
                {
                    LogDebug($"Target column not found: {targetTableName}.{targetColumnName}");
                    continue;
                }
                
                LogDebug($"Found target column: {targetColumn.TableOwner}.{targetColumn.Name} with ID: {targetColumn.Id}");
                
                if (!sourceMapping.IsExpression && !string.IsNullOrEmpty(sourceMapping.ColumnName))
                {
                    // Direct column reference
                    ColumnNode sourceColumn = null;
                    
                    if (sourceTableColumns.TryGetValue(sourceMapping.ColumnName, out var foundSource))
                    {
                        sourceColumn = foundSource;
                        LogDebug($"Found source column in dictionary: {sourceColumn.TableOwner}.{sourceColumn.Name}");
                    }
                    else if (!string.IsNullOrEmpty(sourceMapping.TableName))
                    {
                        // We know the table name
                        sourceColumn = FindColumnNode(sourceMapping.TableName, sourceMapping.ColumnName);
                        LogDebug($"Looking for source column in specific table: {sourceMapping.TableName}.{sourceMapping.ColumnName}, Found: {sourceColumn != null}");
                    }
                    else
                    {
                        // We need to search all tables
                        LogDebug($"Searching for column {sourceMapping.ColumnName} in all tables");
                        foreach (var table in Context.Tables.Values)
                        {
                            var column = FindColumnNode(table.Name, sourceMapping.ColumnName);
                            if (column != null)
                            {
                                sourceColumn = column;
                                LogDebug($"Found source column in table {table.Name}: {column.TableOwner}.{column.Name}");
                                break;
                            }
                        }
                    }
                    
                    if (sourceColumn != null)
                    {
                        LogDebug($"Creating edge from source column ID: {sourceColumn.Id} to target column ID: {targetColumn.Id}");
                        
                        // Create direct lineage edge from source column to target column
                        var edge = new LineageEdge
                        {
                            Id = Guid.NewGuid().ToString(),
                            SourceId = sourceColumn.Id,
                            TargetId = targetColumn.Id,
                            Type = "Direct",
                            Operation = "INSERT"
                        };
                        
                        Graph.AddEdge(edge);
                        LogDebug($"Created lineage edge: {sourceColumn.TableOwner}.{sourceColumn.Name} -> {targetColumn.TableOwner}.{targetColumn.Name}");
                    }
                    else
                    {
                        LogDebug($"Source column not found: {sourceMapping.TableName ?? "unknown"}.{sourceMapping.ColumnName}");
                        
                        // Try with original column reference as fallback
                        if (sourceMapping.Expression is ColumnReferenceExpression colRef)
                        {
                            LogDebug("Trying to create edge with direct column reference as fallback");
                            
                            // Create expression node for this column reference
                            var expressionId = CreateNodeId("EXPR", $"INSERT_{targetColumnName}_{Guid.NewGuid().ToString().Substring(0, 8)}");
                            var expressionNode = new ExpressionNode
                            {
                                Id = expressionId,
                                Name = sourceMapping.Alias ?? $"ColExpr_{i}",
                                ObjectName = GetSqlText(colRef),
                                Type = "ColumnReference",
                                Expression = GetSqlText(colRef)
                            };
                            
                            Graph.AddNode(expressionNode);
                            LogDebug($"Created column reference node: {expressionNode.ObjectName}");
                            
                            // Create edge to target column
                            var edge = new LineageEdge
                            {
                                Id = Guid.NewGuid().ToString(),
                                SourceId = expressionNode.Id,
                                TargetId = targetColumn.Id,
                                Type = "Direct",
                                Operation = "INSERT"
                            };
                            
                            Graph.AddEdge(edge);
                            LogDebug($"Created fallback lineage edge: {expressionNode.ObjectName} -> {targetColumn.TableOwner}.{targetColumn.Name}");
                        }
                    }
                }
                else if (sourceMapping.IsExpression && sourceMapping.Expression != null)
                {
                    // Handle expressions (functions, calculations, etc.)
                    // Create an expression node
                    var expressionId = CreateNodeId("EXPR", $"INSERT_{targetColumnName}_{Guid.NewGuid().ToString().Substring(0, 8)}");
                    
                    var expressionNode = new ExpressionNode
                    {
                        Id = expressionId,
                        Name = sourceMapping.Alias ?? $"Expression_{i}",
                        ObjectName = GetSqlText(sourceMapping.Expression),
                        Type = "Expression",
                        Expression = GetSqlText(sourceMapping.Expression)
                    };
                    
                    Graph.AddNode(expressionNode);
                    LogDebug($"Created expression node: {expressionNode.ObjectName}");
                    
                    // Extract column references from the expression and link them
                    ExtractColumnReferencesFromExpression(sourceMapping.Expression, expressionNode);
                    
                    // Create edge from expression to target column
                    var edge = new LineageEdge
                    {
                        Id = Guid.NewGuid().ToString(),
                        SourceId = expressionNode.Id,
                        TargetId = targetColumn.Id,
                        Type = "Indirect",
                        Operation = "INSERT"
                    };
                    
                    Graph.AddEdge(edge);
                    LogDebug($"Created lineage edge from expression to target: {expressionNode.ObjectName} -> {targetColumn.TableOwner}.{targetColumn.Name}");
                }
            }
            
            // Direct column mapping based on name matching as a last resort
            // This helps when the SELECT statement columns don't have clear source mapping
            LogDebug("Attempting direct column mapping based on name matching");
            
            // Iterate through target columns and try to find matching source columns by name
            foreach (var targetColName in targetColumnNames)
            {
                if (targetTableColumns.TryGetValue(targetColName, out var targetCol))
                {
                    // Check if we already created an edge for this target column
                    var existingEdges = Graph.Edges.Where(e => e.TargetId == targetCol.Id).ToList();
                    if (existingEdges.Count > 0)
                    {
                        LogDebug($"Target column {targetCol.TableOwner}.{targetCol.Name} already has {existingEdges.Count} incoming edges, skipping direct mapping");
                        continue;
                    }
                    
                    // Try to find a matching source column with the same name
                    if (sourceTableColumns.TryGetValue(targetColName, out var sourceCol))
                    {
                        // Create a direct edge
                        var edge = new LineageEdge
                        {
                            Id = Guid.NewGuid().ToString(),
                            SourceId = sourceCol.Id,
                            TargetId = targetCol.Id,
                            Type = "Direct",
                            Operation = "INSERT"
                        };
                        
                        Graph.AddEdge(edge);
                        LogDebug($"Created direct name-matching edge: {sourceCol.TableOwner}.{sourceCol.Name} -> {targetCol.TableOwner}.{targetCol.Name}");
                    }
                }
            }
        }

        /// <summary>
        /// Extract column references from a complex expression and link them to the expression node
        /// </summary>
        private void ExtractColumnReferencesFromExpression(ScalarExpression expression, ExpressionNode expressionNode)
        {
            if (expression is ColumnReferenceExpression columnRef)
            {
                // Direct column reference
                string columnName = columnRef.MultiPartIdentifier?.Identifiers?.Last()?.Value;
                string tableName = null;
                
                // Get the table name if specified
                if (columnRef.MultiPartIdentifier?.Identifiers?.Count > 1)
                {
                    tableName = columnRef.MultiPartIdentifier.Identifiers[0].Value;
                    
                    // Resolve table alias if needed
                    if (Context.TableAliases.TryGetValue(tableName, out string resolvedTable))
                    {
                        tableName = resolvedTable;
                    }
                }
                
                // Find the column node
                ColumnNode columnNode = null;
                if (!string.IsNullOrEmpty(tableName))
                {
                    columnNode = FindColumnNode(tableName, columnName);
                }
                else
                {
                    // Search all tables for the column
                    foreach (var table in Context.Tables.Values)
                    {
                        var col = FindColumnNode(table.Name, columnName);
                        if (col != null)
                        {
                            columnNode = col;
                            break;
                        }
                    }
                }
                
                if (columnNode != null)
                {
                    // Create edge from column to expression
                    var edge = new LineageEdge
                    {
                        Id = Guid.NewGuid().ToString(),
                        SourceId = columnNode.Id,
                        TargetId = expressionNode.Id,
                        Type = "Indirect",
                        Operation = "INSERT"
                    };
                    
                    Graph.AddEdge(edge);
                    LogDebug($"Created lineage edge: {columnNode.TableOwner}.{columnNode.Name} -> {expressionNode.ObjectName}");
                }
            }
            else if (expression is BinaryExpression binaryExpr)
            {
                // Process both sides of the binary expression
                ExtractColumnReferencesFromExpression(binaryExpr.FirstExpression, expressionNode);
                ExtractColumnReferencesFromExpression(binaryExpr.SecondExpression, expressionNode);
            }
            else if (expression is FunctionCall functionCall)
            {
                // Process function parameters
                foreach (var parameter in functionCall.Parameters)
                {
                    if (parameter is ScalarExpression scalarParam)
                    {
                        ExtractColumnReferencesFromExpression(scalarParam, expressionNode);
                    }
                }
            }
            else if (expression is ParenthesisExpression parenExpr)
            {
                // Process the expression inside parentheses
                ExtractColumnReferencesFromExpression(parenExpr.Expression, expressionNode);
            }
            else if (expression is SearchedCaseExpression caseExpr)
            {
                // Process CASE expression
                foreach (var whenClause in caseExpr.WhenClauses)
                {
                    // The whenExpression is a BooleanExpression, we need to handle it differently
                    ExtractColumnReferencesFromBooleanExpression(whenClause.WhenExpression, expressionNode);
                    ExtractColumnReferencesFromExpression(whenClause.ThenExpression, expressionNode);
                }
                
                if (caseExpr.ElseExpression != null)
                {
                    ExtractColumnReferencesFromExpression(caseExpr.ElseExpression, expressionNode);
                }
            }
            else if (expression is SimpleCaseExpression simpleCaseExpr)
            {
                // Process simple CASE expression
                ExtractColumnReferencesFromExpression(simpleCaseExpr.InputExpression, expressionNode);
                
                foreach (var whenClause in simpleCaseExpr.WhenClauses)
                {
                    ExtractColumnReferencesFromExpression(whenClause.WhenExpression, expressionNode);
                    ExtractColumnReferencesFromExpression(whenClause.ThenExpression, expressionNode);
                }
                
                if (simpleCaseExpr.ElseExpression != null)
                {
                    ExtractColumnReferencesFromExpression(simpleCaseExpr.ElseExpression, expressionNode);
                }
            }
        }
        
        /// <summary>
        /// Extract column references from a Boolean expression and link them to the expression node
        /// </summary>
        private void ExtractColumnReferencesFromBooleanExpression(BooleanExpression expression, ExpressionNode expressionNode)
        {
            if (expression is BooleanComparisonExpression comparisonExpr)
            {
                // Process both sides of the comparison
                if (comparisonExpr.FirstExpression is ScalarExpression firstScalar)
                {
                    ExtractColumnReferencesFromExpression(firstScalar, expressionNode);
                }
                
                if (comparisonExpr.SecondExpression is ScalarExpression secondScalar)
                {
                    ExtractColumnReferencesFromExpression(secondScalar, expressionNode);
                }
            }
            else if (expression is BooleanBinaryExpression binaryExpr)
            {
                // Process both sides of the binary expression (AND, OR)
                ExtractColumnReferencesFromBooleanExpression(binaryExpr.FirstExpression, expressionNode);
                ExtractColumnReferencesFromBooleanExpression(binaryExpr.SecondExpression, expressionNode);
            }
            else if (expression is BooleanParenthesisExpression parenExpr)
            {
                // Process the expression inside parentheses
                ExtractColumnReferencesFromBooleanExpression(parenExpr.Expression, expressionNode);
            }
            else if (expression is BooleanNotExpression notExpr)
            {
                // Process the expression being negated
                ExtractColumnReferencesFromBooleanExpression(notExpr.Expression, expressionNode);
            }
            else if (expression is BooleanIsNullExpression isNullExpr)
            {
                // Process the expression being checked for NULL
                if (isNullExpr.Expression is ScalarExpression scalarExpr)
                {
                    ExtractColumnReferencesFromExpression(scalarExpr, expressionNode);
                }
            }
            else if (expression is InPredicate inExpr)
            {
                // Process the expression and values in the IN predicate
                if (inExpr.Expression is ScalarExpression scalarExpr)
                {
                    ExtractColumnReferencesFromExpression(scalarExpr, expressionNode);
                }
                
                // Process values in the IN list
                if (inExpr.Values != null)
                {
                    foreach (var value in inExpr.Values)
                    {
                        if (value is ScalarExpression valueExpr)
                        {
                            ExtractColumnReferencesFromExpression(valueExpr, expressionNode);
                        }
                    }
                }
            }
            else if (expression is LikePredicate likeExpr)
            {
                // Process LIKE predicate
                if (likeExpr.FirstExpression is ScalarExpression firstExpr)
                {
                    ExtractColumnReferencesFromExpression(firstExpr, expressionNode);
                }
                
                if (likeExpr.SecondExpression is ScalarExpression secondExpr)
                {
                    ExtractColumnReferencesFromExpression(secondExpr, expressionNode);
                }
                
                if (likeExpr.EscapeExpression is ScalarExpression escapeExpr)
                {
                    ExtractColumnReferencesFromExpression(escapeExpr, expressionNode);
                }
            }
        }

        /// <summary>
        /// Find a column node by table and column name
        /// </summary>
        private ColumnNode FindColumnNode(string tableName, string columnName)
        {
            LogDebug($"Finding column: {tableName}.{columnName}");
            var result = Graph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(c => string.Equals(c.TableOwner, tableName, StringComparison.OrdinalIgnoreCase) &&
                                    string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
            
            if (result != null)
                LogDebug($"Found column: {result.TableOwner}.{result.Name} with ID {result.Id}");
            else
                LogDebug($"Column not found: {tableName}.{columnName}");
                
            return result;
        }

        /// <summary>
        /// Extract table name from a table reference
        /// </summary>
        private string ExtractTableName(TableReference tableRef)
        {
            if (tableRef is NamedTableReference namedTableRef)
            {
                return string.Join(".", namedTableRef.SchemaObject.Identifiers.Select(i => i.Value));
            }
            
            return "Unknown";
        }
    }

    /// <summary>
    /// Helper class to track source column mappings in INSERT...SELECT
    /// </summary>
    internal class SourceColumnMapping
    {
        public string? TableName { get; set; }
        public string? ColumnName { get; set; }
        public string? Alias { get; set; }
        public ScalarExpression? Expression { get; set; }
        public bool IsExpression { get; set; }
    }
} 
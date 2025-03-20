using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Models.Edges;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Statements
{
    /// <summary>
    /// Handler for INSERT statements
    /// </summary>
    public class InsertStatementHandler : AbstractQueryHandler
    {
        /// <summary>
        /// Creates a new INSERT statement handler
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="logger">Logger (optional)</param>
        public InsertStatementHandler(VisitorContext context, ILogger logger = null)
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
            return fragment is InsertStatement;
        }
        
        /// <summary>
        /// Processes the SQL fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <param name="context">Visitor context</param>
        /// <returns>True if the fragment was fully processed; otherwise, false</returns>
        public override bool Handle(TSqlFragment fragment, VisitorContext context)
        {
            if (fragment is InsertStatement insertStmt)
            {
                ProcessInsertStatement(insertStmt);
                return true;
            }
            
            return false;
        }
        
        /// <summary>
        /// Processes an INSERT statement
        /// </summary>
        private void ProcessInsertStatement(InsertStatement insert)
        {
            if (insert?.InsertSpecification?.Target == null || 
                insert.InsertSpecification.InsertSource == null)
                return;
                
            LogDebug($"Processing INSERT statement at line {insert.StartLine}");
            
            // Extract target table
            string targetTableName = ExtractTableName(insert.InsertSpecification.Target);
            if (string.IsNullOrEmpty(targetTableName))
                return;
                
            LogDebug($"Target table: {targetTableName}");
            
            // Get or create target table node
            var targetTable = Context.GetOrCreateTableNode(
                targetTableName, 
                targetTableName.StartsWith("#") ? "TempTable" : "Table"
            );
            
            // Extract target columns
            var targetColumnNames = new List<string>();
            
            if (insert.InsertSpecification.Columns != null && insert.InsertSpecification.Columns.Count > 0)
            {
                // Explicit column list
                foreach (var columnItem in insert.InsertSpecification.Columns)
                {
                    if (columnItem is ColumnReferenceExpression colRef &&
                        colRef.MultiPartIdentifier?.Identifiers.Count > 0)
                    {
                        targetColumnNames.Add(colRef.MultiPartIdentifier.Identifiers.Last().Value);
                    }
                }
                
                LogDebug($"Target columns: {string.Join(", ", targetColumnNames)}");
            }
            else
            {
                // Implicit column list - try to get all columns from the table
                var tableColumns = targetTable.Columns
                    .Select(id => Graph.GetNodeById(id) as ColumnNode)
                    .Where(c => c != null)
                    .ToList();
                    
                if (tableColumns.Count > 0)
                {
                    targetColumnNames.AddRange(tableColumns.Select(c => c.Name));
                    LogDebug($"Implicit target columns: {string.Join(", ", targetColumnNames)}");
                }
                else
                {
                    LogWarning($"No columns found for target table {targetTableName}");
                }
            }
            
            // Process the INSERT source
            if (insert.InsertSpecification.InsertSource is ValuesInsertSource valuesSource)
            {
                // INSERT INTO ... VALUES
                ProcessValuesInsert(valuesSource, targetTableName, targetColumnNames);
            }
            else if (insert.InsertSpecification.InsertSource is SelectInsertSource selectSource)
            {
                // INSERT INTO ... SELECT
                ProcessSelectInsert(selectSource, targetTableName, targetColumnNames);
            }
            else if (insert.InsertSpecification.InsertSource is ExecuteInsertSource executeSource)
            {
                // INSERT INTO ... EXEC
                ProcessExecuteInsert(executeSource, targetTableName, targetColumnNames);
            }
        }
        
        /// <summary>
        /// Processes an INSERT with VALUES
        /// </summary>
        private void ProcessValuesInsert(ValuesInsertSource valuesSource, string targetTableName, List<string> targetColumnNames)
        {
            LogDebug("Processing INSERT VALUES");
            
            if (valuesSource.RowValues == null || valuesSource.RowValues.Count == 0)
                return;
                
            // Create an expression node for each value in each row
            foreach (var rowValue in valuesSource.RowValues)
            {
                if (rowValue?.ColumnValues == null)
                    continue;
                    
                for (int i = 0; i < Math.Min(rowValue.ColumnValues.Count, targetColumnNames.Count); i++)
                {
                    var columnValue = rowValue.ColumnValues[i];
                    var targetColumnName = targetColumnNames[i];
                    
                    // Get or create target column
                    var targetColumn = Context.GetOrCreateColumnNode(targetTableName, targetColumnName);
                    
                    if (targetColumn != null && columnValue != null)
                    {
                        // Create expression node for the value
                        var expressionNode = new ExpressionNode
                        {
                            Id = CreateNodeId("EXPR", $"Value_{targetTableName}_{targetColumnName}"),
                            Name = $"Value_{targetColumnName}",
                            ObjectName = GetSqlText(columnValue),
                            ExpressionType = "Value",
                            Expression = GetSqlText(columnValue),
                            TableOwner = targetTableName,
                            ResultType = targetColumn.DataType
                        };
                        
                        Graph.AddNode(expressionNode);
                        
                        // Create edge from value expression to target column
                        var edge = CreateDirectEdge(
                            expressionNode.Id,
                            targetColumn.Id,
                            "insert",
                            $"Insert value into {targetTableName}.{targetColumnName}: {GetSqlText(columnValue)}"
                        );
                        
                        Graph.AddEdge(edge);
                        
                        // Extract any column references from the value
                        var columnRefs = new List<ColumnReferenceExpression>();
                        ExtractColumnReferences(columnValue, columnRefs);
                        
                        foreach (var colRef in columnRefs)
                        {
                            // Find source column
                            var sourceColumn = FindSourceColumn(colRef);
                            
                            if (sourceColumn != null)
                            {
                                // Create edge from source column to value expression
                                var sourceEdge = CreateIndirectEdge(
                                    sourceColumn.Id,
                                    expressionNode.Id,
                                    "reference",
                                    $"Referenced in insert value: {sourceColumn.TableOwner}.{sourceColumn.Name}"
                                );
                                
                                Graph.AddEdge(sourceEdge);
                            }
                        }
                    }
                }
            }
        }
        
        /// <summary>
        /// Processes an INSERT with SELECT
        /// </summary>
        private void ProcessSelectInsert(SelectInsertSource selectSource, string targetTableName, List<string> targetColumnNames)
        {
            LogDebug("Processing INSERT SELECT");
            
            if (selectSource.Select == null)
                return;
                
            // Set context for processing the SELECT
            Context.State["InsertTargetTable"] = targetTableName;
            Context.State["InsertTargetColumns"] = targetColumnNames;
            
            try
            {
                // Process the SELECT to determine columns
                // This is just to ensure the query is traversed - actual column lineage
                // will be handled by the visitor
                var visitor = Context.GetType().Assembly.CreateInstance(
                    "TSqlColumnLineage.Core.Analysis.Visitors.Specialized.ColumnLineageVisitor",
                    false,
                    System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
                    null,
                    new[] { Context, null, Logger },
                    null,
                    null) as TSqlFragmentVisitor;
                    
                if (visitor != null)
                {
                    selectSource.Select.Accept(visitor);
                }
                
                // Try to infer column names and map them to target columns
                if (selectSource.Select.QueryExpression is QuerySpecification querySpec && 
                    querySpec.SelectElements != null)
                {
                    MapSelectColumnsToTargetColumns(querySpec.SelectElements, targetTableName, targetColumnNames);
                }
            }
            finally
            {
                // Clear context
                Context.State.Remove("InsertTargetTable");
                Context.State.Remove("InsertTargetColumns");
            }
        }
        
        /// <summary>
        /// Processes an INSERT with EXEC
        /// </summary>
        private void ProcessExecuteInsert(ExecuteInsertSource executeSource, string targetTableName, List<string> targetColumnNames)
        {
            LogDebug("Processing INSERT EXEC");
            
            if (executeSource.Execute == null)
                return;
                
            // Set context for processing the EXEC
            Context.State["InsertTargetTable"] = targetTableName;
            Context.State["InsertTargetColumns"] = targetColumnNames;
            
            try
            {
                // Process the EXEC to determine columns
                // This is just to ensure the query is traversed - actual column lineage
                // will be determined by the stored procedure handler
                var visitor = Context.GetType().Assembly.CreateInstance(
                    "TSqlColumnLineage.Core.Analysis.Visitors.Specialized.ColumnLineageVisitor",
                    false,
                    System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
                    null,
                    new[] { Context, null, Logger },
                    null,
                    null) as TSqlFragmentVisitor;
                    
                if (visitor != null)
                {
                    executeSource.Execute.Accept(visitor);
                }
                
                // We can't reliably map stored procedure output to target columns
                // without knowing the structure of the stored procedure's output
                // But we can create placeholder expressions to indicate the linkage
                
                for (int i = 0; i < targetColumnNames.Count; i++)
                {
                    var targetColumnName = targetColumnNames[i];
                    
                    // Get or create target column
                    var targetColumn = Context.GetOrCreateColumnNode(targetTableName, targetColumnName);
                    
                    if (targetColumn != null)
                    {
                        // Create expression node for the EXEC output
                        var expressionNode = new ExpressionNode
                        {
                            Id = CreateNodeId("EXPR", $"Exec_{targetTableName}_{targetColumnName}"),
                            Name = $"Exec_{targetColumnName}",
                            ObjectName = GetSqlText(executeSource.Execute),
                            ExpressionType = "StoredProcedureOutput",
                            Expression = GetSqlText(executeSource.Execute),
                            TableOwner = targetTableName,
                            ResultType = targetColumn.DataType
                        };
                        
                        Graph.AddNode(expressionNode);
                        
                        // Create edge from EXEC expression to target column
                        var edge = CreateDirectEdge(
                            expressionNode.Id,
                            targetColumn.Id,
                            "insert",
                            $"Insert from stored procedure output into {targetTableName}.{targetColumnName}"
                        );
                        
                        Graph.AddEdge(edge);
                    }
                }
            }
            finally
            {
                // Clear context
                Context.State.Remove("InsertTargetTable");
                Context.State.Remove("InsertTargetColumns");
            }
        }
        
        /// <summary>
        /// Maps columns from SELECT to target columns
        /// </summary>
        private void MapSelectColumnsToTargetColumns(
            IList<SelectElement> selectElements, 
            string targetTableName,
            List<string> targetColumnNames)
        {
            // Make sure we have target columns
            if (targetColumnNames == null || targetColumnNames.Count == 0)
                return;
                
            // Special case for SELECT * - can't reliably map columns
            if (selectElements.Count == 1 && selectElements[0] is SelectStarExpression)
            {
                LogDebug("SELECT * found, can't reliably map columns to targets");
                return;
            }
            
            // Map each select element to a target column
            for (int i = 0; i < Math.Min(selectElements.Count, targetColumnNames.Count); i++)
            {
                if (selectElements[i] is SelectScalarExpression scalarExpr)
                {
                    var targetColumnName = targetColumnNames[i];
                    
                    // Get or create target column
                    var targetColumn = Context.GetOrCreateColumnNode(targetTableName, targetColumnName);
                    
                    if (targetColumn != null && scalarExpr.Expression != null)
                    {
                        if (scalarExpr.Expression is ColumnReferenceExpression colRef)
                        {
                            // Direct column reference
                            var sourceColumn = FindSourceColumn(colRef);
                            
                            if (sourceColumn != null)
                            {
                                // Create direct edge from source column to target column
                                var edge = CreateDirectEdge(
                                    sourceColumn.Id,
                                    targetColumn.Id,
                                    "insert",
                                    $"Insert from {sourceColumn.TableOwner}.{sourceColumn.Name} to {targetTableName}.{targetColumnName}"
                                );
                                
                                Graph.AddEdge(edge);
                                LogDebug($"Created insert lineage: {sourceColumn.TableOwner}.{sourceColumn.Name} -> {targetTableName}.{targetColumnName}");
                                
                                // If target column has unknown data type, use source column's type
                                if (targetColumn.DataType == "unknown" && sourceColumn.DataType != "unknown")
                                {
                                    targetColumn.DataType = sourceColumn.DataType;
                                }
                            }
                        }
                        else
                        {
                            // Complex expression
                            var expressionNode = new ExpressionNode
                            {
                                Id = CreateNodeId("EXPR", $"Insert_{targetTableName}_{targetColumnName}"),
                                Name = $"Insert_{targetColumnName}",
                                ObjectName = GetSqlText(scalarExpr.Expression),
                                ExpressionType = "InsertExpression",
                                Expression = GetSqlText(scalarExpr.Expression),
                                TableOwner = targetTableName,
                                ResultType = targetColumn.DataType
                            };
                            
                            Graph.AddNode(expressionNode);
                            
                            // Create edge from expression to target column
                            var edge = CreateDirectEdge(
                                expressionNode.Id,
                                targetColumn.Id,
                                "insert",
                                $"Insert from expression to {targetTableName}.{targetColumnName}"
                            );
                            
                            Graph.AddEdge(edge);
                            
                            // Extract column references from the expression
                            var columnRefs = new List<ColumnReferenceExpression>();
                            ExtractColumnReferences(scalarExpr.Expression, columnRefs);
                            
                            foreach (var colRef in columnRefs)
                            {
                                var sourceColumn = FindSourceColumn(colRef);
                                
                                if (sourceColumn != null)
                                {
                                    // Create edge from source column to expression
                                    var sourceEdge = CreateIndirectEdge(
                                        sourceColumn.Id,
                                        expressionNode.Id,
                                        "reference",
                                        $"Referenced in insert expression: {sourceColumn.TableOwner}.{sourceColumn.Name}"
                                    );
                                    
                                    Graph.AddEdge(sourceEdge);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        /// <summary>
        /// Finds a source column from a column reference
        /// </summary>
        private ColumnNode FindSourceColumn(ColumnReferenceExpression colRef)
        {
            if (colRef?.MultiPartIdentifier?.Identifiers == null || 
                colRef.MultiPartIdentifier.Identifiers.Count == 0)
                return null;
                
            string columnName = colRef.MultiPartIdentifier.Identifiers.Last().Value;
            string tableName = null;
            
            // If table is specified
            if (colRef.MultiPartIdentifier.Identifiers.Count > 1)
            {
                tableName = colRef.MultiPartIdentifier.Identifiers[0].Value;
                
                // Resolve alias
                if (LineageContext.TableAliases.TryGetValue(tableName, out var resolvedTable))
                {
                    tableName = resolvedTable;
                }
            }
            
            // Look up the column
            if (!string.IsNullOrEmpty(tableName))
            {
                return Graph.GetColumnNode(tableName, columnName);
            }
            
            // Try to find in any table
            foreach (var table in LineageContext.Tables.Values)
            {
                var column = Graph.GetColumnNode(table.Name, columnName);
                if (column != null)
                    return column;
            }
            
            return null;
        }
        
        /// <summary>
        /// Extracts a table name from a table reference
        /// </summary>
        private string ExtractTableName(TableReference tableRef)
        {
            if (tableRef is NamedTableReference namedTable && 
                namedTable.SchemaObject?.Identifiers != null &&
                namedTable.SchemaObject.Identifiers.Count > 0)
            {
                return string.Join(".", namedTable.SchemaObject.Identifiers.Select(i => i.Value));
            }
            
            return string.Empty;
        }
    }
}
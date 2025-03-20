using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Linq;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Visitors.Specialized
{
    /// <summary>
    /// Specialized visitor for handling table operations
    /// </summary>
    public class TableVisitor : BaseVisitor
    {
        /// <summary>
        /// Creates a new table visitor
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="logger">Logger (optional)</param>
        public TableVisitor(VisitorContext context, ILogger logger = null)
            : base(context, logger)
        {
        }
        
        /// <summary>
        /// Process CREATE TABLE statement
        /// </summary>
        public override void ExplicitVisit(CreateTableStatement node)
        {
            if (node?.SchemaObjectName?.Identifiers == null) return;
            
            string tableName = string.Join(".", node.SchemaObjectName.Identifiers.Select(i => i.Value));
            string schemaName = node.SchemaObjectName.Identifiers.Count > 1 
                ? node.SchemaObjectName.Identifiers[0].Value 
                : "dbo";
                
            LogDebug($"Processing CREATE TABLE {tableName}");
            
            // Determine table type
            string tableType = "Table";
            if (tableName.StartsWith("#"))
            {
                tableType = "TempTable";
            }
            
            // Create table node
            var tableNode = new TableNode
            {
                Id = CreateNodeId("TABLE", tableName),
                Name = tableName,
                ObjectName = tableName,
                SchemaName = schemaName,
                TableType = tableType,
                Definition = GetSqlText(node)
            };
            
            Graph.AddNode(tableNode);
            LineageContext.AddTable(tableNode);
            
            // Process column definitions
            if (node.Definition?.ColumnDefinitions != null)
            {
                foreach (var columnDef in node.Definition.ColumnDefinitions)
                {
                    ProcessColumnDefinition(columnDef, tableNode);
                }
            }
            
            // Process constraints
            if (node.Definition?.TableConstraints != null)
            {
                foreach (var constraint in node.Definition.TableConstraints)
                {
                    ProcessTableConstraint(constraint, tableNode);
                }
            }
        }
        
        /// <summary>
        /// Process a column definition
        /// </summary>
        private void ProcessColumnDefinition(ColumnDefinition columnDef, TableNode tableNode)
        {
            if (columnDef?.ColumnIdentifier?.Value == null) return;
            
            string columnName = columnDef.ColumnIdentifier.Value;
            string dataType = columnDef.DataType != null ? GetSqlText(columnDef.DataType) : "unknown";
            
            LogDebug($"Processing column definition {columnName} ({dataType})");
            
            // Check if column is nullable
            bool isNullable = true; // Default to nullable
            
            if (columnDef.Constraints != null)
            {
                foreach (var constraint in columnDef.Constraints)
                {
                    if (constraint is NullableConstraintDefinition nullableConstraint)
                    {
                        isNullable = nullableConstraint.Nullable;
                    }
                }
            }
            
            // Create column node
            var columnNode = new ColumnNode
            {
                Id = CreateNodeId("COLUMN", $"{tableNode.Name}.{columnName}"),
                Name = columnName,
                ObjectName = columnName,
                TableOwner = tableNode.Name,
                SchemaName = tableNode.SchemaName,
                DataType = dataType,
                IsNullable = isNullable
            };
            
            Graph.AddNode(columnNode);
            
            // Add column to table's columns collection
            tableNode.Columns.Add(columnNode.Id);
            
            // Check for computed column
            if (columnDef.ComputedColumnExpression != null)
            {
                columnNode.IsComputed = true;
                
                // Create expression node for computed column
                var expressionNode = new ExpressionNode
                {
                    Id = CreateNodeId("EXPR", $"{tableNode.Name}.{columnName}"),
                    Name = $"{columnName}_Expr",
                    ObjectName = GetSqlText(columnDef.ComputedColumnExpression),
                    ExpressionType = "ComputedColumn",
                    Expression = GetSqlText(columnDef.ComputedColumnExpression),
                    TableOwner = tableNode.Name,
                    ResultType = dataType
                };
                
                Graph.AddNode(expressionNode);
                
                // Create edge from expression to column
                var edge = CreateDirectEdge(
                    expressionNode.Id,
                    columnNode.Id,
                    "compute",
                    GetSqlText(columnDef.ComputedColumnExpression)
                );
                
                Graph.AddEdge(edge);
                
                // Extract column references from the computed expression
                if (columnDef.ComputedColumnExpression is ScalarExpression scalarExpr)
                {
                    var columnRefs = new List<ColumnReferenceExpression>();
                    ExtractColumnReferences(scalarExpr, columnRefs);
                    
                    // Create edges from referenced columns to expression
                    foreach (var colRef in columnRefs)
                    {
                        string refColName = colRef.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value;
                        
                        if (!string.IsNullOrEmpty(refColName))
                        {
                            // Computed column can only reference columns in the same table
                            var sourceColumn = Graph.GetColumnNode(tableNode.Name, refColName);
                            
                            if (sourceColumn != null)
                            {
                                var refEdge = CreateIndirectEdge(
                                    sourceColumn.Id,
                                    expressionNode.Id,
                                    "reference",
                                    $"{sourceColumn.Name} referenced in computed column {columnName}"
                                );
                                
                                Graph.AddEdge(refEdge);
                            }
                        }
                    }
                }
            }
        }
        
        /// <summary>
        /// Process a table constraint
        /// </summary>
        private void ProcessTableConstraint(ConstraintDefinition constraint, TableNode tableNode)
        {
            if (constraint is UniqueConstraintDefinition uniqueConstraint)
            {
                string constraintType = uniqueConstraint.IsPrimaryKey ? "PRIMARY KEY" : "UNIQUE";
                LogDebug($"Processing {constraintType} constraint on {tableNode.Name}");
                
                // Track keys in table metadata
                if (uniqueConstraint.Columns != null)
                {
                    foreach (var column in uniqueConstraint.Columns)
                    {
                        if (column?.Column?.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value != null)
                        {
                            string columnName = column.Column.MultiPartIdentifier.Identifiers.Last().Value;
                            
                            // Find column node
                            var columnNode = Graph.GetColumnNode(tableNode.Name, columnName);
                            
                            if (columnNode != null)
                            {
                                // Add key info to column metadata
                                columnNode.Metadata[constraintType] = true;
                            }
                        }
                    }
                }
            }
            else if (constraint is ForeignKeyConstraintDefinition fkConstraint)
            {
                LogDebug($"Processing FOREIGN KEY constraint on {tableNode.Name}");
                
                if (fkConstraint.Columns != null && 
                    fkConstraint.ReferenceTableObjectName?.Identifiers?.LastOrDefault()?.Value != null)
                {
                    string refTableName = fkConstraint.ReferenceTableObjectName.Identifiers.Last().Value;
                    
                    // Process each column in the foreign key
                    for (int i = 0; i < fkConstraint.Columns.Count; i++)
                    {
                        var column = fkConstraint.Columns[i];
                        
                        if (column?.Column?.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value != null)
                        {
                            string columnName = column.Column.MultiPartIdentifier.Identifiers.Last().Value;
                            
                            // Find column node
                            var columnNode = Graph.GetColumnNode(tableNode.Name, columnName);
                            
                            if (columnNode != null && i < fkConstraint.ReferencedTableColumns.Count)
                            {
                                var refColumn = fkConstraint.ReferencedTableColumns[i];
                                
                                if (refColumn?.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value != null)
                                {
                                    string refColumnName = refColumn.MultiPartIdentifier.Identifiers.Last().Value;
                                    
                                    // Get or create referenced column
                                    var refColumnNode = Context.GetOrCreateColumnNode(refTableName, refColumnName);
                                    
                                    if (refColumnNode != null)
                                    {
                                        // Create edges for the foreign key relationship
                                        var fkEdge = CreateDirectEdge(
                                            refColumnNode.Id,
                                            columnNode.Id,
                                            "foreignKey",
                                            $"FK from {tableNode.Name}.{columnName} to {refTableName}.{refColumnName}"
                                        );
                                        
                                        Graph.AddEdge(fkEdge);
                                        
                                        // Add FK info to column metadata
                                        columnNode.Metadata["ForeignKey"] = true;
                                        columnNode.Metadata["ReferencesTable"] = refTableName;
                                        columnNode.Metadata["ReferencesColumn"] = refColumnName;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        /// <summary>
        /// Process ALTER TABLE statement
        /// </summary>
        public override void ExplicitVisit(AlterTableStatement node)
        {
            if (node?.SchemaObjectName?.Identifiers == null) return;
            
            string tableName = string.Join(".", node.SchemaObjectName.Identifiers.Select(i => i.Value));
            LogDebug($"Processing ALTER TABLE {tableName}");
            
            // Find existing table node
            var tableNode = LineageContext.GetTable(tableName);
            
            if (tableNode == null)
            {
                // Create table node if it doesn't exist
                tableNode = Context.GetOrCreateTableNode(tableName);
            }
            
            // Process ALTER TABLE actions
            if (node.AlterTableActions != null)
            {
                foreach (var action in node.AlterTableActions)
                {
                    ProcessAlterTableAction(action, tableNode);
                }
            }
        }
        
        /// <summary>
        /// Process an ALTER TABLE action
        /// </summary>
        private void ProcessAlterTableAction(AlterTableAction action, TableNode tableNode)
        {
            // Add column
            if (action is AddTableElementAction addAction)
            {
                if (addAction.Definition is ColumnDefinition columnDef)
                {
                    ProcessColumnDefinition(columnDef, tableNode);
                }
                else if (addAction.Definition is ConstraintDefinition constraintDef)
                {
                    ProcessTableConstraint(constraintDef, tableNode);
                }
            }
            // Alter column
            else if (action is AlterColumnAction alterColumnAction && 
                     alterColumnAction.Column?.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value != null)
            {
                string columnName = alterColumnAction.Column.MultiPartIdentifier.Identifiers.Last().Value;
                LogDebug($"Processing ALTER COLUMN {columnName}");
                
                // Find existing column
                var columnNode = Graph.GetColumnNode(tableNode.Name, columnName);
                
                if (columnNode != null && alterColumnAction.DataType != null)
                {
                    // Update data type
                    string dataType = GetSqlText(alterColumnAction.DataType);
                    
                    // Add column alter info to metadata
                    columnNode.Metadata["AlteredDataType"] = dataType;
                    columnNode.Metadata["OriginalDataType"] = columnNode.DataType;
                    
                    // Clone the column with the new data type
                    var newColumn = new ColumnNode
                    {
                        Id = columnNode.Id,
                        Name = columnNode.Name,
                        ObjectName = columnNode.ObjectName,
                        TableOwner = columnNode.TableOwner,
                        SchemaName = columnNode.SchemaName,
                        DataType = dataType,
                        IsNullable = alterColumnAction.Nullable ?? columnNode.IsNullable,
                        IsComputed = columnNode.IsComputed
                    };
                    
                    // Add metadata from original column
                    foreach (var (key, value) in columnNode.Metadata)
                    {
                        newColumn.Metadata[key] = value;
                    }
                    
                    // Update the column in the graph
                    Graph.AddNode(newColumn);
                }
            }
            // Drop column
            else if (action is DropTableElementAction dropAction && 
                     dropAction.Name?.Identifiers?.LastOrDefault()?.Value != null &&
                     dropAction.TableElementType == TableElementType.Column)
            {
                string columnName = dropAction.Name.Identifiers.Last().Value;
                LogDebug($"Processing DROP COLUMN {columnName}");
                
                // Find the column
                var columnNode = Graph.GetColumnNode(tableNode.Name, columnName);
                
                if (columnNode != null)
                {
                    // Mark column as dropped in metadata
                    columnNode.Metadata["Dropped"] = true;
                    columnNode.Metadata["DroppedAt"] = DateTime.UtcNow;
                }
            }
        }
        
        /// <summary>
        /// Extracts column references from an expression (helper method)
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
        }
    }
}
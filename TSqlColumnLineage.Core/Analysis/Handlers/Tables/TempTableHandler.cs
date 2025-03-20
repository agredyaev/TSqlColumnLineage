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
    /// Handler for temporary tables and table variables
    /// </summary>
    public class TempTableHandler : AbstractQueryHandler
    {
        /// <summary>
        /// Creates a new temporary table handler
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="logger">Logger (optional)</param>
        public TempTableHandler(VisitorContext context, ILogger logger = null)
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
            return fragment is CreateTableStatement createTable && IsTemporaryTable(createTable) ||
                   fragment is DeclareTableVariableStatement ||
                   fragment is SelectStatement select && select.Into != null;
        }
        
        /// <summary>
        /// Processes the SQL fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <param name="context">Visitor context</param>
        /// <returns>True if the fragment was fully processed; otherwise, false</returns>
        public override bool Handle(TSqlFragment fragment, VisitorContext context)
        {
            if (fragment is CreateTableStatement createTable && IsTemporaryTable(createTable))
            {
                ProcessCreateTempTable(createTable);
                return true;
            }
            else if (fragment is DeclareTableVariableStatement declareTable)
            {
                ProcessDeclareTableVariable(declareTable);
                return true;
            }
            else if (fragment is SelectStatement select && select.Into != null)
            {
                ProcessSelectIntoTempTable(select);
                return true;
            }
            
            return false;
        }
        
        /// <summary>
        /// Checks if a CREATE TABLE statement is for a temporary table
        /// </summary>
        private bool IsTemporaryTable(CreateTableStatement createTable)
        {
            if (createTable?.SchemaObjectName?.Identifiers == null || 
                createTable.SchemaObjectName.Identifiers.Count == 0)
                return false;
                
            string tableName = createTable.SchemaObjectName.Identifiers.Last().Value;
            return tableName.StartsWith("#");
        }
        
        /// <summary>
        /// Processes a CREATE TABLE statement for a temporary table
        /// </summary>
        private void ProcessCreateTempTable(CreateTableStatement createTable)
        {
            if (createTable?.SchemaObjectName?.Identifiers == null || 
                createTable.SchemaObjectName.Identifiers.Count == 0 ||
                createTable.Definition?.ColumnDefinitions == null)
                return;
                
            string tableName = createTable.SchemaObjectName.Identifiers.Last().Value;
            LogDebug($"Processing CREATE TEMP TABLE {tableName}");
            
            // Create table node
            var tableNode = new TableNode
            {
                Id = CreateNodeId("TABLE", tableName),
                Name = tableName,
                ObjectName = tableName,
                SchemaName = string.Empty,
                TableType = "TempTable",
                Definition = GetSqlText(createTable)
            };
            
            Graph.AddNode(tableNode);
            LineageContext.AddTable(tableNode);
            
            // Register in temp tables collection
            LineageContext.TempTables[tableName] = tableNode;
            
            // Process column definitions
            foreach (var columnDef in createTable.Definition.ColumnDefinitions)
            {
                if (columnDef?.ColumnIdentifier?.Value == null)
                    continue;
                    
                string columnName = columnDef.ColumnIdentifier.Value;
                string dataType = columnDef.DataType != null ? GetSqlText(columnDef.DataType) : "unknown";
                
                // Check if column is nullable
                bool isNullable = true; // Default to nullable
                if (columnDef.Constraints != null)
                {
                    foreach (var constraint in columnDef.Constraints)
                    {
                        if (constraint is NullableConstraintDefinition nullableConstraint)
                        {
                            isNullable = nullableConstraint.Nullable;
                            break;
                        }
                    }
                }
                
                // Create column node
                var columnNode = new ColumnNode
                {
                    Id = CreateNodeId("COLUMN", $"{tableName}.{columnName}"),
                    Name = columnName,
                    ObjectName = columnName,
                    TableOwner = tableName,
                    SchemaName = string.Empty,
                    DataType = dataType,
                    IsNullable = isNullable
                };
                
                Graph.AddNode(columnNode);
                tableNode.Columns.Add(columnNode.Id);
                
                // Process default value for lineage tracking
                if (columnDef.DefaultConstraint?.Expression != null)
                {
                    ProcessColumnDefaultValue(columnDef.DefaultConstraint.Expression, columnNode);
                }
                
                // Process computed column
                if (columnDef.ComputedColumnExpression != null)
                {
                    columnNode.IsComputed = true;
                    ProcessComputedColumn(columnDef.ComputedColumnExpression, columnNode);
                }
            }
        }
        
        /// <summary>
        /// Processes a DECLARE TABLE statement for a table variable
        /// </summary>
        private void ProcessDeclareTableVariable(DeclareTableVariableStatement declareTable)
        {
            // Extract the table variable name
            string tableName = ExtractTableVariableName(declareTable);
            if (string.IsNullOrEmpty(tableName))
                return;
                
            LogDebug($"Processing DECLARE TABLE VARIABLE {tableName}");
            
            // Check that we have column definitions
            if (declareTable.Body?.Definition?.ColumnDefinitions == null)
                return;
                
            // Create table node
            var tableNode = new TableNode
            {
                Id = CreateNodeId("TABLE", tableName),
                Name = tableName,
                ObjectName = tableName,
                SchemaName = string.Empty,
                TableType = "TableVariable",
                Definition = GetSqlText(declareTable)
            };
            
            Graph.AddNode(tableNode);
            LineageContext.AddTable(tableNode);
            
            // Register in table variables collection
            LineageContext.TableVariables[tableName] = tableNode;
            
            // Process column definitions
            foreach (var columnDef in declareTable.Body.Definition.ColumnDefinitions)
            {
                if (columnDef?.ColumnIdentifier?.Value == null)
                    continue;
                    
                string columnName = columnDef.ColumnIdentifier.Value;
                string dataType = columnDef.DataType != null ? GetSqlText(columnDef.DataType) : "unknown";
                
                // Check if column is nullable
                bool isNullable = true; // Default to nullable
                if (columnDef.Constraints != null)
                {
                    foreach (var constraint in columnDef.Constraints)
                    {
                        if (constraint is NullableConstraintDefinition nullableConstraint)
                        {
                            isNullable = nullableConstraint.Nullable;
                            break;
                        }
                    }
                }
                
                // Create column node
                var columnNode = new ColumnNode
                {
                    Id = CreateNodeId("COLUMN", $"{tableName}.{columnName}"),
                    Name = columnName,
                    ObjectName = columnName,
                    TableOwner = tableName,
                    SchemaName = string.Empty,
                    DataType = dataType,
                    IsNullable = isNullable
                };
                
                Graph.AddNode(columnNode);
                tableNode.Columns.Add(columnNode.Id);
                
                // Process default value for lineage tracking
                if (columnDef.DefaultConstraint?.Expression != null)
                {
                    ProcessColumnDefaultValue(columnDef.DefaultConstraint.Expression, columnNode);
                }
                
                // Process computed column
                if (columnDef.ComputedColumnExpression != null)
                {
                    columnNode.IsComputed = true;
                    ProcessComputedColumn(columnDef.ComputedColumnExpression, columnNode);
                }
            }
        }
        
        /// <summary>
        /// Processes a SELECT INTO statement that creates a temporary table
        /// </summary>
        public void ProcessSelectIntoTempTable(SelectStatement select)
        {
            if (select?.Into == null || select.QueryExpression == null)
                return;
                
            // Extract the target table name
            string tableName = ExtractIntoTableName(select.Into);
            if (string.IsNullOrEmpty(tableName))
                return;
                
            bool isTempTable = tableName.StartsWith("#");
            
            LogDebug($"Processing SELECT INTO {tableName}");
            
            // Create table node
            var tableNode = new TableNode
            {
                Id = CreateNodeId("TABLE", tableName),
                Name = tableName,
                ObjectName = tableName,
                SchemaName = string.Empty,
                TableType = isTempTable ? "TempTable" : "Table",
                Definition = GetSqlText(select)
            };
            
            Graph.AddNode(tableNode);
            LineageContext.AddTable(tableNode);
            
            // Register in temp tables collection if it's a temp table
            if (isTempTable)
            {
                LineageContext.TempTables[tableName] = tableNode;
            }
            
            // Set context for processing the SELECT
            Context.State["SelectIntoTable"] = tableName;
            
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
                    select.QueryExpression.Accept(visitor);
                }
                
                // Try to infer column names from the SELECT
                var columnNames = InferColumnsFromQuery(select.QueryExpression);
                
                // Create column nodes for the inferred columns
                foreach (var columnName in columnNames)
                {
                    var columnNode = new ColumnNode
                    {
                        Id = CreateNodeId("COLUMN", $"{tableName}.{columnName}"),
                        Name = columnName,
                        ObjectName = columnName,
                        TableOwner = tableName,
                        SchemaName = string.Empty,
                        DataType = "unknown" // We don't know types from SELECT INTO
                    };
                    
                    Graph.AddNode(columnNode);
                    tableNode.Columns.Add(columnNode.Id);
                }
            }
            finally
            {
                // Clear context
                Context.State.Remove("SelectIntoTable");
            }
        }
        
        /// <summary>
        /// Processes a default value expression for column lineage
        /// </summary>
        private void ProcessColumnDefaultValue(ScalarExpression defaultExpr, ColumnNode columnNode)
        {
            if (defaultExpr == null || columnNode == null)
                return;
                
            // Create expression node for default value
            var expressionNode = new ExpressionNode
            {
                Id = CreateNodeId("EXPR", $"{columnNode.TableOwner}.{columnNode.Name}_Default"),
                Name = $"{columnNode.Name}_Default",
                ObjectName = GetSqlText(defaultExpr),
                ExpressionType = "DefaultValue",
                Expression = GetSqlText(defaultExpr),
                TableOwner = columnNode.TableOwner,
                ResultType = columnNode.DataType
            };
            
            Graph.AddNode(expressionNode);
            
            // Create edge from expression to column
            var edge = CreateDirectEdge(
                expressionNode.Id,
                columnNode.Id,
                "default",
                $"Default value for {columnNode.TableOwner}.{columnNode.Name}"
            );
            
            Graph.AddEdge(edge);
            
            // Extract column references from default value
            var columnRefs = new List<ColumnReferenceExpression>();
            ExtractColumnReferences(defaultExpr, columnRefs);
            
            foreach (var colRef in columnRefs)
            {
                string refColName = colRef.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value;
                string refTableName = null;
                
                if (colRef.MultiPartIdentifier?.Identifiers.Count > 1)
                {
                    refTableName = colRef.MultiPartIdentifier.Identifiers[0].Value;
                    
                    // Resolve alias if needed
                    if (LineageContext.TableAliases.TryGetValue(refTableName, out var resolvedTable))
                    {
                        refTableName = resolvedTable;
                    }
                }
                
                // Find the referenced column
                ColumnNode refColumn = null;
                
                if (!string.IsNullOrEmpty(refTableName))
                {
                    refColumn = Graph.GetColumnNode(refTableName, refColName);
                }
                else
                {
                    // Try to find in any table
                    foreach (var table in LineageContext.Tables.Values)
                    {
                        var col = Graph.GetColumnNode(table.Name, refColName);
                        if (col != null)
                        {
                            refColumn = col;
                            break;
                        }
                    }
                }
                
                if (refColumn != null)
                {
                    // Create edge from referenced column to expression
                    var refEdge = CreateIndirectEdge(
                        refColumn.Id,
                        expressionNode.Id,
                        "reference",
                        $"Referenced in default value: {refColumn.TableOwner}.{refColumn.Name}"
                    );
                    
                    Graph.AddEdge(refEdge);
                }
            }
        }
        
        /// <summary>
        /// Processes a computed column expression for lineage
        /// </summary>
        private void ProcessComputedColumn(ScalarExpression computedExpr, ColumnNode columnNode)
        {
            if (computedExpr == null || columnNode == null)
                return;
                
            // Create expression node for computed column
            var expressionNode = new ExpressionNode
            {
                Id = CreateNodeId("EXPR", $"{columnNode.TableOwner}.{columnNode.Name}_Computed"),
                Name = $"{columnNode.Name}_Computed",
                ObjectName = GetSqlText(computedExpr),
                ExpressionType = "ComputedColumn",
                Expression = GetSqlText(computedExpr),
                TableOwner = columnNode.TableOwner,
                ResultType = columnNode.DataType
            };
            
            Graph.AddNode(expressionNode);
            
            // Create edge from expression to column
            var edge = CreateDirectEdge(
                expressionNode.Id,
                columnNode.Id,
                "compute",
                $"Computed column definition for {columnNode.TableOwner}.{columnNode.Name}"
            );
            
            Graph.AddEdge(edge);
            
            // Extract column references from computed expression
            var columnRefs = new List<ColumnReferenceExpression>();
            ExtractColumnReferences(computedExpr, columnRefs);
            
            foreach (var colRef in columnRefs)
            {
                string refColName = colRef.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value;
                
                // Computed columns can only reference columns in the same table
                var refColumn = Graph.GetColumnNode(columnNode.TableOwner, refColName);
                
                if (refColumn != null)
                {
                    // Create edge from referenced column to expression
                    var refEdge = CreateIndirectEdge(
                        refColumn.Id,
                        expressionNode.Id,
                        "reference",
                        $"Referenced in computed column: {refColumn.TableOwner}.{refColumn.Name}"
                    );
                    
                    Graph.AddEdge(refEdge);
                }
            }
        }
        
        /// <summary>
        /// Extracts the table variable name from a DECLARE statement
        /// </summary>
        private string ExtractTableVariableName(DeclareTableVariableStatement declareTable)
        {
            // Try different approaches to extract the name
            
            // First, try to get the name from VariableName property if it exists
            try
            {
                var nameProperty = declareTable.GetType().GetProperty("VariableName");
                if (nameProperty != null)
                {
                    var nameValue = nameProperty.GetValue(declareTable);
                    
                    if (nameValue != null)
                    {
                        var valueProperty = nameValue.GetType().GetProperty("Value");
                        if (valueProperty != null)
                        {
                            var value = valueProperty.GetValue(nameValue)?.ToString();
                            if (!string.IsNullOrEmpty(value))
                            {
                                return value.StartsWith("@") ? value : $"@{value}";
                            }
                        }
                    }
                }
            }
            catch
            {
                // Ignore reflection errors
            }
            
            // Next, try to find the name in any Identifier properties
            try
            {
                var properties = declareTable.GetType().GetProperties();
                
                foreach (var prop in properties)
                {
                    if (prop.PropertyType == typeof(Identifier) || 
                        prop.Name.EndsWith("Identifier"))
                    {
                        var identifier = prop.GetValue(declareTable) as Identifier;
                        if (identifier?.Value != null)
                        {
                            return identifier.Value.StartsWith("@") ? identifier.Value : $"@{identifier.Value}";
                        }
                    }
                }
            }
            catch
            {
                // Ignore reflection errors
            }
            
            // Try to extract from script tokens
            try
            {
                var tokens = declareTable.ScriptTokenStream;
                
                for (int i = 0; i < tokens.Count - 1; i++)
                {
                    if (tokens[i].Text == "@" && i + 1 < tokens.Count)
                    {
                        return $"@{tokens[i + 1].Text}";
                    }
                }
            }
            catch
            {
                // Ignore errors
            }
            
            // Give up and return a default name
            return $"@TableVar_{Guid.NewGuid().ToString("N").Substring(0, 8)}";
        }
        
        /// <summary>
        /// Extracts the table name from the INTO clause
        /// </summary>
        private string ExtractIntoTableName(TSqlFragment intoClause)
        {
            if (intoClause == null)
                return string.Empty;
                
            // Try to get SchemaObjectName property
            try
            {
                var nameProperty = intoClause.GetType().GetProperty("SchemaObjectName") ??
                                  intoClause.GetType().GetProperty("Name");
                                  
                if (nameProperty != null)
                {
                    var nameObj = nameProperty.GetValue(intoClause) as SchemaObjectName;
                    
                    if (nameObj?.Identifiers != null && nameObj.Identifiers.Count > 0)
                    {
                        return string.Join(".", nameObj.Identifiers.Select(i => i.Value));
                    }
                }
            }
            catch
            {
                // Ignore reflection errors
            }
            
            // Try to get from script tokens
            try
            {
                var tokens = intoClause.ScriptTokenStream;
                bool intoFound = false;
                
                for (int i = 0; i < tokens.Count; i++)
                {
                    if (tokens[i].Text.Equals("INTO", StringComparison.OrdinalIgnoreCase))
                    {
                        intoFound = true;
                    }
                    else if (intoFound && !string.IsNullOrWhiteSpace(tokens[i].Text) && 
                             tokens[i].Text != "(" && tokens[i].Text != ")")
                    {
                        return tokens[i].Text;
                    }
                }
            }
            catch
            {
                // Ignore errors
            }
            
            // Return original text as last resort
            return intoClause.ToString();
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
    }
}
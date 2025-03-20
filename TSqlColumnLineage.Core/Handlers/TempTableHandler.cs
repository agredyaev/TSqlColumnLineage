using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Visitors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace TSqlColumnLineage.Core.Handlers
{
    /// <summary>
    /// Handler for temporary tables (#temp) and table variables (@table)
    /// </summary>
    public class TempTableHandler : AbstractQueryHandler
    {
        public TempTableHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger logger)
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
            if (fragment is CreateTableStatement createTableStmt)
            {
                ProcessCreateTempTable(createTableStmt);
                return true;
            }
            
            if (fragment is DeclareTableVariableStatement declareTableStmt)
            {
                ProcessDeclareTableVariable(declareTableStmt);
                return true;
            }
            
            if (fragment is SelectStatement selectStmt && selectStmt.Into != null)
            {
                ProcessSelectIntoTempTable(selectStmt);
                return true;
            }
            
            return false;
        }

        /// <summary>
        /// Creates a temporary table from a CREATE TABLE definition
        /// </summary>
        public void ProcessCreateTempTable(CreateTableStatement node)
        {
            if (node?.SchemaObjectName == null || node.Definition?.ColumnDefinitions == null)
                return;

            string tableName = string.Join(".", node.SchemaObjectName.Identifiers.Select(i => i.Value));
            LogDebug($"Processing CREATE TEMP TABLE: {tableName}");

            // Check if it's a temporary table (starts with #)
            bool isTempTable = tableName.StartsWith("#");
            if (!isTempTable)
                return;

            // Create node for the temporary table
            var tableNode = new TableNode
            {
                Id = CreateNodeId("TABLE", tableName),
                Name = tableName,
                ObjectName = tableName,
                TableType = "TempTable",
                Definition = GetSqlText(node)
            };

            Graph.AddNode(tableNode);
            // Store in regular Tables collection
            Context.AddTable(tableNode);
            // Also track it as temp table in metadata
            Context.Metadata[$"TempTable:{tableName.ToLowerInvariant()}"] = tableNode;

            // Add columns of the temporary table
            foreach (var columnDef in node.Definition.ColumnDefinitions)
            {
                if (columnDef?.ColumnIdentifier?.Value == null)
                    continue;

                string columnName = columnDef.ColumnIdentifier.Value;
                var dataType = columnDef.DataType != null
                    ? GetSqlText(columnDef.DataType)
                    : "unknown";

                var columnNode = new ColumnNode
                {
                    Id = CreateNodeId("COLUMN", $"{tableName}.{columnName}"),
                    Name = columnName,
                    ObjectName = columnName,
                    TableOwner = tableName,
                    DataType = dataType,
                    IsNullable = !columnDef.Constraints.Any(c => c is NullableConstraintDefinition ncd && !ncd.Nullable)
                };

                Graph.AddNode(columnNode);
                tableNode.Columns.Add(columnNode.Id);
            }
        }

        /// <summary>
        /// Creates a table variable from a DECLARE statement
        /// </summary>
        public void ProcessDeclareTableVariable(DeclareTableVariableStatement node)
        {
            // Adjust for version compatibility
            // Instead of checking for DeclarationComment, use direct identification from node
            if (node?.Body?.Definition?.ColumnDefinitions == null)
                return;

            // Extract variable name using a version-agnostic approach
            string tableName = ExtractTableVariableName(node);
            LogDebug($"Processing DECLARE TABLE VARIABLE: {tableName}");

            // Create node for the table variable
            var tableNode = new TableNode
            {
                Id = CreateNodeId("TABLE", tableName),
                Name = tableName,
                ObjectName = tableName,
                TableType = "TableVariable",
                Definition = GetSqlText(node)
            };

            Graph.AddNode(tableNode);
            // Store in regular Tables collection
            Context.AddTable(tableNode);
            // Also track it as table variable in metadata
            Context.Metadata[$"TableVar:{tableName.ToLowerInvariant()}"] = tableNode;

            // Add columns of the table variable
            foreach (var columnDef in node.Body.Definition.ColumnDefinitions)
            {
                if (columnDef?.ColumnIdentifier?.Value == null)
                    continue;

                string columnName = columnDef.ColumnIdentifier.Value;
                var dataType = columnDef.DataType != null
                    ? GetSqlText(columnDef.DataType)
                    : "unknown";

                var columnNode = new ColumnNode
                {
                    Id = CreateNodeId("COLUMN", $"{tableName}.{columnName}"),
                    Name = columnName,
                    ObjectName = columnName,
                    TableOwner = tableName,
                    DataType = dataType,
                    IsNullable = !columnDef.Constraints.Any(c => c is NullableConstraintDefinition ncd && !ncd.Nullable)
                };

                Graph.AddNode(columnNode);
                tableNode.Columns.Add(columnNode.Id);
            }
        }

        /// <summary>
        /// Processes SELECT INTO #temp to create a temporary table
        /// </summary>
        public void ProcessSelectIntoTempTable(SelectStatement node)
        {
            if (node?.Into == null)
                return;

            // Extract table name using a version-agnostic approach
            string tableName;
            
            // First try to see if the Into property is already a SchemaObjectName
            if (node.Into is SchemaObjectName schemaObject)
            {
                tableName = ExtractNameFromSchemaObject(schemaObject);
            }
            else
            {
                // Try to extract SchemaObject property through reflection
                try
                {
                    var nameProperty = node.Into.GetType().GetProperty("SchemaObject");
                    if (nameProperty != null)
                    {
                        var obj = nameProperty.GetValue(node.Into) as SchemaObjectName;
                        if (obj != null)
                        {
                            tableName = ExtractNameFromSchemaObject(obj);
                        }
                        else
                        {
                            tableName = node.Into.ToString() ?? "Unknown";
                        }
                    }
                    else
                    {
                        tableName = node.Into.ToString() ?? "Unknown";
                    }
                }
                catch (Exception ex)
                {
                    LogDebug($"Error extracting table name: {ex.Message}");
                    tableName = node.Into.ToString() ?? "Unknown";
                }
            }

            // Check if it's a temporary table
            bool isTempTable = tableName.StartsWith("#");
            if (!isTempTable)
                return;

            LogDebug($"Processing SELECT INTO #TEMP: {tableName}");

            // Create node for the temporary table
            var tableNode = new TableNode
            {
                Id = CreateNodeId("TABLE", tableName),
                Name = tableName,
                ObjectName = tableName,
                TableType = "TempTable",
                Definition = GetSqlText(node)
            };

            Graph.AddNode(tableNode);
            // Store in regular Tables collection
            Context.AddTable(tableNode);
            // Also track it as temp table in metadata
            Context.Metadata[$"TempTable:{tableName.ToLowerInvariant()}"] = tableNode;

            // Columns will be added when processing the SELECT list
            // But we need to store information that we are currently processing SELECT INTO
            Context.Metadata["currentSelectInto"] = tableName;

            // After processing the SELECT list, this flag needs to be removed
        }

        /// <summary>
        /// Extracts table name from a table variable declaration in a version-agnostic way
        /// </summary>
        private string ExtractTableVariableName(DeclareTableVariableStatement node)
        {
            try
            {
                // Try different approaches to extract the table variable name
                
                // Method 1: Try using the Variables property if it exists
                var varsProperty = node.GetType().GetProperty("Variables") ??
                                   node.GetType().GetProperty("Declarations");
                if (varsProperty != null)
                {
                    var vars = varsProperty.GetValue(node) as IEnumerable<object>;
                    if (vars != null && vars.Any())
                    {
                        var firstVar = vars.First();
                        var nameProperty = firstVar.GetType().GetProperty("Name") ??
                                          firstVar.GetType().GetProperty("VariableName");
                        if (nameProperty != null)
                        {
                            var nameValue = nameProperty.GetValue(firstVar)?.ToString();
                            if (!string.IsNullOrEmpty(nameValue))
                            {
                                return $"@{nameValue}";
                            }
                        }
                    }
                }
                
                // Method 2: Try using the Name property directly
                var nameProperty2 = node.GetType().GetProperty("Name");
                if (nameProperty2 != null)
                {
                    var nameValue = nameProperty2.GetValue(node)?.ToString();
                    if (!string.IsNullOrEmpty(nameValue))
                    {
                        return nameValue.StartsWith("@") ? nameValue : $"@{nameValue}";
                    }
                }
                
                // Method 3: Try to extract from Identifiers if available
                var idsProperty = node.GetType().GetProperty("Identifiers");
                if (idsProperty != null)
                {
                    var ids = idsProperty.GetValue(node) as IEnumerable<Identifier>;
                    if (ids != null && ids.Any())
                    {
                        var firstId = ids.First();
                        return $"@{firstId.Value}";
                    }
                }
                
                // Fallback with token extraction if all else fails
                var tokens = node.ScriptTokenStream;
                if (tokens != null)
                {
                    for (int i = 0; i < tokens.Count; i++)
                    {
                        if (tokens[i].Text.Equals("@", StringComparison.Ordinal) && i + 1 < tokens.Count)
                        {
                            return $"@{tokens[i + 1].Text}";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogDebug($"Error extracting table variable name: {ex.Message}");
            }
            
            // Ultimate fallback
            return "@TableVar_" + Guid.NewGuid().ToString("N").Substring(0, 8);
        }
        
        /// <summary>
        /// Extracts a name from a schema object using Identifiers
        /// </summary>
        private string ExtractNameFromSchemaObject(SchemaObjectName schemaObject)
        {
            if (schemaObject?.Identifiers == null || !schemaObject.Identifiers.Any())
                return "Unknown";
                
            return string.Join(".", schemaObject.Identifiers.Select(i => i.Value));
        }
    }
}

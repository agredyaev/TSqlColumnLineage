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
    /// Handler for stored procedure statements and their parameter interactions
    /// </summary>
    public class StoredProcedureHandler : AbstractQueryHandler
    {
        // Track procedure parameters and their mappings
        private Dictionary<string, ColumnNode> _procedureParameters = new();
        // Track variable assignments to follow data flow
        private Dictionary<string, ColumnNode> _variableAssignments = new();

        public StoredProcedureHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger logger)
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
            if (fragment is CreateProcedureStatement procDef)
            {
                ProcessStoredProcedureStatement(procDef);
                return true;
            }
            
            if (fragment is ExecuteStatement execStmt)
            {
                ProcessExecuteStatement(execStmt);
                return true;
            }
            
            if (fragment is DeclareVariableStatement declareStmt)
            {
                ProcessVariableDeclaration(declareStmt);
                return true;
            }
            
            if (fragment is SetVariableStatement setStmt)
            {
                ProcessSetVariableStatement(setStmt);
                return true;
            }
            
            if (fragment is SelectStatement selectStmt && selectStmt.Into != null)
            {
                ProcessSelectIntoStatement(selectStmt);
                return true;
            }
            
            return false;
        }

        /// <summary>
        /// Process a stored procedure definition
        /// </summary>
        public void ProcessStoredProcedureStatement(CreateProcedureStatement node)
        {
            if (node == null)
                return;

            LogDebug($"Processing CREATE PROCEDURE at Line {node.StartLine}");

            // Get the procedure name
            string procedureName = GetProcedureName(node);
            LogDebug($"Procedure name: {procedureName}");

            // Create a TableNode to represent the procedure
            var procedureNode = new TableNode
            {
                Id = CreateNodeId("PROCEDURE", procedureName),
                Name = procedureName,
                ObjectName = procedureName,
                TableType = "StoredProcedure",
                Definition = GetSqlText(node)
            };

            Graph.AddNode(procedureNode);
            Context.Metadata["CurrentProcedure"] = procedureName;

            // Process procedure parameters
            ProcessParameters(node.Parameters, procedureNode);

            // Process procedure body
            if (node.StatementList?.Statements != null)
            {
                // Mark the context as being inside a stored procedure
                Context.Metadata["InStoredProcedure"] = true;
                
                try
                {
                    // Process each statement in the procedure
                    foreach (var statement in node.StatementList.Statements)
                    {
                        ProcessStatement(statement, procedureName);
                    }
                }
                finally
                {
                    // Clear the stored procedure context
                    Context.Metadata.Remove("InStoredProcedure");
                    Context.Metadata.Remove("CurrentProcedure");
                }
            }
        }

        /// <summary>
        /// Process a procedure execution statement (EXEC/EXECUTE)
        /// </summary>
        public void ProcessExecuteStatement(ExecuteStatement node)
        {
            if (node == null)
                return;

            LogDebug($"Processing EXECUTE at Line {node.StartLine}");

            // Extract procedure name being called
            string procedureName = GetExecutedProcedureName(node);
            if (string.IsNullOrEmpty(procedureName))
                return;

            LogDebug($"Executing procedure: {procedureName}");

            // Process parameter mappings to track lineage
            if (node.ExecuteSpecification?.ExecutableEntity is ExecutableProcedureReference procRef &&
                procRef.Parameters != null && procRef.Parameters.Count > 0)
            {
                for (int i = 0; i < procRef.Parameters.Count; i++)
                {
                    var param = procRef.Parameters[i];
                    string paramName = $"@Param{i + 1}";
                    
                    // Try to get explicit parameter name if available using reflection
                    try
                    {
                        // First try Variable property
                        var varProp = param.GetType().GetProperty("Variable");
                        if (varProp != null)
                        {
                            var varObj = varProp.GetValue(param);
                            if (varObj != null)
                            {
                                var nameProp = varObj.GetType().GetProperty("Name");
                                if (nameProp != null)
                                {
                                    var nameVal = nameProp.GetValue(varObj)?.ToString();
                                    if (!string.IsNullOrEmpty(nameVal))
                                    {
                                        paramName = nameVal;
                                    }
                                }
                            }
                        }
                        
                        // If still using default name, try ParameterName property
                        if (paramName == $"@Param{i + 1}")
                        {
                            var nameProp = param.GetType().GetProperty("ParameterName");
                            if (nameProp != null)
                            {
                                var nameVal = nameProp.GetValue(param)?.ToString();
                                if (!string.IsNullOrEmpty(nameVal))
                                {
                                    paramName = nameVal;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        LogDebug($"Error getting parameter name: {ex.Message}");
                    }

                    // Process the parameter value for lineage
                    if (param.ParameterValue != null)
                    {
                        LogDebug($"Processing parameter: {paramName}");
                        Visitor.Visit(param.ParameterValue);
                        
                        // Track parameter mappings if this is a column or variable
                        if (param.ParameterValue is ColumnReferenceExpression colRef)
                        {
                            string sourceColumnName = string.Join(".", colRef.MultiPartIdentifier.Identifiers.Select(i => i.Value));
                            
                            // Create parameter node if needed
                            var paramNode = GetOrCreateParameterNode(procedureName, paramName);

                            // Add lineage from source column to parameter
                            if (Context.GetColumnContext("current") is ColumnNode sourceColumn)
                            {
                                var edge = new LineageEdge
                                {
                                    Id = Guid.NewGuid().ToString(),
                                    SourceId = sourceColumn.Id,
                                    TargetId = paramNode.Id,
                                    Type = "parameter",
                                    Operation = "map",
                                    SqlExpression = GetSqlText(param)
                                };
                                
                                Graph.AddEdge(edge);
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Process a variable declaration
        /// </summary>
        public void ProcessVariableDeclaration(DeclareVariableStatement node)
        {
            if (node?.Declarations == null || node.Declarations.Count == 0)
                return;

            foreach (var declaration in node.Declarations)
            {
                string varName = declaration.VariableName?.Value ?? "";
                if (string.IsNullOrEmpty(varName))
                    continue;
                
                LogDebug($"Processing variable declaration: {varName}");
                
                // Create a column node to represent the variable
                var variableNode = new ColumnNode
                {
                    Id = CreateNodeId("VARIABLE", varName),
                    Name = varName,
                    ObjectName = varName,
                    TableOwner = Context.Metadata.ContainsKey("CurrentProcedure") 
                              ? Context.Metadata["CurrentProcedure"]?.ToString() ?? ""
                              : "Variables",
                    DataType = declaration.DataType != null ? GetSqlText(declaration.DataType) : "unknown"
                };
                
                Graph.AddNode(variableNode);
                _variableAssignments[varName.ToLowerInvariant()] = variableNode;
                
                // Handle initial assignment if present
                if (declaration.Value != null)
                {
                    // Save current context
                    var previousContext = Context.GetColumnContext("current");
                    
                    // Set target as this variable
                    var assignContext = new ColumnLineageContext
                    {
                        TargetColumn = variableNode,
                        DependencyType = "assignment"
                    };
                    
                    Context.SetColumnContext("current", variableNode);
                    
                    try 
                    {
                        // Process the value expression
                        Visitor.Visit(declaration.Value);
                    }
                    finally
                    {
                        // Restore context
                        Context.SetColumnContext("current", previousContext);
                    }
                }
            }
        }

        /// <summary>
        /// Process a SET statement for variable assignment
        /// </summary>
        public void ProcessSetVariableStatement(SetVariableStatement node)
        {
            if (node?.Variable == null || node.Expression == null)
                return;
                
            string varName = node.Variable.Name;
            LogDebug($"Processing variable assignment: {varName}");
            
            // Get or create variable node
            var variableNode = GetOrCreateVariableNode(varName);
            
            // Save current context
            var previousContext = Context.GetColumnContext("current");
            
            // Set target as this variable
            var assignContext = new ColumnLineageContext
            {
                TargetColumn = variableNode,
                DependencyType = "assignment"
            };
            
            Context.SetColumnContext("current", variableNode);
            
            try 
            {
                // Process the value expression
                Visitor.Visit(node.Expression);
            }
            finally
            {
                // Restore context
                Context.SetColumnContext("current", previousContext);
            }
        }

        /// <summary>
        /// Process a SELECT INTO statement (captures column lineage)
        /// </summary>
        public void ProcessSelectIntoStatement(SelectStatement node)
        {
            if (node?.Into == null)
                return;

            // Process by delegating to the TempTableHandler
            var tempTableHandler = new TempTableHandler(Visitor, Graph, Context, Logger);
            tempTableHandler.ProcessSelectIntoTempTable(node);
        }

        #region Helper Methods

        /// <summary>
        /// Process parameters in a stored procedure definition
        /// </summary>
        private void ProcessParameters(IList<ProcedureParameter> parameters, TableNode procedureNode)
        {
            if (parameters == null || parameters.Count == 0)
                return;

            foreach (var param in parameters)
            {
                if (param?.VariableName?.Value == null)
                    continue;

                string paramName = param.VariableName.Value;
                string dataType = param.DataType != null ? GetSqlText(param.DataType) : "unknown";

                LogDebug($"Processing parameter: {paramName} ({dataType})");

                var paramNode = new ColumnNode
                {
                    Id = CreateNodeId("PARAMETER", $"{procedureNode.Name}.{paramName}"),
                    Name = paramName,
                    ObjectName = paramName,
                    TableOwner = procedureNode.Name,
                    DataType = dataType
                };

                Graph.AddNode(paramNode);
                procedureNode.Columns.Add(paramNode.Id);
                _procedureParameters[paramName.ToLowerInvariant()] = paramNode;
            }
        }

        /// <summary>
        /// Process a statement within a stored procedure
        /// </summary>
        private void ProcessStatement(TSqlStatement statement, string procedureName)
        {
            if (statement == null)
                return;

            // Specific handling for various statement types
            switch (statement)
            {
                case SelectStatement selectStmt:
                    // If this is SELECT INTO, use special handler
                    if (selectStmt.Into != null)
                    {
                        ProcessSelectIntoStatement(selectStmt);
                    }
                    else
                    {
                        // Regular SELECT processing
                        Visitor.Visit(selectStmt);
                    }
                    break;

                case DeclareVariableStatement declareStmt:
                    ProcessVariableDeclaration(declareStmt);
                    break;

                case SetVariableStatement setStmt:
                    ProcessSetVariableStatement(setStmt);
                    break;

                case ExecuteStatement execStmt:
                    ProcessExecuteStatement(execStmt);
                    break;

                case IfStatement ifStmt:
                    // Process both branches
                    if (ifStmt.ThenStatement != null)
                    {
                        ProcessStatement(ifStmt.ThenStatement, procedureName);
                    }
                    if (ifStmt.ElseStatement != null)
                    {
                        ProcessStatement(ifStmt.ElseStatement, procedureName);
                    }
                    break;

                case WhileStatement whileStmt:
                    if (whileStmt.Statement != null)
                    {
                        ProcessStatement(whileStmt.Statement, procedureName);
                    }
                    break;

                case BeginEndBlockStatement blockStmt:
                    if (blockStmt.StatementList?.Statements != null)
                    {
                        foreach (var stmt in blockStmt.StatementList.Statements)
                        {
                            ProcessStatement(stmt, procedureName);
                        }
                    }
                    break;

                default:
                    // Default handling for other statement types
                    Visitor.Visit(statement);
                    break;
            }
        }

        /// <summary>
        /// Get the procedure name from a CREATE PROCEDURE statement
        /// </summary>
        private string GetProcedureName(CreateProcedureStatement node)
        {
            try
            {
                // Extract procedure name using reflection
                var procRefProp = node.GetType().GetProperty("ProcedureReference");
                if (procRefProp != null)
                {
                    var procRef = procRefProp.GetValue(node);
                    if (procRef != null)
                    {
                        // Try different approaches to get the name
                        
                        // Approach 1: Try to get Name.SchemaObject.Identifiers
                        var nameProp = procRef.GetType().GetProperty("Name");
                        if (nameProp != null)
                        {
                            var nameObj = nameProp.GetValue(procRef);
                            if (nameObj != null)
                            {
                                // Try to get SchemaObject
                                var schemaObjProp = nameObj.GetType().GetProperty("SchemaObject");
                                if (schemaObjProp != null)
                                {
                                    var schemaObj = schemaObjProp.GetValue(nameObj);
                                    if (schemaObj != null)
                                    {
                                        // Try to get Identifiers
                                        var idsProp = schemaObj.GetType().GetProperty("Identifiers");
                                        if (idsProp != null)
                                        {
                                            var ids = idsProp.GetValue(schemaObj) as IEnumerable<object>;
                                            if (ids != null)
                                            {
                                                // Extract identifier values
                                                var idValues = new List<string>();
                                                foreach (var id in ids)
                                                {
                                                    var valueProp = id.GetType().GetProperty("Value");
                                                    if (valueProp != null)
                                                    {
                                                        var value = valueProp.GetValue(id)?.ToString();
                                                        if (!string.IsNullOrEmpty(value))
                                                        {
                                                            idValues.Add(value);
                                                        }
                                                    }
                                                }
                                                
                                                if (idValues.Count > 0)
                                                {
                                                    return string.Join(".", idValues);
                                                }
                                            }
                                        }
                                    }
                                }
                                
                                // Fallback: try to extract name directly
                                return nameObj.ToString() ?? "UnknownProcedure";
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogDebug($"Error getting procedure name: {ex.Message}");
            }
            
            return "UnknownProcedure";
        }

        /// <summary>
        /// Get the procedure name from an EXECUTE statement
        /// </summary>
        private string GetExecutedProcedureName(ExecuteStatement node)
        {
            try
            {
                // Extract procedure name using reflection
                var execSpecProp = node.GetType().GetProperty("ExecuteSpecification");
                if (execSpecProp != null)
                {
                    var execSpec = execSpecProp.GetValue(node);
                    if (execSpec != null)
                    {
                        var entityProp = execSpec.GetType().GetProperty("ExecutableEntity");
                        if (entityProp != null)
                        {
                            var entity = entityProp.GetValue(execSpec);
                            if (entity != null && entity.GetType().Name.Contains("ProcedureReference"))
                            {
                                var procRefProp = entity.GetType().GetProperty("ProcedureReference");
                                if (procRefProp != null)
                                {
                                    var procRef = procRefProp.GetValue(entity);
                                    if (procRef != null)
                                    {
                                        // Try to get identifiers from procedure reference
                                        var idValues = ExtractIdentifiersFromObject(procRef);
                                        if (idValues.Count > 0)
                                        {
                                            return string.Join(".", idValues);
                                        }
                                        
                                        // Fallback to string representation
                                        return procRef.ToString() ?? string.Empty;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogDebug($"Error getting executed procedure name: {ex.Message}");
            }
            
            return string.Empty;
        }

        /// <summary>
        /// Get or create a parameter node for a procedure
        /// </summary>
        private ColumnNode GetOrCreateParameterNode(string procedureName, string paramName)
        {
            string key = paramName.ToLowerInvariant();
            
            if (_procedureParameters.TryGetValue(key, out var paramNode))
            {
                return paramNode;
            }
            
            // Create parameter node
            paramNode = new ColumnNode
            {
                Id = CreateNodeId("PARAMETER", $"{procedureName}.{paramName}"),
                Name = paramName,
                ObjectName = paramName,
                TableOwner = procedureName,
                DataType = "unknown"
            };
            
            Graph.AddNode(paramNode);
            _procedureParameters[key] = paramNode;
            
            return paramNode;
        }

        /// <summary>
        /// Get or create a variable node
        /// </summary>
        private ColumnNode GetOrCreateVariableNode(string varName)
        {
            string key = varName.ToLowerInvariant();
            
            if (_variableAssignments.TryGetValue(key, out var varNode))
            {
                return varNode;
            }
            
            // Create variable node
            string procName = Context.Metadata.ContainsKey("CurrentProcedure") 
                          ? Context.Metadata["CurrentProcedure"]?.ToString() ?? ""
                          : "Variables";
                          
            varNode = new ColumnNode
            {
                Id = CreateNodeId("VARIABLE", varName),
                Name = varName,
                ObjectName = varName,
                TableOwner = procName,
                DataType = "unknown"
            };
            
            Graph.AddNode(varNode);
            _variableAssignments[key] = varNode;
            
            return varNode;
        }

        /// <summary>
        /// Extract identifier values from an object using reflection
        /// </summary>
        private List<string> ExtractIdentifiersFromObject(object obj)
        {
            var result = new List<string>();
            
            try
            {
                // Try different approaches to extract identifiers
                
                // Approach 1: Try Name.SchemaObject.Identifiers
                var nameProp = obj.GetType().GetProperty("Name");
                if (nameProp != null)
                {
                    var nameObj = nameProp.GetValue(obj);
                    if (nameObj != null)
                    {
                        var schemaObjProp = nameObj.GetType().GetProperty("SchemaObject");
                        if (schemaObjProp != null)
                        {
                            var schemaObj = schemaObjProp.GetValue(nameObj);
                            if (schemaObj != null)
                            {
                                var idsProp = schemaObj.GetType().GetProperty("Identifiers");
                                if (idsProp != null)
                                {
                                    var ids = idsProp.GetValue(schemaObj) as IEnumerable<object>;
                                    if (ids != null)
                                    {
                                        foreach (var id in ids)
                                        {
                                            var valueProp = id.GetType().GetProperty("Value");
                                            if (valueProp != null)
                                            {
                                                var value = valueProp.GetValue(id)?.ToString();
                                                if (!string.IsNullOrEmpty(value))
                                                {
                                                    result.Add(value);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                
                // Approach 2: Try direct Identifiers property
                if (result.Count == 0)
                {
                    var idsProp = obj.GetType().GetProperty("Identifiers");
                    if (idsProp != null)
                    {
                        var ids = idsProp.GetValue(obj) as IEnumerable<object>;
                        if (ids != null)
                        {
                            foreach (var id in ids)
                            {
                                var valueProp = id.GetType().GetProperty("Value");
                                if (valueProp != null)
                                {
                                    var value = valueProp.GetValue(id)?.ToString();
                                    if (!string.IsNullOrEmpty(value))
                                    {
                                        result.Add(value);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogDebug($"Error extracting identifiers: {ex.Message}");
            }
            
            return result;
        }
        
        #endregion
    }
}

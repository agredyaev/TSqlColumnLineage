using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Extensions;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Models.Edges;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Statements
{
    /// <summary>
    /// Handler for stored procedure statements, including CREATE PROCEDURE, 
    /// EXECUTE statements, variable declarations, and parameter tracking.
    /// </summary>
    public class StoredProcedureHandler : AbstractQueryHandler
    {
        // Track procedure parameters for lineage tracking
        private readonly Dictionary<string, ColumnNode> _parameters = new(StringComparer.OrdinalIgnoreCase);
        
        // Track variable assignments to follow data flow
        private readonly Dictionary<string, ColumnNode> _variables = new(StringComparer.OrdinalIgnoreCase);
        
        /// <summary>
        /// Creates a new stored procedure handler
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="logger">Logger (optional)</param>
        public StoredProcedureHandler(VisitorContext context, ILogger logger = null)
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
            return fragment is CreateProcedureStatement ||
                   fragment is ExecuteStatement ||
                   fragment is DeclareVariableStatement ||
                   fragment is SetVariableStatement;
        }
        
        /// <summary>
        /// Processes the SQL fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <param name="context">Visitor context</param>
        /// <returns>True if the fragment was fully processed; otherwise, false</returns>
        public override bool Handle(TSqlFragment fragment, VisitorContext context)
        {
            try
            {
                if (fragment is CreateProcedureStatement createProc)
                {
                    ProcessCreateProcedure(createProc);
                    return true;
                }
                else if (fragment is ExecuteStatement executeStmt)
                {
                    ProcessExecuteStatement(executeStmt);
                    return true;
                }
                else if (fragment is DeclareVariableStatement declareVar)
                {
                    ProcessDeclareVariable(declareVar);
                    return true;
                }
                else if (fragment is SetVariableStatement setVar)
                {
                    ProcessSetVariable(setVar);
                    return true;
                }
                
                return false;
            }
            catch (Exception ex)
            {
                LogError($"Error processing {fragment.GetType().Name}", ex);
                return false;
            }
        }
        
        /// <summary>
        /// Processes a CREATE PROCEDURE statement
        /// </summary>
        private void ProcessCreateProcedure(CreateProcedureStatement createProc)
        {
            if (createProc == null)
                return;
                
            string procedureName = ExtractProcedureName(createProc);
            LogDebug($"Processing CREATE PROCEDURE {procedureName}");
            
            // Create table node for the procedure
            var procedureNode = new TableNode
            {
                Id = CreateNodeId("PROC", procedureName),
                Name = procedureName,
                ObjectName = procedureName,
                SchemaName = string.Empty,
                TableType = "StoredProcedure",
                Definition = GetSqlText(createProc)
            };
            
            Graph.AddNode(procedureNode);
            LineageContext.AddTable(procedureNode);
            
            // Register in metadata
            LineageContext.Metadata["CurrentProcedure"] = procedureName;
            
            // Process parameters
            if (createProc.Parameters != null)
            {
                foreach (var param in createProc.Parameters)
                {
                    ProcessParameter(param, procedureNode);
                }
            }
            
            // Save current context state
            var inProcedure = Context.State.ContainsKey("InStoredProcedure");
            
            // Set context
            Context.State["InStoredProcedure"] = true;
            
            try
            {
                // Process procedure body
                if (createProc.StatementList?.Statements != null)
                {
                    foreach (var statement in createProc.StatementList.Statements)
                    {
                        ProcessStatement(statement);
                    }
                }
            }
            finally
            {
                // Restore context
                if (!inProcedure)
                {
                    Context.State.Remove("InStoredProcedure");
                }
                
                LineageContext.Metadata.Remove("CurrentProcedure");
            }
        }
        
        /// <summary>
        /// Processes a parameter in a procedure
        /// </summary>
        private void ProcessParameter(ProcedureParameter param, TableNode procedureNode)
        {
            if (param?.VariableName?.Value == null)
                return;
                
            string paramName = param.VariableName.Value;
            string dataType = param.DataType != null ? GetSqlText(param.DataType) : "unknown";
            
            LogDebug($"Processing parameter {paramName} ({dataType})");
            
            // Create parameter node
            var paramNode = new ColumnNode
            {
                Id = CreateNodeId("PARAM", $"{procedureNode.Name}.{paramName}"),
                Name = paramName,
                ObjectName = paramName,
                TableOwner = procedureNode.Name,
                SchemaName = string.Empty,
                DataType = dataType
            };
            
            // Add parameter info
            paramNode.Metadata["Direction"] = param.IsOutput ? "OUTPUT" : "INPUT";
            
            Graph.AddNode(paramNode);
            procedureNode.Columns.Add(paramNode.Id);
            
            // Track parameter
            _parameters[paramName.ToLowerInvariant()] = paramNode;
            
            // Process default value if specified
            if (param.Value != null)
            {
                ProcessParameterDefaultValue(param.Value, paramNode);
            }
        }
        
        /// <summary>
        /// Processes a parameter default value
        /// </summary>
        private void ProcessParameterDefaultValue(ScalarExpression defaultValue, ColumnNode paramNode)
        {
            if (defaultValue == null || paramNode == null)
                return;
                
            // Create expression node for default value
            var expressionNode = new ExpressionNode
            {
                Id = CreateNodeId("EXPR", $"{paramNode.TableOwner}.{paramNode.Name}_Default"),
                Name = $"{paramNode.Name}_Default",
                ObjectName = GetSqlText(defaultValue),
                ExpressionType = "DefaultValue",
                Expression = GetSqlText(defaultValue),
                TableOwner = paramNode.TableOwner,
                ResultType = paramNode.DataType
            };
            
            Graph.AddNode(expressionNode);
            
            // Create edge from expression to parameter
            var edge = CreateDirectEdge(
                expressionNode.Id,
                paramNode.Id,
                "default",
                $"Default value for parameter {paramNode.TableOwner}.{paramNode.Name}"
            );
            
            Graph.AddEdge(edge);
            
            // Extract column references from default value
            var columnRefs = new List<ColumnReferenceExpression>();
            ExtractColumnReferences(defaultValue, columnRefs);
            
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
                        $"Referenced in parameter default value: {refColumn.TableOwner}.{refColumn.Name}"
                    );
                    
                    Graph.AddEdge(refEdge);
                }
            }
        }
        
        /// <summary>
        /// Processes a statement in a procedure
        /// </summary>
        private void ProcessStatement(TSqlStatement statement)
        {
            if (statement == null)
                return;
            
            try
            {
                // First try to process the statement with specialzed handling
                if (statement is DeclareVariableStatement declareVar)
                {
                    ProcessDeclareVariable(declareVar);
                }
                else if (statement is SetVariableStatement setVar)
                {
                    ProcessSetVariable(setVar);
                }
                else if (statement is ExecuteStatement execStmt)
                {
                    ProcessExecuteStatement(execStmt);
                }
                else if (statement is IfStatement ifStmt)
                {
                    // Handle If statement specially - process both branches
                    if (ifStmt.ThenStatement != null)
                    {
                        ProcessStatement(ifStmt.ThenStatement);
                    }
                    
                    if (ifStmt.ElseStatement != null)
                    {
                        ProcessStatement(ifStmt.ElseStatement);
                    }
                }
                else if (statement is WhileStatement whileStmt)
                {
                    // Handle While statement specially - process body
                    if (whileStmt.Statement != null)
                    {
                        ProcessStatement(whileStmt.Statement);
                    }
                }
                else if (statement is BeginEndBlockStatement blockStmt)
                {
                    // Process each statement in the block
                    if (blockStmt.StatementList?.Statements != null)
                    {
                        foreach (var stmt in blockStmt.StatementList.Statements)
                        {
                            ProcessStatement(stmt);
                        }
                    }
                }
                else
                {
                    // Default case - use the visitor to process the statement
                    var visitor = CreateVisitor();
                    if (visitor != null)
                    {
                        statement.Accept(visitor);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError($"Error processing statement {statement.GetType().Name}", ex);
            }
        }
        
        /// <summary>
        /// Processes an EXECUTE statement
        /// </summary>
        private void ProcessExecuteStatement(ExecuteStatement execute)
        {
            if (execute?.ExecuteSpecification?.ExecutableEntity == null)
                return;
                
            string procedureName = ExtractExecuteTargetName(execute);
            LogDebug($"Processing EXECUTE {procedureName}");
            
            // Create expression node for the execution
            var executeNode = new ExpressionNode
            {
                Id = CreateNodeId("EXEC", procedureName),
                Name = $"Execute_{procedureName}",
                ObjectName = GetSqlText(execute),
                ExpressionType = "StoredProcedureExecution",
                Expression = GetSqlText(execute)
            };
            
            Graph.AddNode(executeNode);
            
            // Process parameters if provided
            if (execute.ExecuteSpecification.ExecutableEntity is ExecutableProcedureReference procRef &&
                procRef.Parameters != null)
            {
                for (int i = 0; i < procRef.Parameters.Count; i++)
                {
                    var param = procRef.Parameters[i];
                    
                    // Determine parameter name
                    string paramName = $"@Param{i + 1}";
                    
                    if (param.Variable != null)
                    {
                        paramName = param.Variable.Name;
                    }
                    else if (param.ParameterName != null)
                    {
                        paramName = param.ParameterName.Value;
                    }
                    
                    LogDebug($"Processing parameter {paramName}");
                    
                    // Process parameter value
                    if (param.ParameterValue != null)
                    {
                        var paramNode = GetOrCreateParameter(procedureName, paramName);
                        
                        if (param.ParameterValue is ColumnReferenceExpression colRef)
                        {
                            // Column reference - find the source column
                            var sourceColumn = FindSourceColumn(colRef);
                            
                            if (sourceColumn != null)
                            {
                                // Create edge from source column to parameter
                                var edge = CreateDirectEdge(
                                    sourceColumn.Id,
                                    paramNode.Id,
                                    "parameter",
                                    $"Parameter mapping: {sourceColumn.TableOwner}.{sourceColumn.Name} -> {paramNode.TableOwner}.{paramNode.Name}"
                                );
                                
                                Graph.AddEdge(edge);
                                
                                // Create edge from parameter to execute node
                                var execEdge = CreateDirectEdge(
                                    paramNode.Id,
                                    executeNode.Id,
                                    "execute",
                                    $"Execute parameter: {paramNode.TableOwner}.{paramNode.Name}"
                                );
                                
                                Graph.AddEdge(execEdge);
                            }
                        }
                        else if (param.ParameterValue is VariableReference varRef)
                        {
                            // Variable reference - find the variable
                            if (_variables.TryGetValue(varRef.Name.ToLowerInvariant(), out var variableNode))
                            {
                                // Create edge from variable to parameter
                                var edge = CreateDirectEdge(
                                    variableNode.Id,
                                    paramNode.Id,
                                    "parameter",
                                    $"Parameter mapping: {variableNode.TableOwner}.{variableNode.Name} -> {paramNode.TableOwner}.{paramNode.Name}"
                                );
                                
                                Graph.AddEdge(edge);
                                
                                // Create edge from parameter to execute node
                                var execEdge = CreateDirectEdge(
                                    paramNode.Id,
                                    executeNode.Id,
                                    "execute",
                                    $"Execute parameter: {paramNode.TableOwner}.{paramNode.Name}"
                                );
                                
                                Graph.AddEdge(execEdge);
                            }
                        }
                        else
                        {
                            // Other expression - create value node
                            var valueNode = new ExpressionNode
                            {
                                Id = CreateNodeId("EXPR", $"{paramNode.TableOwner}.{paramNode.Name}_Value"),
                                Name = $"{paramNode.Name}_Value",
                                ObjectName = GetSqlText(param.ParameterValue),
                                ExpressionType = "ParameterValue",
                                Expression = GetSqlText(param.ParameterValue),
                                TableOwner = paramNode.TableOwner,
                                ResultType = paramNode.DataType
                            };
                            
                            Graph.AddNode(valueNode);
                            
                            // Create edge from value node to parameter
                            var edge = CreateDirectEdge(
                                valueNode.Id,
                                paramNode.Id,
                                "parameter",
                                $"Parameter value: {valueNode.Name} -> {paramNode.TableOwner}.{paramNode.Name}"
                            );
                            
                            Graph.AddEdge(edge);
                            
                            // Create edge from parameter to execute node
                            var execEdge = CreateDirectEdge(
                                paramNode.Id,
                                executeNode.Id,
                                "execute",
                                $"Execute parameter: {paramNode.TableOwner}.{paramNode.Name}"
                            );
                            
                            Graph.AddEdge(execEdge);
                            
                            // Extract column references from the value
                            var columnRefs = new List<ColumnReferenceExpression>();
                            ExtractColumnReferences(param.ParameterValue, columnRefs);
                            
                            foreach (var colRef in columnRefs)
                            {
                                var sourceColumn = FindSourceColumn(colRef);
                                
                                if (sourceColumn != null)
                                {
                                    // Create edge from source column to value node
                                    var sourceEdge = CreateIndirectEdge(
                                        sourceColumn.Id,
                                        valueNode.Id,
                                        "reference",
                                        $"Referenced in parameter value: {sourceColumn.TableOwner}.{sourceColumn.Name}"
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
        /// Processes a DECLARE VARIABLE statement
        /// </summary>
        private void ProcessDeclareVariable(DeclareVariableStatement declareVar)
        {
            if (declareVar?.Declarations == null)
                return;
                
            foreach (var declaration in declareVar.Declarations)
            {
                if (declaration?.VariableName?.Value == null)
                    continue;
                    
                string varName = declaration.VariableName.Value;
                string dataType = declaration.DataType != null ? GetSqlText(declaration.DataType) : "unknown";
                
                LogDebug($"Processing variable declaration {varName} ({dataType})");
                
                // Determine owner
                string owner = "Variables";
                if (LineageContext.Metadata.TryGetValue("CurrentProcedure", out var proc) && proc is string procName)
                {
                    owner = procName;
                }
                
                // Create variable node
                var variableNode = new ColumnNode
                {
                    Id = CreateNodeId("VAR", varName),
                    Name = varName,
                    ObjectName = varName,
                    TableOwner = owner,
                    SchemaName = string.Empty,
                    DataType = dataType
                };
                
                Graph.AddNode(variableNode);
                
                // Track variable
                _variables[varName.ToLowerInvariant()] = variableNode;
                
                // Process initial value if specified
                if (declaration.Value != null)
                {
                    ProcessVariableInitialValue(declaration.Value, variableNode);
                }
            }
        }
        
        /// <summary>
        /// Processes initial value for a variable declaration
        /// </summary>
        private void ProcessVariableInitialValue(ScalarExpression value, ColumnNode variableNode)
        {
            if (value == null || variableNode == null)
                return;
                
            // Create expression node for the value
            var expressionNode = new ExpressionNode
            {
                Id = CreateNodeId("EXPR", $"{variableNode.TableOwner}.{variableNode.Name}_Value"),
                Name = $"{variableNode.Name}_Value",
                ObjectName = GetSqlText(value),
                ExpressionType = "InitialValue",
                Expression = GetSqlText(value),
                TableOwner = variableNode.TableOwner,
                ResultType = variableNode.DataType
            };
            
            Graph.AddNode(expressionNode);
            
            // Create edge from expression to variable
            var edge = CreateDirectEdge(
                expressionNode.Id,
                variableNode.Id,
                "assign",
                $"Initial value for {variableNode.TableOwner}.{variableNode.Name}"
            );
            
            Graph.AddEdge(edge);
            
            // Extract column references from the expression
            var columnRefs = new List<ColumnReferenceExpression>();
            ExtractColumnReferences(value, columnRefs);
            
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
                        $"Referenced in variable value: {sourceColumn.TableOwner}.{sourceColumn.Name}"
                    );
                    
                    Graph.AddEdge(sourceEdge);
                }
            }
        }
        
        /// <summary>
        /// Processes a SET VARIABLE statement
        /// </summary>
        private void ProcessSetVariable(SetVariableStatement setVar)
        {
            if (setVar?.Variable == null || setVar.Expression == null)
                return;
                
            string varName = setVar.Variable.Name;
            LogDebug($"Processing variable assignment {varName}");
            
            // Get or create variable node
            var variableNode = GetOrCreateVariable(varName);
            
            // Create expression node for the assignment
            var expressionNode = new ExpressionNode
            {
                Id = CreateNodeId("EXPR", $"{variableNode.TableOwner}.{variableNode.Name}_Assign"),
                Name = $"{variableNode.Name}_Assign",
                ObjectName = GetSqlText(setVar.Expression),
                ExpressionType = "Assignment",
                Expression = GetSqlText(setVar.Expression),
                TableOwner = variableNode.TableOwner,
                ResultType = variableNode.DataType
            };
            
            Graph.AddNode(expressionNode);
            
            // Create edge from expression to variable
            var edge = CreateDirectEdge(
                expressionNode.Id,
                variableNode.Id,
                "assign",
                $"Assignment to {variableNode.TableOwner}.{variableNode.Name}"
            );
            
            Graph.AddEdge(edge);
            
            // Extract column references from the expression
            var columnRefs = new List<ColumnReferenceExpression>();
            ExtractColumnReferences(setVar.Expression, columnRefs);
            
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
                        $"Referenced in assignment: {sourceColumn.TableOwner}.{sourceColumn.Name}"
                    );
                    
                    Graph.AddEdge(sourceEdge);
                }
            }
        }
        
        /// <summary>
        /// Finds a source column from a column reference
        /// </summary>
        private ColumnNode FindSourceColumn(ColumnReferenceExpression colRef)
        {
            if (colRef?.MultiPartIdentifier?.Identifiers == null || colRef.MultiPartIdentifier.Identifiers.Count == 0)
                return null;
                
            string columnName = colRef.MultiPartIdentifier.Identifiers.Last().Value;
            string tableName = null;
            
            // If there's a table specifier
            if (colRef.MultiPartIdentifier.Identifiers.Count > 1)
            {
                tableName = colRef.MultiPartIdentifier.Identifiers[0].Value;
                
                // Resolve alias if needed
                if (LineageContext.TableAliases.TryGetValue(tableName, out var resolvedTable))
                {
                    tableName = resolvedTable;
                }
            }
            
            // Find the column
            if (!string.IsNullOrEmpty(tableName))
            {
                return Graph.GetColumnNode(tableName, columnName);
            }
            
            // Try to find in variables
            string varKey = columnName.ToLowerInvariant();
            if (_variables.TryGetValue(varKey, out var variableNode))
            {
                return variableNode;
            }
            
            // Try to find in parameters 
            if (_parameters.TryGetValue(varKey, out var paramNode))
            {
                return paramNode;
            }
            
            // Try to find in any table
            foreach (var table in LineageContext.Tables.Values)
            {
                var column = Graph.GetColumnNode(table.Name, columnName);
                if (column != null)
                {
                    return column;
                }
            }
            
            return null;
        }
        
        /// <summary>
        /// Gets or creates a parameter node 
        /// </summary>
        private ColumnNode GetOrCreateParameter(string procName, string paramName)
        {
            string key = paramName.ToLowerInvariant();
            
            if (_parameters.TryGetValue(key, out var paramNode))
            {
                return paramNode;
            }
            
            // Parameter not found, create a new one
            paramNode = new ColumnNode
            {
                Id = CreateNodeId("PARAM", $"{procName}.{paramName}"),
                Name = paramName,
                ObjectName = paramName,
                TableOwner = procName,
                SchemaName = string.Empty,
                DataType = "unknown"
            };
            
            paramNode.Metadata["Dynamic"] = true;
            
            Graph.AddNode(paramNode);
            _parameters[key] = paramNode;
            
            return paramNode;
        }
        
        /// <summary>
        /// Gets or creates a variable node
        /// </summary>
        private ColumnNode GetOrCreateVariable(string varName)
        {
            string key = varName.ToLowerInvariant();
            
            if (_variables.TryGetValue(key, out var varNode))
            {
                return varNode;
            }
            
            // Variable not found, create a new one
            string owner = "Variables";
            if (LineageContext.Metadata.TryGetValue("CurrentProcedure", out var proc) && proc is string procName)
            {
                owner = procName;
            }
            
            varNode = new ColumnNode
            {
                Id = CreateNodeId("VAR", varName),
                Name = varName,
                ObjectName = varName,
                TableOwner = owner,
                SchemaName = string.Empty,
                DataType = "unknown"
            };
            
            varNode.Metadata["Dynamic"] = true;
            
            Graph.AddNode(varNode);
            _variables[key] = varNode;
            
            return varNode;
        }
        
        /// <summary>
        /// Extracts procedure name from a CREATE PROCEDURE statement
        /// </summary>
        private string ExtractProcedureName(CreateProcedureStatement statement)
        {
            try
            {
                if (statement?.ProcedureReference?.Name?.BaseIdentifier?.Value != null)
                {
                    return statement.ProcedureReference.Name.BaseIdentifier.Value;
                }
                
                if (statement?.ProcedureReference?.Name?.SchemaIdentifier?.Value != null &&
                    statement?.ProcedureReference?.Name?.BaseIdentifier?.Value != null)
                {
                    return $"{statement.ProcedureReference.Name.SchemaIdentifier.Value}.{statement.ProcedureReference.Name.BaseIdentifier.Value}";
                }
                
                // Use reflection as a fallback for different ScriptDom versions
                var nameObj = ReflectionUtils.GetPropertyValue(statement?.ProcedureReference, "Name");
                if (nameObj != null)
                {
                    var baseId = ReflectionUtils.GetPropertyValue(nameObj, "BaseIdentifier");
                    var value = ReflectionUtils.GetPropertyValue(baseId, "Value")?.ToString();
                    
                    if (!string.IsNullOrEmpty(value))
                    {
                        return value;
                    }
                }
                
                return "UnknownProcedure";
            }
            catch
            {
                return "UnknownProcedure";
            }
        }
        
        /// <summary>
        /// Extracts the procedure name from an EXECUTE statement
        /// </summary>
        private string ExtractExecuteTargetName(ExecuteStatement statement)
        {
            try
            {
                if (statement?.ExecuteSpecification?.ExecutableEntity is ExecutableProcedureReference procRef)
                {
                    if (procRef.ProcedureReference?.Name?.BaseIdentifier?.Value != null)
                    {
                        return procRef.ProcedureReference.Name.BaseIdentifier.Value;
                    }
                }
                
                // Use reflection as a fallback for different ScriptDom versions
                var entityObj = ReflectionUtils.GetPropertyValue(statement?.ExecuteSpecification, "ExecutableEntity");
                if (entityObj != null)
                {
                    var refObj = ReflectionUtils.GetPropertyValue(entityObj, "ProcedureReference");
                    var nameObj = ReflectionUtils.GetPropertyValue(refObj, "Name");
                    var baseId = ReflectionUtils.GetPropertyValue(nameObj, "BaseIdentifier");
                    var value = ReflectionUtils.GetPropertyValue(baseId, "Value")?.ToString();
                    
                    if (!string.IsNullOrEmpty(value))
                    {
                        return value;
                    }
                }
                
                return "UnknownProcedure";
            }
            catch
            {
                return "UnknownProcedure";
            }
        }
        
        /// <summary>
        /// Creates a visitor for processing statements
        /// </summary>
        private TSqlFragmentVisitor CreateVisitor()
        {
            // Check for cached visitor factory in context
            if (Context.State.TryGetValue("VisitorFactory", out var factory) && factory is Func<VisitorContext, ILogger, TSqlFragmentVisitor>)
            {
                return (factory as Func<VisitorContext, ILogger, TSqlFragmentVisitor>)(Context, Logger);
            }
            
            // Try to create a new visitor instance using reflection
            try
            {
                var visitorType = Type.GetType("TSqlColumnLineage.Core.Analysis.Visitors.Specialized.ColumnLineageVisitor, TSqlColumnLineage.Core");
                if (visitorType != null)
                {
                    return Activator.CreateInstance(visitorType, Context, null, Logger) as TSqlFragmentVisitor;
                }
            }
            catch
            {
                // Fallback to legacy reflection approach
                try
                {
                    return Context.GetType().Assembly.CreateInstance(
                        "TSqlColumnLineage.Core.Analysis.Visitors.Specialized.ColumnLineageVisitor",
                        false,
                        System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic,
                        null,
                        new[] { Context, null, Logger },
                        null,
                        null) as TSqlFragmentVisitor;
                }
                catch
                {
                    LogError("Failed to create visitor instance");
                    return null;
                }
            }
            
            return null;
        }
    }
}
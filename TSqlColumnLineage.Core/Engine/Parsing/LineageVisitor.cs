using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Domain;
using TSqlColumnLineage.Core.Domain.Graph;
using TSqlColumnLineage.Core.Infrastructure.Memory;
using TSqlColumnLineage.Core.Infrastructure.Monitoring;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// ScriptDom visitor that extracts column lineage information 
    /// directly from parsed T-SQL with infrastructure integration
    /// </summary>
    public class LineageVisitor : TSqlFragmentVisitor
    {
        private readonly LineageGraph _graph;
        private readonly Stack<LineageContext> _context = new();
        private readonly int _tempTableCounter = 0;
        private readonly MemoryManager _memoryManager;
        private readonly PerformanceTracker _performanceTracker;
        
        public LineageVisitor(LineageGraph graph)
        {
            _graph = graph ?? throw new ArgumentNullException(nameof(graph));
            _memoryManager = MemoryManager.Instance;
            _performanceTracker = PerformanceTracker.Instance;
            
            // Initialize with root context
            _context.Push(new LineageContext { Type = StatementType.Unknown });
        }
        
        #region Statement Visitors
        
        public override void ExplicitVisit(SelectStatement node)
        {
            using var tracker = _performanceTracker.TrackOperation("LineageVisitor", "SELECT");
            
            // Create context for this select
            var ctx = new LineageContext { Type = StatementType.Select };
            _context.Push(ctx);
            
            try {
                // Process each part of the select
                if (node.WithCtesAndXmlNamespaces != null)
                {
                    HandleCTEs(node.WithCtesAndXmlNamespaces);
                }
                
                if (node.QueryExpression is QuerySpecification querySpec)
                {
                    // Process FROM clause first to establish table context
                    if (querySpec.FromClause != null)
                    {
                        ctx.ProcessingFromClause = true;
                        querySpec.FromClause.Accept(this);
                        ctx.ProcessingFromClause = false;
                    }
                    
                    // Process SELECT list - these are both source and potentially target columns
                    if (querySpec.SelectElements != null)
                    {
                        ctx.ProcessingSelectList = true;
                        foreach (var element in querySpec.SelectElements)
                        {
                            element.Accept(this);
                        }
                        ctx.ProcessingSelectList = false;
                    }
                    
                    // Process WHERE clause - these are source columns
                    if (querySpec.WhereClause != null)
                    {
                        ctx.ProcessingWhereClause = true;
                        querySpec.WhereClause.Accept(this);
                        ctx.ProcessingWhereClause = false;
                    }
                    
                    // Process other clauses
                    querySpec.GroupByClause?.Accept(this);
                    querySpec.HavingClause?.Accept(this);
                    querySpec.OrderByClause?.Accept(this);
                }
                else
                {
                    // Handle other types of queries (UNION, etc.)
                    node.QueryExpression?.Accept(this);
                }
                
                // Create lineage relationships
                foreach (var target in ctx.TargetColumns)
                {
                    foreach (var source in ctx.SourceColumns)
                    {
                        _graph.AddDirectLineage(source, target, "SELECT");
                    }
                }
                
                // Track operations
                _performanceTracker.IncrementCounter("Lineage", "SelectRelationships", 
                    ctx.TargetColumns.Count * ctx.SourceColumns.Count);
            }
            finally
            {
                _context.Pop();
            }
            
            base.ExplicitVisit(node);
        }
        
        public override void ExplicitVisit(InsertStatement node)
        {
            using var tracker = _performanceTracker.TrackOperation("LineageVisitor", "INSERT");
            
            var ctx = new LineageContext { Type = StatementType.Insert };
            _context.Push(ctx);
            
            try
            {
                if (node.InsertSpecification != null)
                {
                    // Get the target table
                    if (node.InsertSpecification.Target is NamedTableReference targetTable)
                    {
                        string tableName = GetTableName(targetTable);
                        tableName = _memoryManager.InternString(tableName);
                        ctx.CurrentTable = tableName;
                        
                        // Register the table
                        int tableId = _graph.AddTableNode(tableName, "Table");
                        
                        // Process column list (target columns)
                        ctx.ProcessingTargetColumns = true;
                        foreach (var column in node.InsertSpecification.Columns)
                        {
                            if (column is ColumnReferenceExpression colRef)
                            {
                                string colName = GetColumnName(colRef);
                                colName = _memoryManager.InternString(colName);
                                int columnId = _graph.AddColumnNode(colName, tableName);
                                ctx.TargetColumns.Add(columnId);
                                _graph.AddColumnToTable(tableId, columnId);
                            }
                        }
                        ctx.ProcessingTargetColumns = false;
                        
                        // Process source (VALUES or SELECT)
                        ctx.ProcessingSourceColumns = true;
                        node.InsertSpecification.InsertSource?.Accept(this);
                        ctx.ProcessingSourceColumns = false;
                    }
                }
                
                // Create lineage relationships
                foreach (var target in ctx.TargetColumns)
                {
                    foreach (var source in ctx.SourceColumns)
                    {
                        _graph.AddDirectLineage(source, target, "INSERT");
                    }
                }
                
                // Track operations
                _performanceTracker.IncrementCounter("Lineage", "InsertRelationships", 
                    ctx.TargetColumns.Count * ctx.SourceColumns.Count);
            }
            finally
            {
                _context.Pop();
            }
            
            base.ExplicitVisit(node);
        }
        
        public override void ExplicitVisit(UpdateStatement node)
        {
            using var tracker = _performanceTracker.TrackOperation("LineageVisitor", "UPDATE");
            
            var ctx = new LineageContext { Type = StatementType.Update };
            _context.Push(ctx);
            
            try
            {
                if (node.UpdateSpecification != null)
                {
                    // Get the target table
                    if (node.UpdateSpecification.Target is NamedTableReference targetTable)
                    {
                        string tableName = GetTableName(targetTable);
                        tableName = _memoryManager.InternString(tableName);
                        ctx.CurrentTable = tableName;
                        
                        // Process FROM clause first to establish table context
                        if (node.UpdateSpecification.FromClause != null)
                        {
                            ctx.ProcessingFromClause = true;
                            node.UpdateSpecification.FromClause.Accept(this);
                            ctx.ProcessingFromClause = false;
                        }
                        
                        // Process SET clauses
                        if (node.UpdateSpecification.SetClauses != null)
                        {
                            foreach (var setClause in node.UpdateSpecification.SetClauses)
                            {
                                if (setClause is AssignmentSetClause assignment)
                                {
                                    // Target column
                                    string colName = GetColumnName(assignment.Column);
                                    colName = _memoryManager.InternString(colName);
                                    int targetColumnId = _graph.AddColumnNode(colName, tableName);
                                    ctx.TargetColumns.Add(targetColumnId);
                                    
                                    // Process expression (source columns)
                                    ctx.ProcessingSourceColumns = true;
                                    assignment.NewValue?.Accept(this);
                                    ctx.ProcessingSourceColumns = false;
                                    
                                    // Create lineage for this specific assignment
                                    foreach (var source in ctx.SourceColumns)
                                    {
                                        _graph.AddDirectLineage(source, targetColumnId, "UPDATE");
                                    }
                                    
                                    // Track operations
                                    _performanceTracker.IncrementCounter("Lineage", "UpdateRelationships", 
                                        ctx.SourceColumns.Count);
                                    
                                    // Clear source columns for next assignment
                                    ctx.SourceColumns.Clear();
                                }
                            }
                        }
                        
                        // Process WHERE clause
                        if (node.UpdateSpecification.WhereClause != null)
                        {
                            ctx.ProcessingWhereClause = true;
                            node.UpdateSpecification.WhereClause.Accept(this);
                            ctx.ProcessingWhereClause = false;
                        }
                    }
                }
            }
            finally
            {
                _context.Pop();
            }
            
            base.ExplicitVisit(node);
        }
        
        public override void ExplicitVisit(MergeStatement node)
        {
            using var tracker = _performanceTracker.TrackOperation("LineageVisitor", "MERGE");
            
            var ctx = new LineageContext { Type = StatementType.Merge };
            _context.Push(ctx);
            
            try
            {
                // Process the target table
                if (node.MergeSpecification?.Target is NamedTableReference targetTable)
                {
                    string targetTableName = GetTableName(targetTable);
                    targetTableName = _memoryManager.InternString(targetTableName);
                    ctx.CurrentTable = targetTableName;
                    
                    // Process source table
                    if (node.MergeSpecification.TableReference != null)
                    {
                        ctx.ProcessingFromClause = true;
                        node.MergeSpecification.TableReference.Accept(this);
                        ctx.ProcessingFromClause = false;
                    }
                    
                    // Process ON condition
                    if (node.MergeSpecification.SearchCondition != null)
                    {
                        ctx.ProcessingWhereClause = true;
                        node.MergeSpecification.SearchCondition.Accept(this);
                        ctx.ProcessingWhereClause = false;
                    }
                    
                    // Process action clauses
                    if (node.MergeSpecification.ActionClauses != null)
                    {
                        foreach (var action in node.MergeSpecification.ActionClauses)
                        {
                            // WHEN MATCHED... UPDATE
                            if (action is MergeUpdateClause updateClause)
                            {
                                ProcessMergeUpdate(updateClause, targetTableName);
                            }
                            // WHEN MATCHED... INSERT
                            else if (action is MergeInsertClause insertClause)
                            {
                                ProcessMergeInsert(insertClause, targetTableName);
                            }
                            // Other clauses like DELETE
                        }
                    }
                }
            }
            finally
            {
                _context.Pop();
            }
            
            base.ExplicitVisit(node);
        }
        
        public override void ExplicitVisit(CreateTableStatement node)
        {
            using var tracker = _performanceTracker.TrackOperation("LineageVisitor", "CREATE_TABLE");
            
            string tableName = GetTableName(node.SchemaObjectName);
            tableName = _memoryManager.InternString(tableName);
            
            // Register the table
            int tableId = _graph.AddTableNode(tableName, "Table");
            
            // Process column definitions
            if (node.Definition is TableDefinition tableDef && tableDef.ColumnDefinitions != null)
            {
                foreach (var colDef in tableDef.ColumnDefinitions)
                {
                    string colName = colDef.ColumnIdentifier.Value;
                    colName = _memoryManager.InternString(colName);
                    string dataType = colDef.DataType?.ToString() ?? "unknown";
                    
                    int columnId = _graph.AddColumnNode(colName, tableName, dataType);
                    _graph.AddColumnToTable(tableId, columnId);
                    
                    // Track column creation
                    _performanceTracker.IncrementCounter("Lineage", "ColumnsCreated");
                }
            }
            
            base.ExplicitVisit(node);
        }
        
        #endregion
        
        #region Expression Visitors
        
        public override void ExplicitVisit(ColumnReferenceExpression node)
        {
            var ctx = _context.Peek();
            
            // Extract column information
            string columnName = GetColumnName(node);
            string tableName = GetTableName(node);
            
            // If no explicit table name, use the current table
            if (string.IsNullOrEmpty(tableName))
            {
                tableName = ctx.GetTableContext(columnName);
            }
            
            if (!string.IsNullOrEmpty(tableName) && !string.IsNullOrEmpty(columnName))
            {
                // Optimize string memory usage
                columnName = _memoryManager.InternString(columnName);
                tableName = _memoryManager.InternString(tableName);
                
                // Register the column
                int columnId = _graph.AddColumnNode(columnName, tableName);
                
                // Add to appropriate collection based on context
                if (ctx.ProcessingTargetColumns)
                {
                    ctx.TargetColumns.Add(columnId);
                }
                else if (ctx.ProcessingSourceColumns || ctx.ProcessingWhereClause || 
                         ctx.ProcessingFromClause || 
                         (ctx.ProcessingSelectList && ctx.Type == StatementType.Select))
                {
                    ctx.SourceColumns.Add(columnId);
                }
                
                // SelectList items are both source and potential targets
                if (ctx.ProcessingSelectList && ctx.Type == StatementType.Select)
                {
                    ctx.TargetColumns.Add(columnId);
                }
            }
            
            base.ExplicitVisit(node);
        }
        
        public override void ExplicitVisit(SelectScalarExpression node)
        {
            var ctx = _context.Peek();
            
            // Process the expression first (source)
            ctx.ProcessingSourceColumns = true;
            node.Expression?.Accept(this);
            ctx.ProcessingSourceColumns = false;
            
            // If we have a column alias, this is a target
            if (node.ColumnName != null)
            {
                string outputColumnName = node.ColumnName.Value;
                outputColumnName = _memoryManager.InternString(outputColumnName);
                string outputTableName = ctx.OutputTableName;
                
                if (string.IsNullOrEmpty(outputTableName))
                {
                    // Default output table for SELECT
                    outputTableName = $"#Output_{ctx.GetContextId()}";
                    ctx.OutputTableName = outputTableName;
                }
                
                outputTableName = _memoryManager.InternString(outputTableName);
                
                // Create the output column
                int columnId = _graph.AddColumnNode(outputColumnName, outputTableName);
                ctx.TargetColumns.Add(columnId);
                
                // Connect source to this specific target
                foreach (var sourceId in ctx.SourceColumns)
                {
                    _graph.AddDirectLineage(sourceId, columnId, "SELECT");
                    _performanceTracker.IncrementCounter("Lineage", "ExpressionRelationships");
                }
                
                // Clear sources for next column
                ctx.SourceColumns.Clear();
            }
            
            base.ExplicitVisit(node);
        }
        
        public override void ExplicitVisit(SelectStarExpression node)
        {
            var ctx = _context.Peek();
            
            // Handle SELECT * expressions
            string tableName = "";
            if (node.Qualifier != null)
            {
                tableName = node.Qualifier.Value;
            }
            
            // Try to find this table in our context
            if (!string.IsNullOrEmpty(tableName))
            {
                string actualTable = ctx.ResolveTableAlias(tableName);
                if (!string.IsNullOrEmpty(actualTable))
                {
                    // Add all columns from this table as sources
                    var columns = ctx.GetAllColumnsFromTable(actualTable);
                    foreach (var colId in columns)
                    {
                        ctx.SourceColumns.Add(colId);
                        
                        // For SELECT, these are also target columns
                        if (ctx.Type == StatementType.Select)
                        {
                            ctx.TargetColumns.Add(colId);
                        }
                    }
                }
            }
            
            base.ExplicitVisit(node);
        }
        
        public override void ExplicitVisit(NamedTableReference node)
        {
            var ctx = _context.Peek();
            
            // Extract table name
            string tableName = GetTableName(node);
            tableName = _memoryManager.InternString(tableName);
            string alias = node.Alias?.Value ?? "";
            alias = _memoryManager.InternString(alias);
            
            // Register the table
            int tableId = _graph.AddTableNode(tableName, "Table");
            
            // Add to context tables
            ctx.AddTable(tableName, tableId);
            
            // Handle alias
            if (!string.IsNullOrEmpty(alias) && alias != tableName)
            {
                ctx.AddTableAlias(alias, tableName);
            }
            
            _performanceTracker.IncrementCounter("Lineage", "TableReferences");
            
            base.ExplicitVisit(node);
        }
        
        public override void ExplicitVisit(CommonTableExpression node)
        {
            using var tracker = _performanceTracker.TrackOperation("LineageVisitor", "CTE");
            
            string cteName = node.ExpressionName?.Value ?? "";
            cteName = _memoryManager.InternString(cteName);
            
            if (!string.IsNullOrEmpty(cteName))
            {
                // Create a context for the CTE definition
                var cteContext = new LineageContext { 
                    Type = StatementType.Select,
                    OutputTableName = cteName
                };
                _context.Push(cteContext);
                
                try
                {
                    // Process the query
                    node.QueryExpression?.Accept(this);
                    
                    // Register the CTE table
                    int tableId = _graph.AddTableNode(cteName, "CTE");
                    
                    // Add CTE to parent context
                    var parentCtx = _context.ElementAt(1);
                    parentCtx.AddTable(cteName, tableId);
                    
                    // Register CTE columns
                    foreach (var columnId in cteContext.TargetColumns)
                    {
                        _graph.AddColumnToTable(tableId, columnId);
                    }
                    
                    _performanceTracker.IncrementCounter("Lineage", "CTEsDefined");
                }
                finally
                {
                    _context.Pop();
                }
            }
            
            base.ExplicitVisit(node);
        }
        
        #endregion
        
        #region Helper Methods
        
        private void ProcessMergeUpdate(MergeUpdateClause updateClause, string targetTableName)
        {
            var ctx = _context.Peek();
            
            if (updateClause.SetClauses != null)
            {
                foreach (var setClause in updateClause.SetClauses)
                {
                    if (setClause is AssignmentSetClause assignment)
                    {
                        // Target column
                        string colName = GetColumnName(assignment.Column);
                        colName = _memoryManager.InternString(colName);
                        int targetColumnId = _graph.AddColumnNode(colName, targetTableName);
                        ctx.TargetColumns.Add(targetColumnId);
                        
                        // Process expression (source columns)
                        ctx.SourceColumns.Clear();
                        ctx.ProcessingSourceColumns = true;
                        assignment.NewValue?.Accept(this);
                        ctx.ProcessingSourceColumns = false;
                        
                        // Create lineage for this specific assignment
                        foreach (var source in ctx.SourceColumns)
                        {
                            _graph.AddDirectLineage(source, targetColumnId, "MERGE UPDATE");
                            _performanceTracker.IncrementCounter("Lineage", "MergeUpdateRelationships");
                        }
                    }
                }
            }
        }
        
        private void ProcessMergeInsert(MergeInsertClause insertClause, string targetTableName)
        {
            var ctx = _context.Peek();
            ctx.TargetColumns.Clear();
            ctx.SourceColumns.Clear();
            
            // Process column list (target columns)
            if (insertClause.Columns != null)
            {
                foreach (var column in insertClause.Columns)
                {
                    if (column is ColumnReferenceExpression colRef)
                    {
                        string colName = GetColumnName(colRef);
                        colName = _memoryManager.InternString(colName);
                        int columnId = _graph.AddColumnNode(colName, targetTableName);
                        ctx.TargetColumns.Add(columnId);
                    }
                }
            }
            
            // Process source values
            if (insertClause.Source != null)
            {
                ctx.ProcessingSourceColumns = true;
                insertClause.Source.Accept(this);
                ctx.ProcessingSourceColumns = false;
                
                // Create lineage
                foreach (var target in ctx.TargetColumns)
                {
                    foreach (var source in ctx.SourceColumns)
                    {
                        _graph.AddDirectLineage(source, target, "MERGE INSERT");
                    }
                }
                
                _performanceTracker.IncrementCounter("Lineage", "MergeInsertRelationships",
                    ctx.TargetColumns.Count * ctx.SourceColumns.Count);
            }
        }
        
        private void HandleCTEs(WithCtesAndXmlNamespaces ctes)
        {
            if (ctes.CommonTableExpressions != null)
            {
                foreach (var cte in ctes.CommonTableExpressions)
                {
                    cte.Accept(this);
                }
            }
        }
        
        private string GetTableName(SchemaObjectName objectName)
        {
            if (objectName == null) return string.Empty;
            
            // Get the base identifier (table name)
            string tableName = objectName.BaseIdentifier?.Value ?? string.Empty;
            
            // Include schema if available for fully qualified name
            if (!string.IsNullOrEmpty(objectName.SchemaIdentifier?.Value))
            {
                tableName = $"{objectName.SchemaIdentifier.Value}.{tableName}";
            }
            
            return tableName;
        }
        
        private string GetTableName(NamedTableReference tableRef)
        {
            if (tableRef?.SchemaObject == null) return string.Empty;
            return GetTableName(tableRef.SchemaObject);
        }
        
        private string GetColumnName(ColumnReferenceExpression colRef)
        {
            if (colRef?.MultiPartIdentifier == null) return string.Empty;
            
            var identifiers = colRef.MultiPartIdentifier.Identifiers;
            if (identifiers.Count == 0) return string.Empty;
            
            // The last identifier is the column name
            return identifiers[identifiers.Count - 1].Value;
        }
        
        private string GetTableName(ColumnReferenceExpression colRef)
        {
            if (colRef?.MultiPartIdentifier == null) return string.Empty;
            
            var identifiers = colRef.MultiPartIdentifier.Identifiers;
            if (identifiers.Count <= 1) return string.Empty;
            
            // The second-to-last identifier is the table name
            return identifiers[identifiers.Count - 2].Value;
        }
        
        private string GetColumnName(MultiPartIdentifier identifier)
        {
            if (identifier == null) return string.Empty;
            
            var identifiers = identifier.Identifiers;
            if (identifiers.Count == 0) return string.Empty;
            
            // The last identifier is the column name
            return identifiers[identifiers.Count - 1].Value;
        }
    }
    
    #region Helper Classes
    
    /// <summary>
    /// Context for lineage analysis of a SQL statement
    /// </summary>
    internal class LineageContext
    {
        // General properties
        public StatementType Type { get; set; }
        public bool IsSubquery { get; set; }
        public string CurrentTable { get; set; } = string.Empty;
        public string OutputTableName { get; set; } = string.Empty;
        
        // Context flags
        public bool ProcessingSelectList { get; set; }
        public bool ProcessingFromClause { get; set; }
        public bool ProcessingWhereClause { get; set; }
        public bool ProcessingSourceColumns { get; set; }
        public bool ProcessingTargetColumns { get; set; }
        
        // Column tracking
        public HashSet<int> SourceColumns { get; } = new();
        public HashSet<int> TargetColumns { get; } = new();
        
        // Table tracking 
        private readonly Dictionary<string, int> _tables = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, string> _tableAliases = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, string> _columnToTable = new(StringComparer.OrdinalIgnoreCase);
        
        // Context ID for unique naming
        private static int _nextContextId = 1;
        private readonly int _contextId = _nextContextId++;
        
        public int GetContextId() => _contextId;
        
        public void AddTable(string tableName, int tableId)
        {
            _tables[tableName] = tableId;
        }
        
        public void AddTableAlias(string alias, string tableName)
        {
            _tableAliases[alias] = tableName;
        }
        
        public string ResolveTableAlias(string nameOrAlias)
        {
            if (_tableAliases.TryGetValue(nameOrAlias, out var actualName))
                return actualName;
                
            return nameOrAlias;
        }
        
        public string GetTableContext(string columnName)
        {
            // Check if we already know which table this column belongs to
            if (_columnToTable.TryGetValue(columnName, out var tableName))
                return tableName;
                
            // If we have a single table in context, use that
            if (_tables.Count == 1)
                return _tables.Keys.First();
                
            // Default to current table
            return CurrentTable;
        }
        
        public HashSet<int> GetAllColumnsFromTable(string tableName)
        {
            // This would normally query from metadata, but for this implementation
            // we'll return an empty set as we don't have full metadata integration
            return new HashSet<int>();
        }
    }
    
    internal enum StatementType
    {
        Unknown,
        Select,
        Insert,
        Update,
        Delete,
        Merge,
        Create,
        Alter,
        Drop
    }
    
    #endregion
}
#endregion
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Engine.Parsing.Models;
using TSqlColumnLineage.Core.Infrastructure.Memory;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// Extracts table and column references from T-SQL fragments.
    /// Implements memory-efficient extraction using data-oriented design.
    /// </summary>
    public static class ReferenceExtractor
    {
        /// <summary>
        /// Extracts table references from a SQL fragment
        /// </summary>
        public static List<TableReference> ExtractTableReferences(TSqlFragment fragment, ParsingOptions options)
        {
            // Memory-optimized implementation
            var visitor = MemoryManager.Instance.GetOrCreateObjectPool<TableReferenceVisitor>(
                () => new TableReferenceVisitor(),
                visitor => visitor.Reset()).Get();

            try
            {
                visitor.Options = options;
                fragment.Accept(visitor);
                return visitor.TableReferences;
            }
            finally
            {
                // Return visitor to pool
                MemoryManager.Instance.GetOrCreateObjectPool<TableReferenceVisitor>(
                    () => new TableReferenceVisitor(),
                    visitor => visitor.Reset()).Return(visitor);
            }
        }

        /// <summary>
        /// Extracts column references from a SQL fragment
        /// </summary>
        public static List<ColumnReference> ExtractColumnReferences(TSqlFragment fragment, ParsingOptions options, List<TableReference> tableReferences = null)
        {
            // Memory-optimized implementation
            var visitor = MemoryManager.Instance.GetOrCreateObjectPool<ColumnReferenceVisitor>(
                () => new ColumnReferenceVisitor(),
                visitor => visitor.Reset()).Get();

            try
            {
                visitor.Options = options;
                visitor.TableReferences = tableReferences;
                fragment.Accept(visitor);
                return visitor.ColumnReferences;
            }
            finally
            {
                // Return visitor to pool
                MemoryManager.Instance.GetOrCreateObjectPool<ColumnReferenceVisitor>(
                    () => new ColumnReferenceVisitor(),
                    visitor => visitor.Reset()).Return(visitor);
            }
        }

        /// <summary>
        /// Determines the SQL fragment type based on TSqlFragment
        /// </summary>
        public static SqlFragmentType DetermineFragmentType(TSqlFragment fragment)
        {
            return fragment switch
            {
                TSqlScript => SqlFragmentType.Batch,
                TSqlBatch => SqlFragmentType.Batch,
                SelectStatement => SqlFragmentType.Select,
                InsertStatement => SqlFragmentType.Insert,
                UpdateStatement => SqlFragmentType.Update,
                DeleteStatement => SqlFragmentType.Delete,
                MergeStatement => SqlFragmentType.Merge,
                CreateTableStatement => SqlFragmentType.Create,
                CreateViewStatement => SqlFragmentType.View,
                CreateProcedureStatement => SqlFragmentType.Procedure,
                CreateFunctionStatement => SqlFragmentType.Function,
                CreateTriggerStatement => SqlFragmentType.Trigger,
                AlterTableStatement => SqlFragmentType.Alter,
                AlterViewStatement => SqlFragmentType.View,
                AlterProcedureStatement => SqlFragmentType.Procedure,
                AlterFunctionStatement => SqlFragmentType.Function,
                AlterTriggerStatement => SqlFragmentType.Trigger,
                DropTableStatement => SqlFragmentType.Drop,
                DropViewStatement => SqlFragmentType.View,
                DropProcedureStatement => SqlFragmentType.Procedure,
                DropFunctionStatement => SqlFragmentType.Function,
                DropTriggerStatement => SqlFragmentType.Trigger,
                DeclareTableStatement => SqlFragmentType.Declare,
                DeclareVariableStatement => SqlFragmentType.Declare,
                SetVariableStatement => SqlFragmentType.Set,
                IfStatement => SqlFragmentType.If,
                WhileStatement => SqlFragmentType.While,
                BeginEndBlockStatement => SqlFragmentType.Begin,
                TryCatchStatement => SqlFragmentType.Try,
                ExecuteStatement => SqlFragmentType.Execute,
                CommonTableExpression => SqlFragmentType.CTE,
                CursorStatement => SqlFragmentType.Other,
                SubqueryExpression => SqlFragmentType.Subquery,
                QueryExpression => SqlFragmentType.Expression,
                BooleanExpression => SqlFragmentType.Expression,
                CaseExpression => SqlFragmentType.Case,
                QualifiedJoin => SqlFragmentType.Join,
                CommentStatement => SqlFragmentType.Comment,
                WhitespaceToken => SqlFragmentType.Whitespace,
                _ => SqlFragmentType.Unknown
            };
        }

        #region Visitor Classes

        /// <summary>
        /// Visitor for extracting table references
        /// </summary>
        private class TableReferenceVisitor : TSqlFragmentVisitor
        {
            public List<TableReference> TableReferences { get; private set; } = new List<TableReference>();
            public ParsingOptions Options { get; set; }

            public override void ExplicitVisit(NamedTableReference node)
            {
                if (node.SchemaObject != null)
                {
                    var reference = new TableReference
                    {
                        TableName = node.SchemaObject.BaseIdentifier?.Value ?? string.Empty,
                        SchemaName = node.SchemaObject.SchemaIdentifier?.Value ?? string.Empty,
                        DatabaseName = node.SchemaObject.DatabaseIdentifier?.Value ?? string.Empty,
                        Alias = node.Alias?.Value ?? string.Empty,
                        ReferenceType = TableReferenceType.Table,
                        StartOffset = node.StartOffset,
                        EndOffset = node.EndOffset
                    };

                    // Skip if table name is empty
                    if (!string.IsNullOrEmpty(reference.TableName))
                    {
                        TableReferences.Add(reference);
                    }
                }

                base.ExplicitVisit(node);
            }

            public override void ExplicitVisit(QualifiedJoin node)
            {
                if (node.FirstTableReference != null)
                {
                    node.FirstTableReference.Accept(this);
                }

                if (node.SecondTableReference != null)
                {
                    node.SecondTableReference.Accept(this);
                }

                base.ExplicitVisit(node);
            }

            public override void ExplicitVisit(TableValuedFunctionReference node)
            {
                if (node.SchemaObject != null)
                {
                    var reference = new TableReference
                    {
                        TableName = node.SchemaObject.BaseIdentifier?.Value ?? string.Empty,
                        SchemaName = node.SchemaObject.SchemaIdentifier?.Value ?? string.Empty,
                        DatabaseName = node.SchemaObject.DatabaseIdentifier?.Value ?? string.Empty,
                        Alias = node.Alias?.Value ?? string.Empty,
                        ReferenceType = TableReferenceType.Function,
                        StartOffset = node.StartOffset,
                        EndOffset = node.EndOffset
                    };

                    // Skip if table name is empty
                    if (!string.IsNullOrEmpty(reference.TableName))
                    {
                        TableReferences.Add(reference);
                    }
                }

                base.ExplicitVisit(node);
            }

            public override void ExplicitVisit(DerivedTable node)
            {
                if (!string.IsNullOrEmpty(node.Alias?.Value))
                {
                    var reference = new TableReference
                    {
                        TableName = node.Alias.Value,
                        ReferenceType = TableReferenceType.Derived,
                        StartOffset = node.StartOffset,
                        EndOffset = node.EndOffset
                    };

                    TableReferences.Add(reference);
                }

                base.ExplicitVisit(node);
            }

            public override void ExplicitVisit(CommonTableExpression node)
            {
                if (node.ExpressionName != null)
                {
                    var reference = new TableReference
                    {
                        TableName = node.ExpressionName.Value,
                        ReferenceType = TableReferenceType.CTE,
                        StartOffset = node.StartOffset,
                        EndOffset = node.EndOffset
                    };

                    TableReferences.Add(reference);
                }

                base.ExplicitVisit(node);
            }

            public override void ExplicitVisit(TemporaryTableReference node)
            {
                var reference = new TableReference
                {
                    TableName = node.Name.Value,
                    Alias = node.Alias?.Value ?? string.Empty,
                    ReferenceType = TableReferenceType.TemporaryTable,
                    StartOffset = node.StartOffset,
                    EndOffset = node.EndOffset
                };

                TableReferences.Add(reference);

                base.ExplicitVisit(node);
            }

            public override void ExplicitVisit(TableVariableReference node)
            {
                var reference = new TableReference
                {
                    TableName = node.Name.Value,
                    Alias = node.Alias?.Value ?? string.Empty,
                    ReferenceType = TableReferenceType.Variable,
                    StartOffset = node.StartOffset,
                    EndOffset = node.EndOffset
                };

                TableReferences.Add(reference);

                base.ExplicitVisit(node);
            }

            /// <summary>
            /// Resets the visitor for reuse
            /// </summary>
            public void Reset()
            {
                TableReferences.Clear();
                Options = null;
            }
        }

        /// <summary>
        /// Visitor for extracting column references
        /// </summary>
        private class ColumnReferenceVisitor : TSqlFragmentVisitor
        {
            public List<ColumnReference> ColumnReferences { get; private set; } = new List<ColumnReference>();
            public List<TableReference> TableReferences { get; set; }
            public ParsingOptions Options { get; set; }

            // Track contexts for source/target determination
            private readonly Stack<Context> _contexts = new Stack<Context>();
            private bool _inSelectList;
            private bool _inWhereClause;
            private bool _inGroupByClause;
            private bool _inOrderByClause;
            private bool _inJoinCondition;
            private bool _inInsertColumnList;
            private bool _inUpdateSetClause;

            private enum Context
            {
                Unknown,
                Select,
                Insert,
                Update,
                Delete,
                Merge,
                Expression
            }

            public override void ExplicitVisit(TSqlScript node)
            {
                _contexts.Push(Context.Unknown);
                base.ExplicitVisit(node);
                _contexts.Pop();
            }

            public override void ExplicitVisit(TSqlBatch node)
            {
                _contexts.Push(Context.Unknown);
                base.ExplicitVisit(node);
                _contexts.Pop();
            }

            public override void ExplicitVisit(SelectStatement node)
            {
                _contexts.Push(Context.Select);
                base.ExplicitVisit(node);
                _contexts.Pop();
            }

            public override void ExplicitVisit(InsertStatement node)
            {
                _contexts.Push(Context.Insert);
                base.ExplicitVisit(node);
                _contexts.Pop();
            }

            public override void ExplicitVisit(UpdateStatement node)
            {
                _contexts.Push(Context.Update);
                base.ExplicitVisit(node);
                _contexts.Pop();
            }

            public override void ExplicitVisit(DeleteStatement node)
            {
                _contexts.Push(Context.Delete);
                base.ExplicitVisit(node);
                _contexts.Pop();
            }

            public override void ExplicitVisit(MergeStatement node)
            {
                _contexts.Push(Context.Merge);
                base.ExplicitVisit(node);
                _contexts.Pop();
            }

            public override void ExplicitVisit(QueryExpression node)
            {
                _contexts.Push(Context.Expression);
                base.ExplicitVisit(node);
                _contexts.Pop();
            }

            public override void ExplicitVisit(SelectElement node)
            {
                bool oldInSelectList = _inSelectList;
                _inSelectList = true;
                base.ExplicitVisit(node);
                _inSelectList = oldInSelectList;
            }

            public override void ExplicitVisit(WhereClause node)
            {
                bool oldInWhereClause = _inWhereClause;
                _inWhereClause = true;
                base.ExplicitVisit(node);
                _inWhereClause = oldInWhereClause;
            }

            public override void ExplicitVisit(GroupByClause node)
            {
                bool oldInGroupByClause = _inGroupByClause;
                _inGroupByClause = true;
                base.ExplicitVisit(node);
                _inGroupByClause = oldInGroupByClause;
            }

            public override void ExplicitVisit(OrderByClause node)
            {
                bool oldInOrderByClause = _inOrderByClause;
                _inOrderByClause = true;
                base.ExplicitVisit(node);
                _inOrderByClause = oldInOrderByClause;
            }

            public override void ExplicitVisit(QualifiedJoin node)
            {
                // Process first table reference
                if (node.FirstTableReference != null)
                {
                    node.FirstTableReference.Accept(this);
                }

                // Process second table reference
                if (node.SecondTableReference != null)
                {
                    node.SecondTableReference.Accept(this);
                }

                // Process join condition with context
                if (node.SearchCondition != null)
                {
                    bool oldInJoinCondition = _inJoinCondition;
                    _inJoinCondition = true;
                    node.SearchCondition.Accept(this);
                    _inJoinCondition = oldInJoinCondition;
                }
            }

            public override void ExplicitVisit(InsertSpecification node)
            {
                // Process target
                if (node.Target != null)
                {
                    node.Target.Accept(this);
                }

                // Process column list with context
                if (node.Columns != null && node.Columns.Count > 0)
                {
                    bool oldInInsertColumnList = _inInsertColumnList;
                    _inInsertColumnList = true;
                    foreach (var column in node.Columns)
                    {
                        column.Accept(this);
                    }
                    _inInsertColumnList = oldInInsertColumnList;
                }

                // Process source
                if (node.InsertSource != null)
                {
                    node.InsertSource.Accept(this);
                }
            }

            public override void ExplicitVisit(UpdateSpecification node)
            {
                // Process target
                if (node.Target != null)
                {
                    node.Target.Accept(this);
                }

                // Process set clauses with context
                if (node.SetClauses != null && node.SetClauses.Count > 0)
                {
                    bool oldInUpdateSetClause = _inUpdateSetClause;
                    _inUpdateSetClause = true;
                    foreach (var setClause in node.SetClauses)
                    {
                        setClause.Accept(this);
                    }
                    _inUpdateSetClause = oldInUpdateSetClause;
                }

                // Process from clause
                if (node.FromClause != null)
                {
                    node.FromClause.Accept(this);
                }

                // Process where clause
                if (node.WhereClause != null)
                {
                    node.WhereClause.Accept(this);
                }
            }

            public override void ExplicitVisit(ColumnReferenceExpression node)
            {
                if (node.MultiPartIdentifier != null)
                {
                    var parts = node.MultiPartIdentifier.Identifiers;
                    if (parts.Count > 0)
                    {
                        string columnName = parts[parts.Count - 1].Value;
                        string tableName = parts.Count > 1 ? parts[parts.Count - 2].Value : string.Empty;
                        string schemaName = parts.Count > 2 ? parts[parts.Count - 3].Value : string.Empty;
                        string databaseName = parts.Count > 3 ? parts[parts.Count - 4].Value : string.Empty;
                        string serverName = parts.Count > 4 ? parts[parts.Count - 5].Value : string.Empty;

                        // Match with table references if available
                        if (!string.IsNullOrEmpty(tableName) && TableReferences != null)
                        {
                            var tableRef = TableReferences.FirstOrDefault(t => 
                                string.Equals(t.Alias, tableName, StringComparison.OrdinalIgnoreCase) ||
                                string.Equals(t.TableName, tableName, StringComparison.OrdinalIgnoreCase));

                            if (tableRef != null && !string.Equals(tableRef.Alias, tableName, StringComparison.OrdinalIgnoreCase))
                            {
                                // We found the table by name, not alias
                                tableName = tableRef.TableName;
                            }
                        }

                        var columnRef = new ColumnReference
                        {
                            ColumnName = columnName,
                            TableName = tableName,
                            SchemaName = schemaName,
                            DatabaseName = databaseName,
                            ServerName = serverName,
                            ReferenceType = ColumnReferenceType.Regular,
                            StartOffset = node.StartOffset,
                            EndOffset = node.EndOffset
                        };

                        // Determine source/target based on context
                        DetermineSourceTarget(columnRef);

                        ColumnReferences.Add(columnRef);
                    }
                }

                base.ExplicitVisit(node);
            }

            public override void ExplicitVisit(SelectSetVariable node)
            {
                if (node.Variable != null && node.Variable.Name != null)
                {
                    var columnRef = new ColumnReference
                    {
                        ColumnName = node.Variable.Name,
                        ReferenceType = ColumnReferenceType.Variable,
                        IsTarget = true,
                        StartOffset = node.StartOffset,
                        EndOffset = node.EndOffset
                    };

                    ColumnReferences.Add(columnRef);
                }

                if (node.Expression != null)
                {
                    node.Expression.Accept(this);
                }
            }

            public override void ExplicitVisit(SelectScalarExpression node)
            {
                if (node.Expression != null)
                {
                    node.Expression.Accept(this);
                }

                if (node.ColumnName != null)
                {
                    var columnRef = new ColumnReference
                    {
                        ColumnName = node.ColumnName.Value,
                        Alias = node.ColumnName.Value,
                        ReferenceType = ColumnReferenceType.Alias,
                        IsTarget = _contexts.Peek() == Context.Select,
                        StartOffset = node.ColumnName.StartOffset,
                        EndOffset = node.ColumnName.EndOffset
                    };

                    ColumnReferences.Add(columnRef);
                }
            }

            public override void ExplicitVisit(StarColumnReference node)
            {
                var columnRef = new ColumnReference
                {
                    ColumnName = "*",
                    TableName = node.TableName?.Value ?? string.Empty,
                    ReferenceType = ColumnReferenceType.Wildcard,
                    IsSource = true,
                    StartOffset = node.StartOffset,
                    EndOffset = node.EndOffset
                };

                ColumnReferences.Add(columnRef);
            }

            public override void ExplicitVisit(AssignmentSetClause node)
            {
                if (node.Column != null && node.Column.MultiPartIdentifier != null)
                {
                    var parts = node.Column.MultiPartIdentifier.Identifiers;
                    if (parts.Count > 0)
                    {
                        string columnName = parts[parts.Count - 1].Value;
                        string tableName = parts.Count > 1 ? parts[parts.Count - 2].Value : string.Empty;

                        var columnRef = new ColumnReference
                        {
                            ColumnName = columnName,
                            TableName = tableName,
                            ReferenceType = ColumnReferenceType.Regular,
                            IsTarget = true,
                            StartOffset = node.Column.StartOffset,
                            EndOffset = node.Column.EndOffset
                        };

                        ColumnReferences.Add(columnRef);
                    }
                }

                // Process the assignment value (source columns)
                if (node.NewValue != null)
                {
                    node.NewValue.Accept(this);
                }
            }

            /// <summary>
            /// Determines if a column is a source or target based on context
            /// </summary>
            private void DetermineSourceTarget(ColumnReference columnRef)
            {
                Context currentContext = _contexts.Count > 0 ? _contexts.Peek() : Context.Unknown;

                // Default is source
                columnRef.IsSource = true;

                switch (currentContext)
                {
                    case Context.Select:
                        // In select list, could be both source and target
                        if (_inSelectList)
                        {
                            columnRef.IsSource = true;
                            columnRef.IsTarget = false;
                        }
                        // In where/group by/order by, it's a source
                        else if (_inWhereClause || _inGroupByClause || _inOrderByClause)
                        {
                            columnRef.IsSource = true;
                            columnRef.IsTarget = false;
                        }
                        // In a join condition, it's a source
                        else if (_inJoinCondition)
                        {
                            columnRef.IsSource = true;
                            columnRef.IsTarget = false;
                        }
                        break;

                    case Context.Insert:
                        // In column list, it's a target
                        if (_inInsertColumnList)
                        {
                            columnRef.IsSource = false;
                            columnRef.IsTarget = true;
                        }
                        else
                        {
                            // Otherwise, it's a source
                            columnRef.IsSource = true;
                            columnRef.IsTarget = false;
                        }
                        break;

                    case Context.Update:
                        // In set clause left side, it's a target
                        if (_inUpdateSetClause)
                        {
                            columnRef.IsSource = false;
                            columnRef.IsTarget = true;
                        }
                        // In where clause, it's a source
                        else if (_inWhereClause)
                        {
                            columnRef.IsSource = true;
                            columnRef.IsTarget = false;
                        }
                        break;

                    case Context.Delete:
                        // Always a source in delete
                        columnRef.IsSource = true;
                        columnRef.IsTarget = false;
                        break;

                    case Context.Merge:
                        // Complex, would need more context tracking
                        columnRef.IsSource = true;
                        columnRef.IsTarget = false;
                        break;

                    default:
                        // Default is source
                        columnRef.IsSource = true;
                        columnRef.IsTarget = false;
                        break;
                }
            }

            /// <summary>
            /// Resets the visitor for reuse
            /// </summary>
            public void Reset()
            {
                ColumnReferences.Clear();
                TableReferences = null;
                Options = null;
                _contexts.Clear();
                _inSelectList = false;
                _inWhereClause = false;
                _inGroupByClause = false;
                _inOrderByClause = false;
                _inJoinCondition = false;
                _inInsertColumnList = false;
                _inUpdateSetClause = false;
            }
        }
        #endregion
    }
}
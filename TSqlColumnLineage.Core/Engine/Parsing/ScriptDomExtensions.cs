using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Domain;
using TSqlColumnLineage.Core.Domain.Graph;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// Extension methods to integrate ScriptDom with column lineage tracking
    /// </summary>
    public static class ScriptDomExtensions
    {
        /// <summary>
        /// Extracts column lineage from a T-SQL fragment
        /// </summary>
        public static LineageGraph ExtractLineage(this TSqlFragment fragment)
        {
            if (fragment == null)
                throw new ArgumentNullException(nameof(fragment));
                
            var graph = new LineageGraph();
            var visitor = new LineageVisitor(graph);
            fragment.Accept(visitor);
            
            return graph;
        }
        
        /// <summary>
        /// Gets all table references from a T-SQL fragment
        /// </summary>
        public static IEnumerable<NamedTableReference> GetTableReferences(this TSqlFragment fragment)
        {
            var visitor = new TableReferenceVisitor();
            fragment.Accept(visitor);
            return visitor.TableReferences;
        }
        
        /// <summary>
        /// Gets all column references from a T-SQL fragment
        /// </summary>
        public static IEnumerable<ColumnReferenceExpression> GetColumnReferences(this TSqlFragment fragment)
        {
            var visitor = new ColumnReferenceVisitor();
            fragment.Accept(visitor);
            return visitor.ColumnReferences;
        }
        
        /// <summary>
        /// Gets all SQL operations from a fragment
        /// </summary>
        public static List<SqlOperation> ExtractSqlOperations(this TSqlFragment fragment)
        {
            var visitor = new OperationVisitor();
            fragment.Accept(visitor);
            return visitor.Operations;
        }
        
        /// <summary>
        /// Gets a SQL operation type for a TSqlFragment
        /// </summary>
        public static SqlOperationType GetSqlOperationType(this TSqlFragment fragment)
        {
            return fragment switch
            {
                SelectStatement => SqlOperationType.Select,
                InsertStatement => SqlOperationType.Insert,
                UpdateStatement => SqlOperationType.Update,
                DeleteStatement => SqlOperationType.Delete,
                MergeStatement => SqlOperationType.Merge,
                CommonTableExpression => SqlOperationType.Cte,
                OrderByClause => SqlOperationType.OrderBy,
                GroupByClause => SqlOperationType.GroupBy,
                QualifiedJoin => SqlOperationType.Join,
                Subquery or QuerySpecification => SqlOperationType.SubQuery,
                UnionSpecification => SqlOperationType.Union,
                ExceptSpecification => SqlOperationType.Except,
                IntersectSpecification => SqlOperationType.Intersect,
                PivotClause => SqlOperationType.Pivot,
                UnpivotClause => SqlOperationType.Unpivot,
                CaseExpression => SqlOperationType.Case,
                FunctionCall => SqlOperationType.Function,
                ScalarExpression => SqlOperationType.Expression,
                ParameterReference => SqlOperationType.Parameter,
                VariableReference => SqlOperationType.Variable,
                _ => SqlOperationType.Unknown
            };
        }
        
        #region ScriptDom Utility Extensions
        
        /// <summary>
        /// Gets the fully qualified name from schema object name
        /// </summary>
        public static string GetFullyQualifiedName(this SchemaObjectName name)
        {
            if (name == null)
                return string.Empty;
                
            var parts = new List<string>();
            
            if (name.ServerIdentifier != null)
                parts.Add(name.ServerIdentifier.Value);
                
            if (name.DatabaseIdentifier != null)
                parts.Add(name.DatabaseIdentifier.Value);
                
            if (name.SchemaIdentifier != null)
                parts.Add(name.SchemaIdentifier.Value);
                
            if (name.BaseIdentifier != null)
                parts.Add(name.BaseIdentifier.Value);
                
            return string.Join(".", parts);
        }
        
        /// <summary>
        /// Gets the simple name of a schema object
        /// </summary>
        public static string GetSimpleName(this SchemaObjectName name)
        {
            return name?.BaseIdentifier?.Value ?? string.Empty;
        }
        
        /// <summary>
        /// Gets the column name from a multi-part identifier
        /// </summary>
        public static string GetColumnName(this MultiPartIdentifier identifier)
        {
            if (identifier?.Identifiers == null || identifier.Identifiers.Count == 0)
                return string.Empty;
                
            return identifier.Identifiers.Last().Value;
        }
        
        /// <summary>
        /// Gets the table name from a multi-part identifier
        /// </summary>
        public static string GetTableName(this MultiPartIdentifier identifier)
        {
            if (identifier?.Identifiers == null || identifier.Identifiers.Count <= 1)
                return string.Empty;
                
            return identifier.Identifiers[identifier.Identifiers.Count - 2].Value;
        }
        
        /// <summary>
        /// Gets the database name from a multi-part identifier
        /// </summary>
        public static string GetDatabaseName(this MultiPartIdentifier identifier)
        {
            if (identifier?.Identifiers == null || identifier.Identifiers.Count <= 3)
                return string.Empty;
                
            return identifier.Identifiers[identifier.Identifiers.Count - 4].Value;
        }
        
        /// <summary>
        /// Gets the schema name from a multi-part identifier
        /// </summary>
        public static string GetSchemaName(this MultiPartIdentifier identifier)
        {
            if (identifier?.Identifiers == null || identifier.Identifiers.Count <= 2)
                return string.Empty;
                
            return identifier.Identifiers[identifier.Identifiers.Count - 3].Value;
        }
        
        #endregion
        
        #region Helper Visitors
        
        /// <summary>
        /// Visitor that collects all table references
        /// </summary>
        private class TableReferenceVisitor : TSqlFragmentVisitor
        {
            public List<NamedTableReference> TableReferences { get; } = new List<NamedTableReference>();
            
            public override void ExplicitVisit(NamedTableReference node)
            {
                TableReferences.Add(node);
                base.ExplicitVisit(node);
            }
        }
        
        /// <summary>
        /// Visitor that collects all column references
        /// </summary>
        private class ColumnReferenceVisitor : TSqlFragmentVisitor
        {
            public List<ColumnReferenceExpression> ColumnReferences { get; } = new List<ColumnReferenceExpression>();
            
            public override void ExplicitVisit(ColumnReferenceExpression node)
            {
                ColumnReferences.Add(node);
                base.ExplicitVisit(node);
            }
        }
        
        /// <summary>
        /// Visitor that extracts SQL operations
        /// </summary>
        private class OperationVisitor : TSqlFragmentVisitor
        {
            public List<SqlOperation> Operations { get; } = new List<SqlOperation>();
            private int _nextId = 1;
            
            public override void ExplicitVisit(SelectStatement node)
            {
                AddOperation(node, "SELECT");
                base.ExplicitVisit(node);
            }
            
            public override void ExplicitVisit(InsertStatement node)
            {
                AddOperation(node, "INSERT");
                base.ExplicitVisit(node);
            }
            
            public override void ExplicitVisit(UpdateStatement node)
            {
                AddOperation(node, "UPDATE");
                base.ExplicitVisit(node);
            }
            
            public override void ExplicitVisit(DeleteStatement node)
            {
                AddOperation(node, "DELETE");
                base.ExplicitVisit(node);
            }
            
            public override void ExplicitVisit(MergeStatement node)
            {
                AddOperation(node, "MERGE");
                base.ExplicitVisit(node);
            }
            
            private void AddOperation(TSqlFragment fragment, string name)
            {
                var operation = new SqlOperation(
                    _nextId++,
                    fragment.GetSqlOperationType(),
                    name,
                    fragment.GetType().Name,
                    $"{fragment.StartLine}:{fragment.StartColumn}"
                );
                
                Operations.Add(operation);
            }
        }
        
        #endregion
    }
}
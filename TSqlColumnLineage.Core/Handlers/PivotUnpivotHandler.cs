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
    /// Handler for PIVOT and UNPIVOT operations
    /// </summary>
    public class PivotUnpivotHandler
    {
        private readonly ColumnLineageVisitor _visitor;
        private readonly LineageGraph _graph;
        private readonly LineageContext _context;
        private readonly ILogger _logger;

        public PivotUnpivotHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger logger)
        {
            _visitor = visitor ?? throw new ArgumentNullException(nameof(visitor));
            _graph = graph ?? throw new ArgumentNullException(nameof(graph));
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _logger = logger;
        }

        /// <summary>
        /// Process a PIVOT table reference
        /// This method is version-agnostic to work with different ScriptDom versions
        /// </summary>
        public void ProcessPivotTableReference(PivotedTableReference node)
        {
            _logger?.LogDebug($"Processing PIVOT at Line {node.StartLine}");

            // Extract the pivot clause using reflection (version-agnostic)
            var pivotClause = GetPivotClause(node);
            if (pivotClause == null)
            {
                _logger?.LogDebug("Could not extract PIVOT clause");
                return;
            }

            // Get the source table
            var tableReference = GetTableReference(node);
            if (tableReference == null)
            {
                _logger?.LogDebug("Could not extract source table reference");
                return;
            }

            // Process the underlying table reference first
            _visitor.Visit(tableReference);

            // Process aggregate function
            var aggregateFunction = GetAggregateFunction(pivotClause);
            if (aggregateFunction != null)
            {
                _logger?.LogDebug($"Processing PIVOT aggregate function: {aggregateFunction.GetType().Name}");
                _visitor.Visit(aggregateFunction);
            }

            // Process pivot column (column used to create new columns)
            var pivotColumns = GetPivotColumns(pivotClause);
            foreach (var pivotColumn in pivotColumns)
            {
                _logger?.LogDebug($"Processing PIVOT column: {(pivotColumn is ColumnReferenceExpression cre ? cre.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value : "Unknown")}");
                _visitor.Visit(pivotColumn);
            }

            // Process value columns (columns to pivot by)
            var valueColumns = GetInColumns(pivotClause);
            foreach (var valueColumn in valueColumns)
            {
                _logger?.LogDebug($"Processing PIVOT value: {(valueColumn is StringLiteral lit ? lit.Value : "Unknown")}");
                _visitor.Visit(valueColumn);
            }
        }

        /// <summary>
        /// Process an UNPIVOT table reference
        /// This method is version-agnostic to work with different ScriptDom versions
        /// </summary>
        public void ProcessUnpivotTableReference(UnpivotedTableReference node)
        {
            _logger?.LogDebug($"Processing UNPIVOT at Line {node.StartLine}");

            // Extract the unpivot clause using reflection (version-agnostic)
            var unpivotClause = GetUnpivotClause(node);
            if (unpivotClause == null)
            {
                _logger?.LogDebug("Could not extract UNPIVOT clause");
                return;
            }

            // Get the source table
            var tableReference = GetTableReference(node);
            if (tableReference == null)
            {
                _logger?.LogDebug("Could not extract source table reference");
                return;
            }

            // Process the underlying table reference first
            _visitor.Visit(tableReference);

            // Process value column (the column that will receive the values)
            var valueColumn = GetValueColumn(unpivotClause);
            if (valueColumn != null)
            {
                _logger?.LogDebug($"Processing UNPIVOT value column: {(valueColumn is ColumnReferenceExpression cre ? cre.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value : "Unknown")}");
                _visitor.Visit(valueColumn);
            }

            // Process name column (the column that will receive the names)
            var nameColumn = GetNameColumn(unpivotClause);
            if (nameColumn != null)
            {
                _logger?.LogDebug($"Processing UNPIVOT name column: {(nameColumn is ColumnReferenceExpression cre ? cre.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value : "Unknown")}");
                _visitor.Visit(nameColumn);
            }

            // Process columns to unpivot
            var columnList = GetInColumns(unpivotClause);
            foreach (var column in columnList)
            {
                _logger?.LogDebug($"Processing UNPIVOT column: {(column is ColumnReferenceExpression cre ? cre.MultiPartIdentifier?.Identifiers.LastOrDefault()?.Value : "Unknown")}");
                _visitor.Visit(column);
            }
        }

        #region Reflection Helper Methods

        // Helper method to extract the PIVOT clause using reflection
        private object? GetPivotClause(PivotedTableReference node)
        {
            try
            {
                var property = node.GetType().GetProperty("PivotClause");
                if (property != null)
                {
                    return property.GetValue(node);
                }
                else
                {
                    // Try to find any property that might contain our pivot clause
                    foreach (var prop in node.GetType().GetProperties())
                    {
                        var value = prop.GetValue(node);
                        if (value != null && prop.Name.Contains("Pivot"))
                        {
                            return value;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"Error extracting PIVOT clause: {ex.Message}");
            }
            return null;
        }

        // Helper method to extract the UNPIVOT clause using reflection
        private object? GetUnpivotClause(UnpivotedTableReference node)
        {
            try
            {
                var property = node.GetType().GetProperty("UnpivotClause");
                if (property != null)
                {
                    return property.GetValue(node);
                }
                else
                {
                    // Try to find any property that might contain our unpivot clause
                    foreach (var prop in node.GetType().GetProperties())
                    {
                        var value = prop.GetValue(node);
                        if (value != null && prop.Name.Contains("Unpivot"))
                        {
                            return value;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"Error extracting UNPIVOT clause: {ex.Message}");
            }
            return null;
        }

        // Extract the source table reference
        private TableReference? GetTableReference(TableReferenceWithAlias node)
        {
            try
            {
                var property = node.GetType().GetProperty("TableReference");
                if (property != null)
                {
                    return property.GetValue(node) as TableReference;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"Error extracting table reference: {ex.Message}");
            }
            return null;
        }

        // Extract the aggregate function from a pivot clause
        private ScalarExpression? GetAggregateFunction(object pivotClause)
        {
            try
            {
                var property = pivotClause.GetType().GetProperty("AggregateFunction");
                if (property != null)
                {
                    return property.GetValue(pivotClause) as ScalarExpression;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"Error extracting aggregate function: {ex.Message}");
            }
            return null;
        }

        // Extract the pivot columns from a pivot clause
        private IEnumerable<ScalarExpression> GetPivotColumns(object pivotClause)
        {
            try
            {
                var property = pivotClause.GetType().GetProperty("PivotColumn") ??
                               pivotClause.GetType().GetProperty("PivotColumns");
                               
                if (property != null)
                {
                    var value = property.GetValue(pivotClause);
                    
                    if (value is IEnumerable<ScalarExpression> list)
                    {
                        return list;
                    }
                    else if (value is ScalarExpression expr)
                    {
                        return new[] { expr };
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"Error extracting pivot columns: {ex.Message}");
            }
            return Enumerable.Empty<ScalarExpression>();
        }

        // Extract the value column from an unpivot clause
        private ScalarExpression? GetValueColumn(object unpivotClause)
        {
            try
            {
                var property = unpivotClause.GetType().GetProperty("ValueColumn");
                if (property != null)
                {
                    return property.GetValue(unpivotClause) as ScalarExpression;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"Error extracting value column: {ex.Message}");
            }
            return null;
        }

        // Extract the name column from an unpivot clause
        private ScalarExpression? GetNameColumn(object unpivotClause)
        {
            try
            {
                var property = unpivotClause.GetType().GetProperty("NameColumn");
                if (property != null)
                {
                    return property.GetValue(unpivotClause) as ScalarExpression;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"Error extracting name column: {ex.Message}");
            }
            return null;
        }

        // Extract the IN columns from a pivot or unpivot clause
        private IEnumerable<ScalarExpression> GetInColumns(object clause)
        {
            try
            {
                var property = clause.GetType().GetProperty("InColumns") ??
                               clause.GetType().GetProperty("ValueColumns") ??
                               clause.GetType().GetProperty("Columns");
                               
                if (property != null)
                {
                    var value = property.GetValue(clause);
                    
                    if (value is IEnumerable<ScalarExpression> list)
                    {
                        return list;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"Error extracting IN columns: {ex.Message}");
            }
            return Enumerable.Empty<ScalarExpression>();
        }

        #endregion
    }
}

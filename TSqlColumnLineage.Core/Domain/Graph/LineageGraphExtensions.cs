using System;
using System.Collections.Generic;
using System.Linq;
using TSqlColumnLineage.Core.Infrastructure.Memory;

namespace TSqlColumnLineage.Core.Domain.Graph
{
    /// <summary>
    /// Extension methods for LineageGraph to handle SQL operations
    /// </summary>
    public static class LineageGraphExtensions
    {
        // Cache of recently created SQL operations
        private static readonly Dictionary<string, SqlOperation> _operationCache =
            new(StringComparer.OrdinalIgnoreCase);

        // Cache size limit
        private const int MaxCacheSize = 1000;

        /// <summary>
        /// Adds a SQL operation to the lineage graph
        /// </summary>
        public static SqlOperation AddSqlOperation(this LineageGraph graph,
            SqlOperationType type, string name, string sqlText = "", string sourceLocation = "")
        {
            ArgumentNullException.ThrowIfNull(graph);

            // Optimize memory
            name = MemoryManager.Instance.InternString(name ?? string.Empty);
            sourceLocation = MemoryManager.Instance.InternString(sourceLocation ?? string.Empty);

            // Create operation ID based on hash of operation details
            string operationKey = $"{type}:{name}:{sourceLocation}:{sqlText.GetHashCode()}";

            // Check cache first
            lock (_operationCache)
            {
                if (_operationCache.TryGetValue(operationKey, out var cachedOperation))
                {
                    return cachedOperation;
                }

                // Cleanup cache if needed
                if (_operationCache.Count >= MaxCacheSize)
                {
                    // Remove oldest entries
                    var oldestKeys = _operationCache.Keys.Take(MaxCacheSize / 4).ToList();
                    foreach (var key in oldestKeys)
                    {
                        _operationCache.Remove(key);
                    }
                }
            }

            // Generate unique ID for operation
            int operationId = graph.GetMetadata("OperationCount") as int? ?? 0;
            graph.SetMetadata("OperationCount", operationId + 1);

            // Create operation
            var operation = new SqlOperation(operationId, type, name, sqlText, sourceLocation);

            // Add to cache
            lock (_operationCache)
            {
                _operationCache[operationKey] = operation;
            }

            // Add operation to graph metadata
            graph.SetMetadata($"Operation:{operationId}", operation);

            return operation;
        }

        /// <summary>
        /// Links a SQL operation to source and target columns in the lineage graph
        /// </summary>
        public static void LinkOperation(this LineageGraph graph, SqlOperation operation,
            IEnumerable<int> sourceColumnIds, IEnumerable<int> targetColumnIds)
        {
            ArgumentNullException.ThrowIfNull(graph);

            ArgumentNullException.ThrowIfNull(operation);

            // Add sources and targets to operation
            if (sourceColumnIds != null)
            {
                foreach (var sourceId in sourceColumnIds)
                {
                    operation.AddSourceColumn(sourceId);
                }
            }

            if (targetColumnIds != null)
            {
                foreach (var targetId in targetColumnIds)
                {
                    operation.AddTargetColumn(targetId);
                }
            }

            // Create lineage edges between sources and targets
            if (sourceColumnIds != null && targetColumnIds != null)
            {
                foreach (var sourceId in sourceColumnIds)
                {
                    foreach (var targetId in targetColumnIds)
                    {
                        // Create lineage edge with operation type as the operation
                        graph.AddDirectLineage(
                            sourceId,
                            targetId,
                            operation.Type.ToString(),
                            operation.SqlText);
                    }
                }
            }
        }

        /// <summary>
        /// Processes a SQL SELECT operation in the lineage graph
        /// </summary>
        public static SqlOperation ProcessSelectOperation(this LineageGraph graph,
            string queryName, string sqlText, string sourceLocation,
            IEnumerable<(string SourceTable, string SourceColumn)> sourceCols,
            IEnumerable<(string TargetTable, string TargetColumn)> targetCols)
        {
            ArgumentNullException.ThrowIfNull(graph);

            // Create operation
            var operation = graph.AddSqlOperation(
                SqlOperationType.Select,
                queryName,
                sqlText,
                sourceLocation);

            // Get source column IDs
            var sourceIds = new List<int>();
            if (sourceCols != null)
            {
                foreach (var (sourceTable, sourceColumn) in sourceCols)
                {
                    int colId = graph.GetColumnNode(sourceTable, sourceColumn);
                    if (colId >= 0)
                    {
                        sourceIds.Add(colId);
                    }
                    else
                    {
                        // Create column if it doesn't exist
                        colId = graph.AddColumnNode(sourceColumn, sourceTable);
                        sourceIds.Add(colId);
                    }
                }
            }

            // Get target column IDs
            var targetIds = new List<int>();
            if (targetCols != null)
            {
                foreach (var (targetTable, targetColumn) in targetCols)
                {
                    int colId = graph.GetColumnNode(targetTable, targetColumn);
                    if (colId >= 0)
                    {
                        targetIds.Add(colId);
                    }
                    else
                    {
                        // Create column if it doesn't exist
                        colId = graph.AddColumnNode(targetColumn, targetTable);
                        targetIds.Add(colId);
                    }
                }
            }

            // Link operation in graph
            graph.LinkOperation(operation, sourceIds, targetIds);

            return operation;
        }

        /// <summary>
        /// Processes a SQL INSERT operation in the lineage graph
        /// </summary>
        public static SqlOperation ProcessInsertOperation(this LineageGraph graph,
            string tableName, string sqlText, string sourceLocation,
            IEnumerable<(string SourceTable, string SourceColumn)> sourceCols,
            IEnumerable<string> targetCols)
        {
            ArgumentNullException.ThrowIfNull(graph);

            // Create operation
            var operation = graph.AddSqlOperation(
                SqlOperationType.Insert,
                tableName,
                sqlText,
                sourceLocation);

            // Get source column IDs
            var sourceIds = new List<int>();
            if (sourceCols != null)
            {
                foreach (var (sourceTable, sourceColumn) in sourceCols)
                {
                    int colId = graph.GetColumnNode(sourceTable, sourceColumn);
                    if (colId >= 0)
                    {
                        sourceIds.Add(colId);
                    }
                    else
                    {
                        // Create column if it doesn't exist
                        colId = graph.AddColumnNode(sourceColumn, sourceTable);
                        sourceIds.Add(colId);
                    }
                }
            }

            // Get target column IDs
            var targetIds = new List<int>();
            if (targetCols != null)
            {
                foreach (var targetColumn in targetCols)
                {
                    int colId = graph.GetColumnNode(tableName, targetColumn);
                    if (colId >= 0)
                    {
                        targetIds.Add(colId);
                    }
                    else
                    {
                        // Create column if it doesn't exist
                        colId = graph.AddColumnNode(targetColumn, tableName);
                        targetIds.Add(colId);
                    }
                }
            }

            // Link operation in graph
            graph.LinkOperation(operation, sourceIds, targetIds);

            return operation;
        }

        /// <summary>
        /// Processes a SQL UPDATE operation in the lineage graph
        /// </summary>
        public static SqlOperation ProcessUpdateOperation(this LineageGraph graph,
            string tableName, string sqlText, string sourceLocation,
            IEnumerable<(string SourceTable, string SourceColumn)> sourceCols,
            IEnumerable<string> targetCols)
        {
            ArgumentNullException.ThrowIfNull(graph);

            // Create operation
            var operation = graph.AddSqlOperation(
                SqlOperationType.Update,
                tableName,
                sqlText,
                sourceLocation);

            // Get source column IDs
            var sourceIds = new List<int>();
            if (sourceCols != null)
            {
                foreach (var (sourceTable, sourceColumn) in sourceCols)
                {
                    int colId = graph.GetColumnNode(sourceTable, sourceColumn);
                    if (colId >= 0)
                    {
                        sourceIds.Add(colId);
                    }
                    else
                    {
                        // Create column if it doesn't exist
                        colId = graph.AddColumnNode(sourceColumn, sourceTable);
                        sourceIds.Add(colId);
                    }
                }
            }

            // Get target column IDs (in the same table)
            var targetIds = new List<int>();
            if (targetCols != null)
            {
                foreach (var targetColumn in targetCols)
                {
                    int colId = graph.GetColumnNode(tableName, targetColumn);
                    if (colId >= 0)
                    {
                        targetIds.Add(colId);
                    }
                    else
                    {
                        // Create column if it doesn't exist
                        colId = graph.AddColumnNode(targetColumn, tableName);
                        targetIds.Add(colId);
                    }
                }
            }

            // Link operation in graph
            graph.LinkOperation(operation, sourceIds, targetIds);

            return operation;
        }

        /// <summary>
        /// Gets all SQL operations registered in the graph
        /// </summary>
        public static List<SqlOperation> GetAllOperations(this LineageGraph graph)
        {
            ArgumentNullException.ThrowIfNull(graph);

            var result = new List<SqlOperation>();
            int operationCount = graph.GetMetadata("OperationCount") as int? ?? 0;

            for (int i = 0; i < operationCount; i++)
            {
                if (graph.GetMetadata($"Operation:{i}") is SqlOperation operation)
                {
                    result.Add(operation);
                }
            }

            return result;
        }
    }
}
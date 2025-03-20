using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace TSqlColumnLineage.Domain.Graph
{
    /// <summary>
    /// High-performance graph structure for SQL column lineage analysis
    /// using data-oriented design principles.
    /// </summary>
    public sealed class LineageGraph
    {
        // Underlying storage implementation
        private readonly GraphStorage _storage;
        
        // Metadata
        private readonly Dictionary<string, object> _metadata = new Dictionary<string, object>();
        
        // Source SQL script
        public string SourceSql { get; set; }
        
        // Creation timestamp
        public DateTime CreatedAt { get; } = DateTime.UtcNow;
        
        // Statistics
        private long _totalOperations;
        private readonly CancellationTokenSource _cancellationSource = new CancellationTokenSource();
        
        /// <summary>
        /// Creates a new lineage graph with specified initial capacity
        /// </summary>
        public LineageGraph(int initialNodeCapacity = 1024, int initialEdgeCapacity = 2048)
        {
            _storage = new GraphStorage(initialNodeCapacity, initialEdgeCapacity);
        }
        
        /// <summary>
        /// Adds a column node to the graph
        /// </summary>
        public int AddColumnNode(string name, string tableName, string dataType = "unknown", bool isNullable = false, bool isComputed = false)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Column name cannot be null or empty", nameof(name));
                
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));
                
            Interlocked.Increment(ref _totalOperations);
            return _storage.AddColumnNode(name, tableName, dataType, isNullable, isComputed);
        }
        
        /// <summary>
        /// Adds a table node to the graph
        /// </summary>
        public int AddTableNode(string name, string tableType = "Table", string alias = "", string definition = "")
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Table name cannot be null or empty", nameof(name));
                
            Interlocked.Increment(ref _totalOperations);
            return _storage.AddTableNode(name, tableType, alias, definition);
        }
        
        /// <summary>
        /// Adds an expression node to the graph
        /// </summary>
        public int AddExpressionNode(string name, string expressionText, string expressionType = "Expression", string resultType = "", string tableOwner = "")
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Expression name cannot be null or empty", nameof(name));
                
            if (string.IsNullOrEmpty(expressionText))
                throw new ArgumentException("Expression text cannot be null or empty", nameof(expressionText));
                
            Interlocked.Increment(ref _totalOperations);
            return _storage.AddExpressionNode(name, expressionText, expressionType, resultType, tableOwner);
        }
        
        /// <summary>
        /// Adds a column to a table
        /// </summary>
        public void AddColumnToTable(int tableId, int columnId)
        {
            Interlocked.Increment(ref _totalOperations);
            _storage.AddColumnToTable(tableId, columnId);
        }
        
        /// <summary>
        /// Adds a direct lineage edge between two columns
        /// </summary>
        public int AddDirectLineage(int sourceColumnId, int targetColumnId, string operation, string sqlExpression = "")
        {
            Interlocked.Increment(ref _totalOperations);
            return _storage.AddEdge(sourceColumnId, targetColumnId, EdgeType.Direct, operation, sqlExpression);
        }
        
        /// <summary>
        /// Adds an indirect lineage edge via an expression
        /// </summary>
        public int AddIndirectLineage(int sourceColumnId, int expressionId, string operation, string sqlExpression = "")
        {
            Interlocked.Increment(ref _totalOperations);
            return _storage.AddEdge(sourceColumnId, expressionId, EdgeType.Indirect, operation, sqlExpression);
        }
        
        /// <summary>
        /// Adds a join relationship between columns
        /// </summary>
        public int AddJoinRelationship(int leftColumnId, int rightColumnId, string joinType)
        {
            Interlocked.Increment(ref _totalOperations);
            return _storage.AddEdge(leftColumnId, rightColumnId, EdgeType.Join, joinType);
        }
        
        /// <summary>
        /// Gets a column node by table and column name
        /// </summary>
        public int GetColumnNode(string tableName, string columnName)
        {
            return _storage.GetColumnNode(tableName, columnName);
        }
        
        /// <summary>
        /// Gets all lineage paths from source to target column
        /// </summary>
        public List<LineagePath> GetLineagePaths(int sourceColumnId, int targetColumnId, int maxDepth = 10)
        {
            var paths = _storage.FindPaths(sourceColumnId, targetColumnId, maxDepth);
            var result = new List<LineagePath>();
            
            foreach (var path in paths)
            {
                var lineagePath = new LineagePath
                {
                    SourceId = sourceColumnId,
                    TargetId = targetColumnId,
                    Edges = new List<EdgeData>()
                };
                
                foreach (var edgeId in path)
                {
                    lineagePath.Edges.Add(_storage.GetEdgeData(edgeId));
                }
                
                result.Add(lineagePath);
            }
            
            return result;
        }
        
        /// <summary>
        /// Gets node data by id
        /// </summary>
        public NodeData GetNodeData(int nodeId)
        {
            return _storage.GetNodeData(nodeId);
        }
        
        /// <summary>
        /// Gets edge data by id
        /// </summary>
        public EdgeData GetEdgeData(int edgeId)
        {
            return _storage.GetEdgeData(edgeId);
        }
        
        /// <summary>
        /// Gets all source columns for a target column
        /// </summary>
        public List<int> GetSourceColumns(int columnId)
        {
            var result = new List<int>();
            var edgeIds = _storage.GetIncomingEdges(columnId);
            
            foreach (var edgeId in edgeIds)
            {
                var edge = _storage.GetEdgeData(edgeId);
                
                // Only include direct lineage
                if (edge.Type == EdgeType.Direct)
                {
                    result.Add(edge.SourceId);
                }
            }
            
            return result;
        }
        
        /// <summary>
        /// Gets all target columns for a source column
        /// </summary>
        public List<int> GetTargetColumns(int columnId)
        {
            var result = new List<int>();
            var edgeIds = _storage.GetOutgoingEdges(columnId);
            
            foreach (var edgeId in edgeIds)
            {
                var edge = _storage.GetEdgeData(edgeId);
                
                // Only include direct lineage
                if (edge.Type == EdgeType.Direct)
                {
                    result.Add(edge.TargetId);
                }
            }
            
            return result;
        }
        
        /// <summary>
        /// Gets all columns in a table
        /// </summary>
        public List<int> GetTableColumns(int tableId)
        {
            return _storage.GetTableColumns(tableId).ToList();
        }
        
        /// <summary>
        /// Sets a metadata value
        /// </summary>
        public void SetMetadata(string key, object value)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", nameof(key));
                
            lock (_metadata)
            {
                _metadata[key] = value;
            }
        }
        
        /// <summary>
        /// Gets a metadata value
        /// </summary>
        public object GetMetadata(string key)
        {
            if (string.IsNullOrEmpty(key))
                return null;
                
            lock (_metadata)
            {
                if (_metadata.TryGetValue(key, out var value))
                {
                    return value;
                }
                
                return null;
            }
        }
        
        /// <summary>
        /// Cancels any ongoing operations
        /// </summary>
        public void CancelOperations()
        {
            _cancellationSource.Cancel();
        }
        
        /// <summary>
        /// Optimizes the graph by compacting storage
        /// </summary>
        public void Compact()
        {
            _storage.Compact();
        }
        
        /// <summary>
        /// Gets statistics about the graph
        /// </summary>
        public GraphStatistics GetStatistics()
        {
            var (totalNodes, columnNodes, tableNodes, expressionNodes) = _storage.GetNodeStatistics();
            var (totalEdges, directEdges, indirectEdges, joinEdges) = _storage.GetEdgeStatistics();
            
            return new GraphStatistics
            {
                TotalNodes = totalNodes,
                ColumnNodes = columnNodes,
                TableNodes = tableNodes,
                ExpressionNodes = expressionNodes,
                TotalEdges = totalEdges,
                DirectEdges = directEdges,
                IndirectEdges = indirectEdges,
                JoinEdges = joinEdges,
                TotalOperations = _totalOperations
            };
        }
    }
    
    /// <summary>
    /// Represents a lineage path between source and target column
    /// </summary>
    public class LineagePath
    {
        public int SourceId { get; set; }
        public int TargetId { get; set; }
        public List<EdgeData> Edges { get; set; }
        
        public override string ToString()
        {
            return $"Path from {SourceId} to {TargetId} with {Edges.Count} edges";
        }
    }
    
    /// <summary>
    /// Statistics about the graph
    /// </summary>
    public class GraphStatistics
    {
        public int TotalNodes { get; set; }
        public int ColumnNodes { get; set; }
        public int TableNodes { get; set; }
        public int ExpressionNodes { get; set; }
        
        public int TotalEdges { get; set; }
        public int DirectEdges { get; set; }
        public int IndirectEdges { get; set; }
        public int JoinEdges { get; set; }
        
        public long TotalOperations { get; set; }
        
        public override string ToString()
        {
            return $"Nodes: {TotalNodes} ({ColumnNodes} columns, {TableNodes} tables, {ExpressionNodes} expressions), " +
                   $"Edges: {TotalEdges} ({DirectEdges} direct, {IndirectEdges} indirect, {JoinEdges} joins), " +
                   $"Operations: {TotalOperations}";
        }
    }
}
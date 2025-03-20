using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Models.Edges;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Models.Graph
{
    /// <summary>
    /// Represents the entire lineage graph with nodes and edges with optimized data structures 
    /// for performance and memory efficiency
    /// </summary>
    public sealed class LineageGraph
    {
        // Core data stores
        private readonly Dictionary<string, LineageNode> _nodes = new Dictionary<string, LineageNode>();
        private readonly Dictionary<string, LineageEdge> _edges = new Dictionary<string, LineageEdge>();
        
        // Optimized lookup indexes
        private readonly Dictionary<string, HashSet<string>> _sourceEdgeIndex = new Dictionary<string, HashSet<string>>();
        private readonly Dictionary<string, HashSet<string>> _targetEdgeIndex = new Dictionary<string, HashSet<string>>();
        private readonly Dictionary<string, HashSet<string>> _nodeTypeIndex = new Dictionary<string, HashSet<string>>();
        private readonly Dictionary<string, Dictionary<string, string>> _columnIndex = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
        
        // Performance optimization support
        private readonly StringPool _stringPool;
        private readonly IdGenerator _idGenerator;
        private int _modificationCount = 0;
        private readonly ReaderWriterLockSlim _graphLock = new ReaderWriterLockSlim();

        /// <summary>
        /// Graph nodes (read-only)
        /// </summary>
        [JsonProperty("nodes")]
        public IReadOnlyList<LineageNode> Nodes
        {
            get
            {
                _graphLock.EnterReadLock();
                try
                {
                    return _nodes.Values.ToList().AsReadOnly();
                }
                finally
                {
                    _graphLock.ExitReadLock();
                }
            }
        }

        /// <summary>
        /// Graph edges (read-only)
        /// </summary>
        [JsonProperty("edges")]
        public IReadOnlyList<LineageEdge> Edges
        {
            get
            {
                _graphLock.EnterReadLock();
                try
                {
                    return _edges.Values.ToList().AsReadOnly();
                }
                finally
                {
                    _graphLock.ExitReadLock();
                }
            }
        }

        /// <summary>
        /// Source SQL query
        /// </summary>
        [JsonProperty("sourceSql")]
        public string SourceSql { get; set; }

        /// <summary>
        /// Graph creation timestamp
        /// </summary>
        [JsonProperty("createdAt")]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        
        /// <summary>
        /// Additional graph metadata
        /// </summary>
        [JsonProperty("metadata")]
        public Dictionary<string, object> Metadata { get; } = new Dictionary<string, object>();
        
        /// <summary>
        /// Gets the number of modifications made to the graph
        /// </summary>
        [JsonIgnore]
        public int ModificationCount => _modificationCount;
        
        /// <summary>
        /// Creates a new lineage graph with the specified string pool and ID generator
        /// </summary>
        /// <param name="stringPool">String pool for optimized string handling</param>
        /// <param name="idGenerator">ID generator for creating node and edge IDs</param>
        public LineageGraph(StringPool stringPool, IdGenerator idGenerator)
        {
            _stringPool = stringPool ?? throw new ArgumentNullException(nameof(stringPool));
            _idGenerator = idGenerator ?? throw new ArgumentNullException(nameof(idGenerator));
        }
        
        /// <summary>
        /// Adds a node to the graph
        /// </summary>
        /// <param name="node">Node to add</param>
        public void AddNode(LineageNode node)
        {
            if (node is null) throw new ArgumentNullException(nameof(node));
            
            _graphLock.EnterWriteLock();
            try
            {
                // Generate ID if needed
                if (string.IsNullOrEmpty(node.Id))
                {
                    node.Id = _idGenerator.CreateNodeId(node.Type, node.Name);
                }
                
                // Intern strings to reduce memory usage
                node.InternStrings(_stringPool);
                
                // Add to main node collection
                _nodes[node.Id] = node;
                
                // Update type index
                if (!_nodeTypeIndex.TryGetValue(node.Type, out var typeNodes))
                {
                    typeNodes = new HashSet<string>();
                    _nodeTypeIndex[node.Type] = typeNodes;
                }
                typeNodes.Add(node.Id);
                
                // Update column index if this is a column node
                if (node is ColumnNode columnNode && !string.IsNullOrEmpty(columnNode.TableOwner))
                {
                    if (!_columnIndex.TryGetValue(columnNode.TableOwner, out var columns))
                    {
                        columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        _columnIndex[columnNode.TableOwner] = columns;
                    }
                    columns[columnNode.Name] = columnNode.Id;
                }
                
                // Track modification
                Interlocked.Increment(ref _modificationCount);
            }
            finally
            {
                _graphLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Adds an edge to the graph
        /// </summary>
        /// <param name="edge">Edge to add</param>
        public void AddEdge(LineageEdge edge)
        {
            if (edge is null) throw new ArgumentNullException(nameof(edge));
            
            _graphLock.EnterWriteLock();
            try
            {
                // Verify nodes exist
                if (!_nodes.ContainsKey(edge.SourceId))
                    throw new KeyNotFoundException($"Source node {edge.SourceId} not found");
                    
                if (!_nodes.ContainsKey(edge.TargetId))
                    throw new KeyNotFoundException($"Target node {edge.TargetId} not found");
    
                // Generate ID if needed
                if (string.IsNullOrEmpty(edge.Id))
                {
                    edge.Id = _idGenerator.CreateGuidId("EDGE");
                }
                
                // Intern strings
                edge.InternStrings(_stringPool);
                
                // Use edge key to prevent duplicates
                var edgeKey = edge.Key;
                if (_edges.ContainsKey(edgeKey))
                {
                    // Edge already exists, update it
                    _edges[edgeKey] = edge;
                }
                else
                {
                    // Add new edge
                    _edges[edgeKey] = edge;
                    
                    // Update source index
                    if (!_sourceEdgeIndex.TryGetValue(edge.SourceId, out var sourceEdges))
                    {
                        sourceEdges = new HashSet<string>();
                        _sourceEdgeIndex[edge.SourceId] = sourceEdges;
                    }
                    sourceEdges.Add(edgeKey);
                    
                    // Update target index
                    if (!_targetEdgeIndex.TryGetValue(edge.TargetId, out var targetEdges))
                    {
                        targetEdges = new HashSet<string>();
                        _targetEdgeIndex[edge.TargetId] = targetEdges;
                    }
                    targetEdges.Add(edgeKey);
                }
                
                // Track modification
                Interlocked.Increment(ref _modificationCount);
            }
            finally
            {
                _graphLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Gets a node by its ID
        /// </summary>
        /// <param name="id">Node ID</param>
        /// <returns>LineageNode if found, null otherwise</returns>
        public LineageNode GetNodeById(string id)
        {
            if (string.IsNullOrEmpty(id)) return null;
            
            _graphLock.EnterReadLock();
            try
            {
                _nodes.TryGetValue(id, out var node);
                return node;
            }
            finally
            {
                _graphLock.ExitReadLock();
            }
        }
        
        /// <summary>
        /// Gets nodes of a specific type
        /// </summary>
        /// <typeparam name="T">Type of node to get</typeparam>
        /// <returns>Collection of nodes of the specified type</returns>
        public IEnumerable<T> GetNodesOfType<T>() where T : LineageNode
        {
            string typeName = typeof(T).Name.Replace("Node", "");
            
            _graphLock.EnterReadLock();
            try
            {
                if (_nodeTypeIndex.TryGetValue(typeName, out var nodeIds))
                {
                    return nodeIds.Select(id => GetNodeById(id)).OfType<T>().ToList();
                }
                
                return Enumerable.Empty<T>();
            }
            finally
            {
                _graphLock.ExitReadLock();
            }
        }
        
        /// <summary>
        /// Gets a column node by table and column name
        /// </summary>
        /// <param name="tableName">Table name</param>
        /// <param name="columnName">Column name</param>
        /// <returns>ColumnNode if found, null otherwise</returns>
        public ColumnNode GetColumnNode(string tableName, string columnName)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(columnName))
                return null;
                
            _graphLock.EnterReadLock();
            try
            {
                if (_columnIndex.TryGetValue(tableName, out var columns) && 
                    columns.TryGetValue(columnName, out var columnId))
                {
                    return GetNodeById(columnId) as ColumnNode;
                }
                
                return null;
            }
            finally
            {
                _graphLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Finds input nodes for a given node
        /// </summary>
        /// <param name="nodeId">ID of the node to get inputs for</param>
        /// <returns>Collection of input nodes</returns>
        public IEnumerable<LineageNode> GetInputNodes(string nodeId)
        {
            if (string.IsNullOrEmpty(nodeId)) 
                return Enumerable.Empty<LineageNode>();
                
            _graphLock.EnterReadLock();
            try
            {
                if (_targetEdgeIndex.TryGetValue(nodeId, out var edgeKeys))
                {
                    return edgeKeys
                        .Select(key => _edges[key])
                        .Select(e => GetNodeById(e.SourceId))
                        .Where(n => n != null)
                        .ToList();
                }
                
                return Enumerable.Empty<LineageNode>();
            }
            finally
            {
                _graphLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Finds output nodes for a given node
        /// </summary>
        /// <param name="nodeId">ID of the node to get outputs for</param>
        /// <returns>Collection of output nodes</returns>
        public IEnumerable<LineageNode> GetOutputNodes(string nodeId)
        {
            if (string.IsNullOrEmpty(nodeId)) 
                return Enumerable.Empty<LineageNode>();
                
            _graphLock.EnterReadLock();
            try
            {
                if (_sourceEdgeIndex.TryGetValue(nodeId, out var edgeKeys))
                {
                    return edgeKeys
                        .Select(key => _edges[key])
                        .Select(e => GetNodeById(e.TargetId))
                        .Where(n => n != null)
                        .ToList();
                }
                
                return Enumerable.Empty<LineageNode>();
            }
            finally
            {
                _graphLock.ExitReadLock();
            }
        }
        
        /// <summary>
        /// Gets edges from a specific source node
        /// </summary>
        /// <param name="sourceId">Source node ID</param>
        /// <returns>Collection of edges from the source</returns>
        public IEnumerable<LineageEdge> GetEdgesFromSource(string sourceId)
        {
            if (string.IsNullOrEmpty(sourceId)) 
                return Enumerable.Empty<LineageEdge>();
                
            _graphLock.EnterReadLock();
            try
            {
                if (_sourceEdgeIndex.TryGetValue(sourceId, out var edgeKeys))
                {
                    return edgeKeys.Select(key => _edges[key]).ToList();
                }
                
                return Enumerable.Empty<LineageEdge>();
            }
            finally
            {
                _graphLock.ExitReadLock();
            }
        }
        
        /// <summary>
        /// Gets edges to a specific target node
        /// </summary>
        /// <param name="targetId">Target node ID</param>
        /// <returns>Collection of edges to the target</returns>
        public IEnumerable<LineageEdge> GetEdgesToTarget(string targetId)
        {
            if (string.IsNullOrEmpty(targetId)) 
                return Enumerable.Empty<LineageEdge>();
                
            _graphLock.EnterReadLock();
            try
            {
                if (_targetEdgeIndex.TryGetValue(targetId, out var edgeKeys))
                {
                    return edgeKeys.Select(key => _edges[key]).ToList();
                }
                
                return Enumerable.Empty<LineageEdge>();
            }
            finally
            {
                _graphLock.ExitReadLock();
            }
        }
        
        /// <summary>
        /// Creates a clone of this graph
        /// </summary>
        /// <returns>A new LineageGraph instance with the same nodes and edges</returns>
        public LineageGraph Clone()
        {
            var clone = new LineageGraph(_stringPool, _idGenerator)
            {
                SourceSql = SourceSql,
                CreatedAt = CreatedAt
            };
            
            _graphLock.EnterReadLock();
            try
            {
                // Clone metadata
                foreach (var (key, value) in Metadata)
                {
                    clone.Metadata[key] = value;
                }
                
                // Clone nodes
                foreach (var node in _nodes.Values)
                {
                    var nodeClone = (LineageNode)node.Clone();
                    clone.AddNode(nodeClone);
                }
                
                // Clone edges
                foreach (var edge in _edges.Values)
                {
                    var edgeClone = edge.Clone();
                    clone.AddEdge(edgeClone);
                }
                
                return clone;
            }
            finally
            {
                _graphLock.ExitReadLock();
            }
        }
        
        /// <summary>
        /// Gets all paths between two nodes
        /// </summary>
        /// <param name="sourceId">Source node ID</param>
        /// <param name="targetId">Target node ID</param>
        /// <param name="maxDepth">Maximum path depth to search</param>
        /// <returns>Collection of paths (lists of edge IDs)</returns>
        public IEnumerable<List<string>> GetPaths(string sourceId, string targetId, int maxDepth = 10)
        {
            if (string.IsNullOrEmpty(sourceId) || string.IsNullOrEmpty(targetId))
                yield break;
                
            if (sourceId == targetId)
            {
                yield return new List<string>();
                yield break;
            }
            
            _graphLock.EnterReadLock();
            try
            {
                var visited = new HashSet<string>();
                var path = new List<string>();
                
                foreach (var completePath in FindPaths(sourceId, targetId, visited, path, maxDepth))
                {
                    yield return completePath;
                }
            }
            finally
            {
                _graphLock.ExitReadLock();
            }
        }
        
        /// <summary>
        /// Recursive helper method to find all paths between nodes
        /// </summary>
        private IEnumerable<List<string>> FindPaths(
            string currentId, 
            string targetId, 
            HashSet<string> visited, 
            List<string> currentPath,
            int maxDepth)
        {
            if (currentPath.Count >= maxDepth)
                yield break;
                
            visited.Add(currentId);
            
            if (currentId == targetId)
            {
                yield return new List<string>(currentPath);
            }
            else
            {
                if (_sourceEdgeIndex.TryGetValue(currentId, out var outEdges))
                {
                    foreach (var edgeKey in outEdges)
                    {
                        var edge = _edges[edgeKey];
                        var nextNode = edge.TargetId;
                        
                        if (!visited.Contains(nextNode))
                        {
                            currentPath.Add(edge.Id);
                            
                            foreach (var path in FindPaths(nextNode, targetId, visited, currentPath, maxDepth))
                            {
                                yield return path;
                            }
                            
                            currentPath.RemoveAt(currentPath.Count - 1);
                        }
                    }
                }
            }
            
            visited.Remove(currentId);
        }
        
        /// <summary>
        /// Compacts the graph by removing orphaned nodes and optimizing memory usage
        /// </summary>
        public void Compact()
        {
            _graphLock.EnterWriteLock();
            try
            {
                // Find all nodes that are connected by edges
                var connectedNodes = new HashSet<string>();
                
                foreach (var edge in _edges.Values)
                {
                    connectedNodes.Add(edge.SourceId);
                    connectedNodes.Add(edge.TargetId);
                }
                
                // Find orphaned nodes (nodes with no connections)
                var orphanedNodes = _nodes.Keys.Except(connectedNodes).ToList();
                
                // Remove orphaned nodes
                foreach (var nodeId in orphanedNodes)
                {
                    var node = _nodes[nodeId];
                    
                    // Remove from type index
                    if (_nodeTypeIndex.TryGetValue(node.Type, out var typeNodes))
                    {
                        typeNodes.Remove(nodeId);
                    }
                    
                    // Remove from column index if it's a column
                    if (node is ColumnNode columnNode)
                    {
                        if (_columnIndex.TryGetValue(columnNode.TableOwner, out var columns))
                        {
                            columns.Remove(columnNode.Name);
                        }
                    }
                    
                    // Remove from main collection
                    _nodes.Remove(nodeId);
                }
                
                // Re-intern all strings to consolidate string pool
                foreach (var node in _nodes.Values)
                {
                    node.InternStrings(_stringPool);
                }
                
                foreach (var edge in _edges.Values)
                {
                    edge.InternStrings(_stringPool);
                }
                
                // Track modification
                Interlocked.Increment(ref _modificationCount);
            }
            finally
            {
                _graphLock.ExitWriteLock();
            }
        }
    }
}
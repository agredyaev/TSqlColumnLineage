using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using TSqlColumnLineage.Core.Models.Edges;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Models.Graph
{
    /// <summary>
    /// Represents the entire lineage graph with nodes and edges
    /// </summary>
    public sealed class LineageGraph
    {
        // Fast lookup structures
        private readonly Dictionary<string, LineageNode> _nodes = new();
        private readonly Dictionary<string, LineageEdge> _edges = new();
        
        // Improved edge lookup with source and target indexes
        private readonly Dictionary<string, HashSet<string>> _sourceEdgeIndex = new();
        private readonly Dictionary<string, HashSet<string>> _targetEdgeIndex = new();
        
        // Lookup for nodes by type
        private readonly Dictionary<string, HashSet<string>> _nodeTypeIndex = new();
        
        // Column lookup by table and name
        private readonly Dictionary<string, Dictionary<string, string>> _columnIndex = new(StringComparer.OrdinalIgnoreCase);
        
        // String interning pool
        private readonly Dictionary<string, string> _stringPool = new();

        /// <summary>
        /// Graph nodes (read-only)
        /// </summary>
        [JsonProperty("nodes")]
        public IReadOnlyList<LineageNode> Nodes => _nodes.Values.ToList().AsReadOnly();

        /// <summary>
        /// Graph edges (read-only)
        /// </summary>
        [JsonProperty("edges")]
        public IReadOnlyList<LineageEdge> Edges => _edges.Values.ToList().AsReadOnly();

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
        public Dictionary<string, object> Metadata { get; } = new();
        
        /// <summary>
        /// Adds a node to the graph
        /// </summary>
        public void AddNode(LineageNode node)
        {
            if (node is null) throw new ArgumentNullException(nameof(node));
            
            // Generate ID if needed
            node.Id = string.IsNullOrEmpty(node.Id) 
                ? Guid.NewGuid().ToString() 
                : node.Id;
            
            // Intern strings to reduce memory usage
            node.Name = InternString(node.Name);
            node.ObjectName = InternString(node.ObjectName);
            node.Type = InternString(node.Type);
            
            if (node is ColumnNode columnNode)
            {
                columnNode.TableOwner = InternString(columnNode.TableOwner);
                columnNode.DataType = InternString(columnNode.DataType);
                
                // Update column index
                if (!string.IsNullOrEmpty(columnNode.TableOwner))
                {
                    if (!_columnIndex.TryGetValue(columnNode.TableOwner, out var columns))
                    {
                        columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                        _columnIndex[columnNode.TableOwner] = columns;
                    }
                    columns[columnNode.Name] = columnNode.Id;
                }
            }
            else if (node is TableNode tableNode)
            {
                tableNode.TableType = InternString(tableNode.TableType);
                tableNode.Alias = InternString(tableNode.Alias);
            }
            else if (node is ExpressionNode exprNode)
            {
                exprNode.ExpressionType = InternString(exprNode.ExpressionType);
                exprNode.ResultType = InternString(exprNode.ResultType);
                exprNode.TableOwner = InternString(exprNode.TableOwner);
            }

            // Add to main node collection
            if (!_nodes.ContainsKey(node.Id))
            {
                _nodes[node.Id] = node;
                
                // Update type index
                if (!_nodeTypeIndex.TryGetValue(node.Type, out var typeNodes))
                {
                    typeNodes = new HashSet<string>();
                    _nodeTypeIndex[node.Type] = typeNodes;
                }
                typeNodes.Add(node.Id);
            }
        }

        /// <summary>
        /// Adds an edge to the graph
        /// </summary>
        public void AddEdge(LineageEdge edge)
        {
            if (edge is null) throw new ArgumentNullException(nameof(edge));
            if (!_nodes.ContainsKey(edge.SourceId)) throw new KeyNotFoundException($"Source node {edge.SourceId} not found");
            if (!_nodes.ContainsKey(edge.TargetId)) throw new KeyNotFoundException($"Target node {edge.TargetId} not found");

            // Generate ID if needed
            edge.Id = string.IsNullOrEmpty(edge.Id) 
                ? Guid.NewGuid().ToString() 
                : edge.Id;
                
            // Intern strings
            edge.Type = InternString(edge.Type);
            edge.Operation = InternString(edge.Operation);
            
            // Use edge key to prevent duplicates
            var edgeKey = edge.Key;
            if (!_edges.ContainsKey(edgeKey))
            {
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
        }

        /// <summary>
        /// Gets a node by its ID
        /// </summary>
        public LineageNode GetNodeById(string id)
        {
            return _nodes.TryGetValue(id, out var node) ? node : null;
        }
        
        /// <summary>
        /// Gets nodes of a specific type
        /// </summary>
        public IEnumerable<T> GetNodesOfType<T>() where T : LineageNode
        {
            string typeName = typeof(T).Name.Replace("Node", "");
            
            if (_nodeTypeIndex.TryGetValue(typeName, out var nodeIds))
            {
                return nodeIds.Select(id => GetNodeById(id)).OfType<T>();
            }
            
            return Enumerable.Empty<T>();
        }
        
        /// <summary>
        /// Gets a column node by table and column name
        /// </summary>
        public ColumnNode GetColumnNode(string tableName, string columnName)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(columnName))
                return null;
                
            if (_columnIndex.TryGetValue(tableName, out var columns) && 
                columns.TryGetValue(columnName, out var columnId))
            {
                return GetNodeById(columnId) as ColumnNode;
            }
            
            return null;
        }

        /// <summary>
        /// Finds input nodes for a given node
        /// </summary>
        public IEnumerable<LineageNode> GetInputNodes(string nodeId)
        {
            if (_targetEdgeIndex.TryGetValue(nodeId, out var edgeKeys))
            {
                return edgeKeys
                    .Select(key => _edges[key])
                    .Select(e => GetNodeById(e.SourceId))
                    .Where(n => n != null);
            }
            
            return Enumerable.Empty<LineageNode>();
        }

        /// <summary>
        /// Finds output nodes for a given node
        /// </summary>
        public IEnumerable<LineageNode> GetOutputNodes(string nodeId)
        {
            if (_sourceEdgeIndex.TryGetValue(nodeId, out var edgeKeys))
            {
                return edgeKeys
                    .Select(key => _edges[key])
                    .Select(e => GetNodeById(e.TargetId))
                    .Where(n => n != null);
            }
            
            return Enumerable.Empty<LineageNode>();
        }
        
        /// <summary>
        /// Gets edges from a specific source node
        /// </summary>
        public IEnumerable<LineageEdge> GetEdgesFromSource(string sourceId)
        {
            if (_sourceEdgeIndex.TryGetValue(sourceId, out var edgeKeys))
            {
                return edgeKeys.Select(key => _edges[key]);
            }
            
            return Enumerable.Empty<LineageEdge>();
        }
        
        /// <summary>
        /// Gets edges to a specific target node
        /// </summary>
        public IEnumerable<LineageEdge> GetEdgesToTarget(string targetId)
        {
            if (_targetEdgeIndex.TryGetValue(targetId, out var edgeKeys))
            {
                return edgeKeys.Select(key => _edges[key]);
            }
            
            return Enumerable.Empty<LineageEdge>();
        }
        
        /// <summary>
        /// Interns a string to reduce memory usage
        /// </summary>
        private string InternString(string str)
        {
            if (string.IsNullOrEmpty(str))
                return str;
                
            if (!_stringPool.TryGetValue(str, out var internedString))
            {
                _stringPool[str] = str;
                return str;
            }
            return internedString;
        }
        
        /// <summary>
        /// Creates a clone of this graph
        /// </summary>
        public LineageGraph Clone()
        {
            var clone = new LineageGraph
            {
                SourceSql = SourceSql,
                CreatedAt = CreatedAt
            };
            
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
    }
}
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace TSqlColumnLineage.Core.Models
{
    /// <summary>
    /// Class representing the entire lineage graph
    /// </summary>
    public sealed class LineageGraph
    {
        private readonly LineageNodeFactory _nodeFactory;
        private readonly LineageEdgeFactory _edgeFactory;

        public LineageGraph(LineageNodeFactory nodeFactory, LineageEdgeFactory edgeFactory)
        {
            _nodeFactory = nodeFactory ?? throw new ArgumentNullException(nameof(nodeFactory));
            _edgeFactory = edgeFactory ?? throw new ArgumentNullException(nameof(edgeFactory));
        }

        /// <summary>
        /// Graph nodes (read-only)
        /// </summary>
        [JsonProperty("nodes")]
        public IReadOnlyList<LineageNode> Nodes => _nodes.Values.ToList().AsReadOnly();
        private readonly Dictionary<string, LineageNode> _nodes = new();

        /// <summary>
        /// Graph edges (read-only)
        /// </summary>
        [JsonProperty("edges")]
        public IReadOnlyList<LineageEdge> Edges => _edges.Values.ToList().AsReadOnly();
        private readonly Dictionary<string, LineageEdge> _edges = new();

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
        /// Adds a node to the graph
        /// </summary>
        public void AddNode(LineageNode node)
        {
            if (node is null) throw new ArgumentNullException(nameof(node));
            
            node.Id = string.IsNullOrEmpty(node.Id) 
                ? Guid.NewGuid().ToString() 
                : node.Id;

            if (!_nodes.ContainsKey(node.Id))
            {
                _nodes[node.Id] = node;
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

            edge.Id = string.IsNullOrEmpty(edge.Id) 
                ? Guid.NewGuid().ToString() 
                : edge.Id;

            var edgeKey = $"{edge.SourceId}-{edge.TargetId}-{edge.Type}";
            if (!_edges.ContainsKey(edgeKey))
            {
                _edges[edgeKey] = edge;
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
        /// Finds input nodes for a given node
        /// </summary>
        public IEnumerable<LineageNode> GetInputNodes(string nodeId)
        {
            return _edges.Values
                .Where(e => e.TargetId == nodeId)
                .Select(e => GetNodeById(e.SourceId))
                .Where(n => n != null);
        }

        /// <summary>
        /// Finds output nodes for a given node
        /// </summary>
        public IEnumerable<LineageNode> GetOutputNodes(string nodeId)
        {
            return _edges.Values
                .Where(e => e.SourceId == nodeId)
                .Select(e => GetNodeById(e.TargetId))
                .Where(n => n != null);
        }
    }
}

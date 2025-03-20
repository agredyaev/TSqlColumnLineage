using System;
using System.IO;
using Newtonsoft.Json;
using TSqlColumnLineage.Core.Models;

namespace TSqlColumnLineage.Core.Services
{
    /// <summary>
    /// Interface for graph-related operations
    /// </summary>
    public interface IGraphService
    {
        /// <summary>
        /// Serializes the lineage graph to JSON format
        /// </summary>
        /// <param name="graph">Lineage graph to serialize</param>
        /// <returns>JSON string representing the graph</returns>
        string SerializeToJson(LineageGraph graph);

        /// <summary>
        /// Serializes the lineage graph to a JSON file
        /// </summary>
        /// <param name="graph">Lineage graph to serialize</param>
        /// <param name="filePath">File path to save the JSON output</param>
        void SerializeToJsonFile(LineageGraph graph, string filePath);

        /// <summary>
        /// Deserializes a lineage graph from a JSON string
        /// </summary>
        /// <param name="json">JSON string representing the graph</param>
        /// <returns>LineageGraph object</returns>
        LineageGraph DeserializeFromJson(string json);

        /// <summary>
        /// Deserializes a lineage graph from a JSON file
        /// </summary>
        /// <param name="filePath">File path to read the JSON input from</param>
        /// <returns>LineageGraph object</returns>
        LineageGraph DeserializeFromJsonFile(string filePath);
    }

    /// <summary>
    /// Service for graph-related operations
    /// </summary>
    public class GraphService : IGraphService
    {
        private readonly LineageNodeFactory _nodeFactory;
        private readonly LineageEdgeFactory _edgeFactory;

        /// <summary>
        /// Initializes a new instance of the GraphService class
        /// </summary>
        /// <param name="nodeFactory">Factory for creating lineage nodes</param>
        /// <param name="edgeFactory">Factory for creating lineage edges</param>
        public GraphService(LineageNodeFactory nodeFactory, LineageEdgeFactory edgeFactory)
        {
            _nodeFactory = nodeFactory ?? throw new ArgumentNullException(nameof(nodeFactory));
            _edgeFactory = edgeFactory ?? throw new ArgumentNullException(nameof(edgeFactory));
        }

        /// <summary>
        /// Serializes the lineage graph to JSON format
        /// </summary>
        /// <param name="graph">Lineage graph to serialize</param>
        /// <returns>JSON string representing the graph</returns>
        public string SerializeToJson(LineageGraph graph)
        {
            return JsonConvert.SerializeObject(graph, Formatting.Indented);
        }

        /// <summary>
        /// Serializes the lineage graph to a JSON file
        /// </summary>
        /// <param name="graph">Lineage graph to serialize</param>
        /// <param name="filePath">File path to save the JSON output</param>
        public void SerializeToJsonFile(LineageGraph graph, string filePath)
        {
            File.WriteAllText(filePath, SerializeToJson(graph));
        }

        /// <summary>
        /// Deserializes a lineage graph from a JSON string
        /// </summary>
        /// <param name="json">JSON string representing the graph</param>
        /// <returns>LineageGraph object</returns>
        public LineageGraph DeserializeFromJson(string json)
        {
            var graph = JsonConvert.DeserializeObject<LineageGraph>(json);
            
            // Since the deserialized graph won't have the factories,
            // we need to create a new graph with our factories and copy the data
            if (graph != null)
            {
                var newGraph = new LineageGraph(_nodeFactory, _edgeFactory)
                {
                    SourceSql = graph.SourceSql,
                    CreatedAt = graph.CreatedAt
                };
                
                // Copy nodes and edges - in a real implementation
                // you'd need more complex logic to ensure all references are maintained
                foreach (var node in graph.Nodes)
                {
                    newGraph.AddNode(node);
                }
                
                foreach (var edge in graph.Edges)
                {
                    newGraph.AddEdge(edge);
                }
                
                return newGraph;
            }
            
            return null;
        }

        /// <summary>
        /// Deserializes a lineage graph from a JSON file
        /// </summary>
        /// <param name="filePath">File path to read the JSON input from</param>
        /// <returns>LineageGraph object</returns>
        public LineageGraph DeserializeFromJsonFile(string filePath)
        {
            var json = File.ReadAllText(filePath);
            return DeserializeFromJson(json);
        }
    }
}

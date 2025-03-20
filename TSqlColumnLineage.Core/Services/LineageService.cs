using System;
using System.IO;
using Newtonsoft.Json;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Parsing;
using TSqlColumnLineage.Core.Visitors;

namespace TSqlColumnLineage.Core.Services
{
    /// <summary>
    /// Interface for lineage service operations
    /// </summary>
    public interface ILineageService
    {
        /// <summary>
        /// Builds the lineage graph for a given SQL query
        /// </summary>
        /// <param name="sqlQuery">SQL query to analyze</param>
        /// <returns>LineageGraph object representing the column lineage</returns>
        LineageGraph BuildLineage(string sqlQuery);

        /// <summary>
        /// Exports the lineage graph to JSON format
        /// </summary>
        /// <param name="graph">Lineage graph to export</param>
        /// <returns>JSON string representing the graph</returns>
        string ExportToJson(LineageGraph graph);

        /// <summary>
        /// Saves the lineage graph to a JSON file
        /// </summary>
        /// <param name="graph">Lineage graph to save</param>
        /// <param name="filePath">File path to save the JSON output</param>
        void SaveToJsonFile(LineageGraph graph, string filePath);
    }

    /// <summary>
    /// Main service for building column lineage
    /// </summary>
    public class LineageService : ILineageService
    {
        private readonly SqlParser _parser;
        private readonly IMetadataService _metadataService;
        private readonly IGraphService _graphService;
        private readonly ILogger _logger;

        /// <summary>
        /// Initializes a new instance of the LineageService
        /// </summary>
        private readonly LineageNodeFactory _nodeFactory;
        private readonly LineageEdgeFactory _edgeFactory;

        public LineageService(
            SqlParser parser, 
            IMetadataService metadataService, 
            IGraphService graphService, 
            ILogger logger,
            LineageNodeFactory nodeFactory,
            LineageEdgeFactory edgeFactory)
        {
            _parser = parser ?? throw new ArgumentNullException(nameof(parser));
            _metadataService = metadataService ?? throw new ArgumentNullException(nameof(metadataService));
            _graphService = graphService ?? throw new ArgumentNullException(nameof(graphService));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _nodeFactory = nodeFactory ?? throw new ArgumentNullException(nameof(nodeFactory));
            _edgeFactory = edgeFactory ?? throw new ArgumentNullException(nameof(edgeFactory));
        }

        /// <summary>
        /// Builds the lineage graph for a given SQL query
        /// </summary>
        /// <param name="sqlQuery">SQL query to analyze</param>
        /// <returns>LineageGraph object representing the column lineage</returns>
        public LineageGraph BuildLineage(string sqlQuery)
        {
            var fragment = _parser.Parse(sqlQuery);
            var graph = new LineageGraph(_nodeFactory, _edgeFactory) { SourceSql = sqlQuery };
            var context = new LineageContext(graph);

            // Populate context with metadata
            _metadataService.PopulateContext(context);
            
            // Store the metadata service directly for easier access
            context.Metadata["MetadataService"] = _metadataService;

            // Create visitor and traverse AST
            var visitor = new ColumnLineageVisitor(graph, context, _logger);
            visitor.Visit(fragment);

            return graph;
        }

        /// <summary>
        /// Exports the lineage graph to JSON format
        /// </summary>
        /// <param name="graph">Lineage graph to export</param>
        /// <returns>JSON string representing the graph</returns>
        public string ExportToJson(LineageGraph graph)
        {
            return _graphService.SerializeToJson(graph);
        }

        /// <summary>
        /// Saves the lineage graph to a JSON file
        /// </summary>
        /// <param name="graph">Lineage graph to save</param>
        /// <param name="filePath">File path to save the JSON output</param>
        public void SaveToJsonFile(LineageGraph graph, string filePath)
        {
            _graphService.SerializeToJsonFile(graph, filePath);
        }
    }
}

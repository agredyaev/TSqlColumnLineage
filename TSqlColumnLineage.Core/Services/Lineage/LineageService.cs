using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TSqlColumnLineage.Core.Analysis.Context;
using TSqlColumnLineage.Core.Analysis.Handlers.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Analysis.Visitors.Specialized;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Parsing;

namespace TSqlColumnLineage.Core.Services.Lineage
{
    /// <summary>
    /// Options for lineage service
    /// </summary>
    public class LineageServiceOptions
    {
        /// <summary>
        /// SQL Server version to use for parsing
        /// </summary>
        public SqlServerVersion SqlServerVersion { get; set; } = SqlServerVersion.Latest;
        
        /// <summary>
        /// Maximum script size in bytes (0 for no limit)
        /// </summary>
        public int MaxScriptSizeBytes { get; set; } = 20 * 1024 * 1024; // 20MB default
        
        /// <summary>
        /// Maximum processing time in milliseconds (0 for no limit)
        /// </summary>
        public int MaxProcessingTimeMs { get; set; } = 60 * 1000; // 60 seconds default
        
        /// <summary>
        /// Maximum number of fragments to process (0 for no limit)
        /// </summary>
        public int MaxFragmentsToProcess { get; set; } = 100000; // 100K fragments default
        
        /// <summary>
        /// Whether to use parallelized processing for batch scripts
        /// </summary>
        public bool UseParallelProcessing { get; set; } = true;
        
        /// <summary>
        /// Number of threads to use for parallel processing (0 for auto)
        /// </summary>
        public int ParallelThreads { get; set; } = 0;
    }
    
    /// <summary>
    /// Result of lineage analysis
    /// </summary>
    public class LineageResult
    {
        /// <summary>
        /// The lineage graph
        /// </summary>
        public LineageGraph Graph { get; set; }
        
        /// <summary>
        /// Performance information
        /// </summary>
        public Dictionary<string, object> PerformanceInfo { get; set; } = new Dictionary<string, object>();
        
        /// <summary>
        /// Processing errors, if any
        /// </summary>
        public List<string> Errors { get; set; } = new List<string>();
        
        /// <summary>
        /// Warnings from processing
        /// </summary>
        public List<string> Warnings { get; set; } = new List<string>();
    }
    
    /// <summary>
    /// Main service for building column lineage with optimized performance
    /// </summary>
    public sealed class LineageService : IDisposable
    {
        private readonly SqlParser _parser;
        private readonly ILogger _logger;
        private readonly StringPool _stringPool;
        private readonly IdGenerator _idGenerator;
        private readonly LineageServiceOptions _options;
        
        /// <summary>
        /// Creates a new instance of the lineage service
        /// </summary>
        /// <param name="options">Service options</param>
        /// <param name="logger">Logger for diagnostic info</param>
        public LineageService(LineageServiceOptions options = null, ILogger logger = null)
        {
            _options = options ?? new LineageServiceOptions();
            _logger = logger;
            
            // Initialize utilities
            _stringPool = new StringPool();
            _idGenerator = new IdGenerator(_stringPool);
            
            // Initialize parser
            _parser = new SqlParser(
                _options.SqlServerVersion,
                _stringPool,
                batchSizeLimitBytes: _options.MaxScriptSizeBytes,
                logger: _logger);
        }
        
        /// <summary>
        /// Creates a new instance with explicit dependencies
        /// </summary>
        /// <param name="parser">SQL parser</param>
        /// <param name="stringPool">String pool</param>
        /// <param name="idGenerator">ID generator</param>
        /// <param name="options">Service options</param>
        /// <param name="logger">Logger</param>
        public LineageService(
            SqlParser parser,
            StringPool stringPool,
            IdGenerator idGenerator,
            LineageServiceOptions options = null,
            ILogger logger = null)
        {
            _parser = parser ?? throw new ArgumentNullException(nameof(parser));
            _stringPool = stringPool ?? throw new ArgumentNullException(nameof(stringPool));
            _idGenerator = idGenerator ?? throw new ArgumentNullException(nameof(idGenerator));
            _options = options ?? new LineageServiceOptions();
            _logger = logger;
        }

        /// <summary>
        /// Analyzes a SQL query to build column lineage
        /// </summary>
        /// <param name="sqlQuery">SQL query to analyze</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Lineage analysis result</returns>
        public async Task<LineageResult> AnalyzeQueryAsync(string sqlQuery, CancellationToken cancellationToken = default)
        {
            var startTime = DateTime.UtcNow;
            var result = new LineageResult();
            
            try
            {
                _logger?.LogInformation($"Starting analysis of SQL query ({sqlQuery.Length} chars)");
                
                // Parse the SQL query
                var fragment = await _parser.ParseAsync(sqlQuery, cancellationToken);
                var parseTime = DateTime.UtcNow - startTime;
                _logger?.LogDebug($"SQL parsing completed in {parseTime.TotalMilliseconds}ms");
                
                // Create the lineage graph and context
                var graph = new LineageGraph(_stringPool, _idGenerator) { SourceSql = sqlQuery };
                var lineageContext = new LineageContext(graph, _stringPool, _idGenerator);
                
                // Create visitor context with processing limits
                var visitorContext = new VisitorContext(lineageContext, _stringPool, _logger)
                {
                    MaxProcessingTimeMs = _options.MaxProcessingTimeMs,
                    MaxFragmentVisitCount = _options.MaxFragmentsToProcess
                };
                
                // Create the handler registry
                var handlerRegistry = HandlerRegistry.CreateDefault(visitorContext, _stringPool, _idGenerator, _logger);
                
                // Create and execute the visitor
                var visitor = new ColumnLineageVisitor(
                    visitorContext, 
                    _stringPool, 
                    _idGenerator, 
                    handlerRegistry,
                    _logger,
                    cancellationToken);
                
                // Use non-recursive traversal to avoid stack overflow
                visitor.VisitNonRecursive(fragment);
                
                // Check if processing was stopped before completion
                if (visitorContext.ShouldStop)
                {
                    result.Warnings.Add($"Processing stopped due to limits: " +
                                       $"Time limit {_options.MaxProcessingTimeMs}ms, " +
                                       $"Fragment limit {_options.MaxFragmentsToProcess}");
                }
                
                // Optimize the graph by removing orphaned nodes
                graph.Compact();
                
                // Set result
                result.Graph = graph;
                
                // Set performance info
                var totalTime = DateTime.UtcNow - startTime;
                result.PerformanceInfo["TotalTimeMs"] = totalTime.TotalMilliseconds;
                result.PerformanceInfo["ParseTimeMs"] = parseTime.TotalMilliseconds;
                result.PerformanceInfo["AnalysisTimeMs"] = (totalTime - parseTime).TotalMilliseconds;
                result.PerformanceInfo["FragmentsProcessed"] = visitorContext.FragmentVisitCount;
                result.PerformanceInfo["NodeCount"] = graph.Nodes.Count;
                result.PerformanceInfo["EdgeCount"] = graph.Edges.Count;
                result.PerformanceInfo["StringPoolStats"] = _stringPool.GetStatistics();
                
                _logger?.LogInformation($"Analysis completed in {totalTime.TotalMilliseconds}ms, " +
                                      $"found {graph.Nodes.Count} nodes and {graph.Edges.Count} edges");
                
                return result;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error analyzing SQL query");
                
                result.Errors.Add($"Error analyzing SQL query: {ex.Message}");
                return result;
            }
        }
        
        /// <summary>
        /// Analyzes a large SQL script with multiple batches
        /// </summary>
        /// <param name="sqlScript">SQL script to analyze</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Lineage analysis result</returns>
        public async Task<LineageResult> AnalyzeScriptAsync(string sqlScript, CancellationToken cancellationToken = default)
        {
            var startTime = DateTime.UtcNow;
            var result = new LineageResult();
            
            try
            {
                _logger?.LogInformation($"Starting analysis of SQL script ({sqlScript.Length} chars)");
                
                // Create the lineage graph and context
                var graph = new LineageGraph(_stringPool, _idGenerator) { SourceSql = sqlScript };
                var lineageContext = new LineageContext(graph, _stringPool, _idGenerator);
                
                // Create visitor context with processing limits
                var visitorContext = new VisitorContext(lineageContext, _stringPool, _logger)
                {
                    MaxProcessingTimeMs = _options.MaxProcessingTimeMs,
                    MaxFragmentVisitCount = _options.MaxFragmentsToProcess
                };
                
                // Create the handler registry
                var handlerRegistry = HandlerRegistry.CreateDefault(visitorContext, _stringPool, _idGenerator, _logger);
                
                // Parse the script into statements
                List<TSqlStatement> statements;
                if (_options.UseParallelProcessing)
                {
                    // Parse in parallel for large scripts
                    statements = await _parser.ParseLargeScriptParallelAsync(
                        sqlScript, 
                        _options.ParallelThreads, 
                        cancellationToken);
                }
                else
                {
                    // Parse sequentially
                    statements = new List<TSqlStatement>(_parser.ParseLargeScript(sqlScript));
                }
                
                var parseTime = DateTime.UtcNow - startTime;
                _logger?.LogDebug($"SQL parsing completed in {parseTime.TotalMilliseconds}ms, found {statements.Count} statements");
                
                // Process each statement
                var visitor = new ColumnLineageVisitor(
                    visitorContext, 
                    _stringPool, 
                    _idGenerator, 
                    handlerRegistry,
                    _logger,
                    cancellationToken);
                
                int processedCount = 0;
                foreach (var statement in statements)
                {
                    if (cancellationToken.IsCancellationRequested || visitorContext.ShouldStop)
                        break;
                        
                    // Use non-recursive traversal to avoid stack overflow
                    visitor.VisitNonRecursive(statement);
                    processedCount++;
                    
                    // Log progress periodically
                    if (processedCount % 10 == 0)
                    {
                        _logger?.LogDebug($"Processed {processedCount}/{statements.Count} statements");
                    }
                }
                
                // Check if processing was stopped before completion
                if (visitorContext.ShouldStop)
                {
                    result.Warnings.Add($"Processing stopped due to limits: " +
                                       $"Time limit {_options.MaxProcessingTimeMs}ms, " +
                                       $"Fragment limit {_options.MaxFragmentsToProcess}");
                }
                
                // Optimize the graph by removing orphaned nodes
                graph.Compact();
                
                // Set result
                result.Graph = graph;
                
                // Set performance info
                var totalTime = DateTime.UtcNow - startTime;
                result.PerformanceInfo["TotalTimeMs"] = totalTime.TotalMilliseconds;
                result.PerformanceInfo["ParseTimeMs"] = parseTime.TotalMilliseconds;
                result.PerformanceInfo["AnalysisTimeMs"] = (totalTime - parseTime).TotalMilliseconds;
                result.PerformanceInfo["StatementsProcessed"] = processedCount;
                result.PerformanceInfo["FragmentsProcessed"] = visitorContext.FragmentVisitCount;
                result.PerformanceInfo["NodeCount"] = graph.Nodes.Count;
                result.PerformanceInfo["EdgeCount"] = graph.Edges.Count;
                result.PerformanceInfo["StringPoolStats"] = _stringPool.GetStatistics();
                
                _logger?.LogInformation($"Analysis completed in {totalTime.TotalMilliseconds}ms, " +
                                      $"found {graph.Nodes.Count} nodes and {graph.Edges.Count} edges");
                
                return result;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error analyzing SQL script");
                
                result.Errors.Add($"Error analyzing SQL script: {ex.Message}");
                return result;
            }
        }
        
        /// <summary>
        /// Gets statistics about string pool usage
        /// </summary>
        /// <returns>String with statistics</returns>
        public string GetStringPoolStatistics()
        {
            return _stringPool.GetStatistics();
        }
        
        /// <summary>
        /// Disposes resources
        /// </summary>
        public void Dispose()
        {
            // Clean up resources
            _logger?.LogInformation("Disposing LineageService");
        }
    }
}
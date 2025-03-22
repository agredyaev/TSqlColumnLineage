using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TSqlColumnLineage.Core.Domain.Context;
using TSqlColumnLineage.Core.Domain.Graph;
using TSqlColumnLineage.Core.Engine.Parsing;
using TSqlColumnLineage.Core.Infrastructure;
using TSqlColumnLineage.Core.Infrastructure.Monitoring;

namespace TSqlColumnLineage.Core
{
    /// <summary>
    /// Main entry point for T-SQL column lineage tracking, integrating all components
    /// </summary>
    public class TSqlLineageTracker : IDisposable
    {
        private readonly SimplifiedSqlParser _parser;
        private readonly InfrastructureService _infrastructureService;
        private readonly PerformanceTracker _performanceTracker;
        private readonly LineageGraph _graph;
        private readonly ContextManager _contextManager;
        
        /// <summary>
        /// Creates a new lineage tracker
        /// </summary>
        public TSqlLineageTracker()
        {
            // Initialize infrastructure
            _infrastructureService = InfrastructureService.Instance;
            _infrastructureService.Initialize(true);
            
            _performanceTracker = PerformanceTracker.Instance;
            
            // Create the parser
            _parser = new SimplifiedSqlParser();
            
            // Initialize domain components
            _graph = new LineageGraph();
            _contextManager = new ContextManager(_graph);
        }
        
        /// <summary>
        /// Creates a new lineage tracker with an existing graph
        /// </summary>
        public TSqlLineageTracker(LineageGraph graph)
        {
            // Initialize infrastructure
            _infrastructureService = InfrastructureService.Instance;
            _infrastructureService.Initialize(true);
            
            _performanceTracker = PerformanceTracker.Instance;
            
            // Create the parser
            _parser = new SimplifiedSqlParser();
            
            // Use provided graph
            _graph = graph ?? throw new ArgumentNullException(nameof(graph));
            _contextManager = new ContextManager(_graph);
        }
        
        /// <summary>
        /// Gets the current lineage graph
        /// </summary>
        public LineageGraph Graph => _graph;
        
        /// <summary>
        /// Analyzes a T-SQL script for column lineage
        /// </summary>
        public void AnalyzeScript(string sqlScript)
        {
            if (string.IsNullOrEmpty(sqlScript))
                throw new ArgumentException("SQL script cannot be null or empty", nameof(sqlScript));
                
            using var tracker = _performanceTracker.TrackOperation("LineageTracker", "AnalyzeScript");
            
            try
            {
                // Parse and extract lineage
                var lineage = _parser.ExtractLineage(sqlScript);
                
                // Merge with existing graph if needed
                if (_graph != lineage)
                {
                    MergeGraphs(lineage);
                }
                
                // Track source SQL
                _graph.SourceSql = sqlScript;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error analyzing SQL script: {ex.Message}", ex);
            }
        }
        
        /// <summary>
        /// Analyzes a T-SQL script for column lineage asynchronously
        /// </summary>
        public async Task AnalyzeScriptAsync(string sqlScript, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(sqlScript))
                throw new ArgumentException("SQL script cannot be null or empty", nameof(sqlScript));
                
            using var tracker = _performanceTracker.TrackOperation("LineageTracker", "AnalyzeScriptAsync");
            
            try
            {
                // Run the analysis on a background thread
                await Task.Run(() => AnalyzeScript(sqlScript), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                _performanceTracker.IncrementCounter("LineageTracker", "AnalysisCancelled");
                throw;
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("LineageTracker", "AnalysisErrors");
                throw new Exception($"Error analyzing SQL script asynchronously: {ex.Message}", ex);
            }
        }
        
        /// <summary>
        /// Analyzes multiple T-SQL scripts in parallel
        /// </summary>
        public async Task AnalyzeMultipleScriptsAsync(IReadOnlyList<string> sqlScripts, 
            CancellationToken cancellationToken = default)
        {
            if (sqlScripts == null || sqlScripts.Count == 0)
                throw new ArgumentException("SQL script list cannot be null or empty", nameof(sqlScripts));
                
            using var tracker = _performanceTracker.TrackOperation("LineageTracker", "AnalyzeMultipleScripts");
            
            try
            {
                // Process scripts in parallel using infrastructure
                await _infrastructureService.ProcessBatchAsync(
                    sqlScripts,
                    async (script, token) => {
                        // Create a separate graph for each script
                        var scriptGraph = _parser.ExtractLineage(script);
                        
                        // Merge with main graph
                        lock (_graph)
                        {
                            MergeGraphs(scriptGraph);
                        }
                        
                        return true;
                    },
                    "AnalyzeScripts",
                    null, // Use default batch size
                    cancellationToken
                );
            }
            catch (OperationCanceledException)
            {
                _performanceTracker.IncrementCounter("LineageTracker", "BatchAnalysisCancelled");
                throw;
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("LineageTracker", "BatchAnalysisErrors");
                throw new Exception($"Error analyzing multiple SQL scripts: {ex.Message}", ex);
            }
        }
        
        /// <summary>
        /// Gets diagnostics information about the tracker
        /// </summary>
        public LineageTrackerDiagnostics GetDiagnostics()
        {
            // Get graph statistics
            var graphStats = _graph.GetStatistics();
            
            // Get infrastructure diagnostics
            var infraDiags = _infrastructureService.GetDiagnostics();
            
            return new LineageTrackerDiagnostics
            {
                TotalNodes = graphStats.TotalNodes,
                ColumnNodes = graphStats.ColumnNodes,
                TableNodes = graphStats.TableNodes,
                ExpressionNodes = graphStats.ExpressionNodes,
                TotalEdges = graphStats.TotalEdges,
                DirectEdges = graphStats.DirectEdges,
                IndirectEdges = graphStats.IndirectEdges,
                JoinEdges = graphStats.JoinEdges,
                MemoryUsageMB = infraDiags.MemoryStatus.TotalMemoryMB,
                MemoryPressure = infraDiags.MemoryStatus.PressureLevel.ToString(),
                OperationCount = infraDiags.PerformanceStats.TotalOperations
            };
        }
        
        /// <summary>
        /// Merges a source graph into the main graph
        /// </summary>
        private void MergeGraphs(LineageGraph sourceGraph)
        {
            if (sourceGraph == null)
                return;
                
            using var tracker = _performanceTracker.TrackOperation("LineageTracker", "MergeGraphs");
            
            // TODO: Implement proper graph merging logic
            // This would involve matching nodes between graphs and creating
            // merged relationships. For now, we'll just use the single graph
            // returned by the parser.
        }
        
        /// <summary>
        /// Disposes resources
        /// </summary>
        public void Dispose()
        {
            _contextManager?.Dispose();
        }
    }
    
    /// <summary>
    /// Diagnostics information for the lineage tracker
    /// </summary>
    public class LineageTrackerDiagnostics
    {
        public int TotalNodes { get; set; }
        public int ColumnNodes { get; set; }
        public int TableNodes { get; set; }
        public int ExpressionNodes { get; set; }
        
        public int TotalEdges { get; set; }
        public int DirectEdges { get; set; }
        public int IndirectEdges { get; set; }
        public int JoinEdges { get; set; }
        
        public double MemoryUsageMB { get; set; }
        public required string MemoryPressure { get; set; }
        public long OperationCount { get; set; }
        
        public override string ToString()
        {
            return $"Nodes: {TotalNodes} ({ColumnNodes} columns, {TableNodes} tables, {ExpressionNodes} expressions)\n" +
                   $"Edges: {TotalEdges} ({DirectEdges} direct, {IndirectEdges} indirect, {JoinEdges} joins)\n" +
                   $"Memory: {MemoryUsageMB:F2} MB ({MemoryPressure})\n" +
                   $"Operations: {OperationCount:N0}";
        }
    }
}
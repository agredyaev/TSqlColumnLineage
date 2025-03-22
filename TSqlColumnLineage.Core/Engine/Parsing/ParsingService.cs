using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Domain.Context;
using TSqlColumnLineage.Core.Domain.Graph;
using TSqlColumnLineage.Core.Infrastructure;
using TSqlColumnLineage.Core.Infrastructure.Memory;
using TSqlColumnLineage.Core.Infrastructure.Monitoring;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// Main entry point for SQL parsing in the T-SQL column lineage tracker.
    /// Implements data-oriented design for memory-efficient processing.
    /// </summary>
    public class ParsingService
    {
        // Default parsing options
        private static readonly ParsingOptions _defaultOptions = new();

        // Core services
        private readonly SqlParser _sqlParser;
        private readonly StreamingSqlParser _streamingParser;
        private readonly InfrastructureService _infrastructureService;
        private readonly PerformanceTracker _performanceTracker;
        private readonly MemoryManager _memoryManager;

        // Progress reporting
        private event EventHandler<ParsingProgressEventArgs> _progressChanged;

        /// <summary>
        /// Event raised when parsing progress changes
        /// </summary>
        public event EventHandler<ParsingProgressEventArgs> ProgressChanged
        {
            add 
            {
                _progressChanged += value; 
                _streamingParser.ProgressChanged += value;
            }
            remove 
            {
                _progressChanged -= value;
                _streamingParser.ProgressChanged -= value;
            }
        }

        /// <summary>
        /// Creates a new parsing service
        /// </summary>
        public ParsingService()
        {
            // Initialize parsers
            _sqlParser = new SqlParser();
            _streamingParser = new StreamingSqlParser();

            // Get infrastructure services
            _infrastructureService = InfrastructureService.Instance;
            _infrastructureService.Initialize();
            _performanceTracker = PerformanceTracker.Instance;
            _memoryManager = MemoryManager.Instance;
        }

        /// <summary>
        /// Parses a SQL script and returns a parsed script object
        /// </summary>
        public ParsedScript Parse(string scriptText, string source = "", ParsingOptions options = null)
        {
            options ??= _defaultOptions;

            // Check if we should use streaming parser for large scripts
            if (scriptText.Length > options.MaxFragmentSize * 10)
            {
                return ParseStreamingAsync(scriptText, source, options).GetAwaiter().GetResult();
            }

            // Use regular parser for smaller scripts
            return _sqlParser.Parse(scriptText, source, options);
        }

        /// <summary>
        /// Parses a SQL script asynchronously
        /// </summary>
        public Task<ParsedScript> ParseAsync(string scriptText, string source = "", ParsingOptions options = null, CancellationToken cancellationToken = default)
        {
            options ??= _defaultOptions;

            // Check if we should use streaming parser for large scripts
            if (scriptText.Length > options.MaxFragmentSize * 10)
            {
                return ParseStreamingAsync(scriptText, source, options, cancellationToken);
            }

            // Use regular parser for smaller scripts
            return _sqlParser.ParseAsync(scriptText, source, options, cancellationToken);
        }

        /// <summary>
        /// Parses a SQL script from a file asynchronously
        /// </summary>
        public async Task<ParsedScript> ParseFileAsync(string filePath, ParsingOptions options = null, CancellationToken cancellationToken = default)
        {
            options ??= _defaultOptions;

            using var perfTracker = _performanceTracker.TrackOperation("Parsing", "ParseFile");

            try
            {
                // Get file info
                var fileInfo = new FileInfo(filePath);
                string fileName = Path.GetFileName(filePath);

                // Check if we should use streaming parser for large files
                bool useStreaming = fileInfo.Length > options.MaxFragmentSize * 10;

                // Read the file
                string scriptText = await File.ReadAllTextAsync(filePath, cancellationToken);

                if (useStreaming)
                {
                    return await ParseStreamingAsync(scriptText, fileName, options, cancellationToken);
                }
                else
                {
                    return await _sqlParser.ParseAsync(scriptText, fileName, options, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("Parsing", "FileErrors");
                throw new ParsingException($"Error parsing file {filePath}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Parses a large SQL script using streaming to minimize memory usage
        /// </summary>
        public async Task<ParsedScript> ParseStreamingAsync(string scriptText, string source = "", ParsingOptions options = null, CancellationToken cancellationToken = default)
        {
            options ??= _defaultOptions;

            using var perfTracker = _performanceTracker.TrackOperation("Parsing", "StreamingParse");

            // Create result containers
            var batches = new List<SqlFragment>();
            var errors = new List<ParseError>();

            try
            {
                // Parse the script with callbacks
                await _streamingParser.ParseStreamingAsync(
                    scriptText,
                    source,
                    // Batch callback
                    batch => 
                    {
                        batches.Add(batch);
                    },
                    // Statement callback
                    statement => 
                    {
                        // No need to do anything with statements here
                    },
                    options,
                    cancellationToken
                );

                // Create parsed script from batches
                var result = ParsedScript.FromBatches(batches, scriptText, source);
                result.Errors = errors;

                // Track performance
                _performanceTracker.IncrementCounter("Parsing", "TotalScripts");
                _performanceTracker.IncrementCounter("Parsing", "TotalFragments", result.TotalFragmentCount);

                return result;
            }
            catch (OperationCanceledException)
            {
                _performanceTracker.IncrementCounter("Parsing", "Cancelled");
                throw;
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("Parsing", "Errors");
                _performanceTracker.IncrementCounter("Parsing", "ExceptionErrors");

                throw new ParsingException($"Error parsing script: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Processes a parsed script to extract lineage information
        /// </summary>
        public Task ProcessLineageAsync(ParsedScript parsedScript, LineageGraph graph, ContextManager? contextManager = null, 
            CancellationToken cancellationToken = default)
        {
            if (parsedScript == null || graph == null)
                throw new ArgumentNullException(nameof(parsedScript));

            using var perfTracker = _performanceTracker.TrackOperation("Parsing", "ProcessLineage");

            try
            {
                // Create context manager if not provided
                bool ownsContextManager = contextManager == null;
                contextManager ??= new ContextManager(graph);

                try
                {
                    // Store source SQL in graph
                    graph.SourceSql = parsedScript.ScriptText;
                    graph.SetMetadata("Source", parsedScript.Source);

                    // Track tables and columns for each fragment
                    foreach (var fragment in parsedScript.Fragments)
                    {
                        // Check for cancellation
                        cancellationToken.ThrowIfCancellationRequested();

                        // Create query context for this fragment
                        using var queryContext = StreamingSqlParser.CreateQueryContext(fragment, contextManager);

                        // Process table references
                        foreach (var tableRef in fragment.TableReferences)
                        {
                            // Create table if it doesn't exist
                            int tableId = contextManager.GetTableId(tableRef.TableName);
                            if (tableId < 0)
                            {
                                // Create table in the graph
                                tableId = graph.AddTableNode(
                                    tableRef.TableName,
                                    tableRef.ReferenceType.ToString());

                                // Register it
                                contextManager.RegisterTable(tableRef.TableName, tableId);
                            }

                            // Add alias if present
                            if (!string.IsNullOrEmpty(tableRef.Alias))
                            {
                                queryContext?.AddLocalAlias(tableRef.Alias, tableRef.TableName);
                            }
                        }

                        // Process column references
                        foreach (var colRef in fragment.ColumnReferences)
                        {
                            int columnId;

                            // Determine table for this column
                            string tableName = colRef.TableName;

                            if (string.IsNullOrEmpty(tableName))
                            {
                                // Try to infer table from context
                                // This would be handled by the Analysis module
                                continue;
                            }

                            // Check if column exists in the graph
                            columnId = contextManager.GetColumnNode(tableName, colRef.ColumnName);

                            if (columnId < 0)
                            {
                                // Create column node
                                columnId = graph.AddColumnNode(colRef.ColumnName, tableName);

                                // Add to table if the table exists
                                int tableId = contextManager.GetTableId(tableName);
                                if (tableId >= 0)
                                {
                                    graph.AddColumnToTable(tableId, columnId);
                                }
                            }

                            // Track source/target in query context
                            if (queryContext != null)
                            {
                                if (colRef.IsSource)
                                {
                                    queryContext.AddInputTable(tableName, contextManager.GetTableId(tableName));
                                }

                                if (colRef.IsTarget)
                                {
                                    queryContext.AddOutputColumn(tableName, columnId);
                                }
                            }
                        }
                    }

                    // Track performance metrics
                    var stats = graph.GetStatistics();
                    _performanceTracker.IncrementCounter("Parsing", "TotalColumns", stats.ColumnNodes);
                    _performanceTracker.IncrementCounter("Parsing", "TotalTables", stats.TableNodes);
                }
                finally
                {
                    // Clean up if we created the context manager
                    if (ownsContextManager)
                    {
                        contextManager.Dispose();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _performanceTracker.IncrementCounter("Parsing", "Cancelled");
                throw;
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("Parsing", "LineageErrors");
                throw new ParsingException($"Error processing lineage: {ex.Message}", ex);
            }

            return Task.CompletedTask;
        }

        /// <summary>
        /// Reports parsing progress
        /// </summary>
        private void ReportProgress(double progressPercentage, string message)
        {
            if (_progressChanged != null)
            {
                var args = new ParsingProgressEventArgs
                {
                    ProgressPercentage = progressPercentage,
                    Message = message
                };
                
                _progressChanged(this, args);
            }
        }
    }

    /// <summary>
    /// Exception thrown during parsing
    /// </summary>
    public class ParsingException : Exception
    {
        public ParsingException(string message) : base(message)
        {
        }

        public ParsingException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Domain.Context;
using TSqlColumnLineage.Core.Infrastructure.Concurency;
using TSqlColumnLineage.Core.Infrastructure.Memory;
using TSqlColumnLineage.Core.Infrastructure.Monitoring;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// Streaming parser for processing large T-SQL scripts with minimal memory footprint.
    /// Implements memory-efficient parsing with callbacks for batches and statements.
    /// </summary>
    public class StreamingSqlParser
    {
        // Default options
        private static readonly ParsingOptions _defaultOptions = new();

        // Performance tracking
        private readonly PerformanceTracker _performanceTracker;

        // Memory manager
        private readonly MemoryManager _memoryManager;

        // Batch operation manager for parallel processing
        private readonly BatchOperationManager _batchManager;

        // Progress reporting
        private event EventHandler<ParsingProgressEventArgs> _progressChanged;

        /// <summary>
        /// Event raised when parsing progress changes
        /// </summary>
        public event EventHandler<ParsingProgressEventArgs> ProgressChanged
        {
            add => _progressChanged += value;
            remove => _progressChanged -= value;
        }

        /// <summary>
        /// Creates a new streaming parser
        /// </summary>
        public StreamingSqlParser()
        {
            _performanceTracker = PerformanceTracker.Instance;
            _memoryManager = MemoryManager.Instance;
            _batchManager = BatchOperationManager.Instance;
        }

        /// <summary>
        /// Parses a T-SQL script in streaming mode with callbacks
        /// </summary>
        public async Task ParseStreamingAsync(string scriptText, string source, 
            Action<SqlFragment> batchCallback, Action<SqlFragment> statementCallback,
            ParsingOptions options = null, CancellationToken cancellationToken = default)
        {
            options ??= _defaultOptions;

            // Track performance
            using var perfTracker = _performanceTracker.TrackOperation("Parsing", "StreamingSqlParser");

            // Initialize parser
            TSqlParser parser = null;
            int fragmentCount = 0;
            int batchCount = 0;
            int errorCount = 0;

            try
            {
                parser = ParserFactory.CreateParser(options);
                IList<TSqlParserToken> tokenStream = ParserFactory.CreateTokenStream();

                // Parse the script and process batches efficiently
                using (var reader = new StringReader(scriptText))
                {
                    // Get token stream
                    IList<ParseError> tokenErrors;
                    parser.GetTokenStream(reader, tokenStream, out tokenErrors);
                    errorCount += tokenErrors.Count;

                    // Split script into batches based on GO separator
                    var batchSql = new StringBuilder();
                    int batchStartOffset = 0;
                    int batchLineNumber = 1;
                    int batchColumnNumber = 1;
                    int totalBatches = CountBatches(tokenStream);

                    // Report initial progress
                    ReportProgress(0, totalBatches, fragmentCount, errorCount);

                    // Process accumulated batch
                    async Task ProcessAccumulatedBatchAsync()
                    {
                        if (batchSql.Length == 0) return;

                        // Parse this batch
                        string batchText = batchSql.ToString();
                        TSqlFragment batchAst;
                        IList<ParseError> batchErrors;

                        using (var batchReader = new StringReader(batchText))
                        {
                            batchAst = parser.Parse(batchReader, out batchErrors);
                        }

                        errorCount += batchErrors.Count;

                        // Process batch if parsing succeeded
                        if (batchAst != null)
                        {
                            // Process batch
                            var batchFragment = ProcessBatch(batchAst, batchText, batchStartOffset, 
                                batchLineNumber, batchColumnNumber, options);
                            
                            // Update metrics
                            batchCount++;
                            fragmentCount += batchFragment.Children.Count + 1;

                            // Invoke callback
                            batchCallback?.Invoke(batchFragment);

                            // Process statements in parallel if there are many
                            if (batchFragment.Children.Count > 10)
                            {
                                // Process statements in parallel
                                await _batchManager.ProcessBatchAsync(
                                    batchFragment.Children,
                                    (fragment, token) => 
                                    {
                                        statementCallback?.Invoke(fragment);
                                        return Task.FromResult(true);
                                    },
                                    "ProcessStatements",
                                    null,
                                    cancellationToken
                                );
                            }
                            else
                            {
                                // Process statements sequentially
                                foreach (var fragment in batchFragment.Children)
                                {
                                    statementCallback?.Invoke(fragment);
                                }
                            }

                            // Report progress
                            ReportProgress(batchCount, totalBatches, fragmentCount, errorCount);
                        }

                        // Clear the buffer
                        batchSql.Clear();
                    }

                    // Process tokens to find batch separators
                    foreach (var token in tokenStream)
                    {
                        // Check for cancellation
                        cancellationToken.ThrowIfCancellationRequested();

                        if (token.TokenType == TSqlTokenType.Go)
                        {
                            // Process accumulated batch
                            await ProcessAccumulatedBatchAsync();

                            // Update batch start position
                            batchStartOffset = token.Offset + token.Text.Length;
                            batchLineNumber = token.Line;
                            batchColumnNumber = token.Column + token.Text.Length;
                        }
                        else
                        {
                            // Append to current batch
                            batchSql.Append(token.Text);
                        }
                    }

                    // Process the last batch if any
                    await ProcessAccumulatedBatchAsync();
                }

                // Track performance
                _performanceTracker.IncrementCounter("Parsing", "TotalScripts");
                _performanceTracker.IncrementCounter("Parsing", "TotalFragments", fragmentCount);
                _performanceTracker.IncrementCounter("Parsing", "TotalBatches", batchCount);
                _performanceTracker.IncrementCounter("Parsing", "Errors", errorCount);
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
                
                // Report error through progress
                ReportProgress(batchCount, batchCount, fragmentCount, errorCount + 1, ex.Message);
                
                throw;
            }
            finally
            {
                // Return parser to pool
                if (parser != null)
                {
                    ParserFactory.ReturnParser(parser);
                }
            }
        }

        /// <summary>
        /// Processes a parsed batch in streaming mode
        /// </summary>
        private SqlFragment ProcessBatch(TSqlFragment batchAst, string batchText, int startOffset, int lineNumber, int columnNumber, ParsingOptions options)
        {
            // Create batch fragment
            var batchFragment = new SqlFragment
            {
                FragmentType = SqlFragmentType.Batch,
                SqlText = batchText,
                StartOffset = batchAst.StartOffset,
                EndOffset = batchAst.EndOffset,
                LineNumber = batchAst.StartLine,
                ColumnNumber = batchAst.StartColumn,
                Statement = "BATCH",
                ParsedFragment = batchAst,
                Children = [],
                TableReferences = []
            };

            // Process statements in batch
            if (batchAst is TSqlBatch batch)
            {
                foreach (var statement in batch.Statements)
                {
                    // Process fragment
                    var fragment = ProcessFragment(statement, batchText, statement.StartOffset, 
                        statement.StartLine, statement.StartColumn, options);
                    
                    batchFragment.Children.Add(fragment);
                    fragment.ParentBatch = batchFragment;
                    
                    // Collect table references from children
                    batchFragment.TableReferences.AddRange(fragment.TableReferences);
                }
            }

            return batchFragment;
        }

        /// <summary>
        /// Processes a fragment in streaming mode
        /// </summary>
        private SqlFragment ProcessFragment(TSqlFragment fragmentAst, string scriptText, int startOffset, int lineNumber, int columnNumber, ParsingOptions options)
        {
            // Determine fragment type
            SqlFragmentType fragmentType = ReferenceExtractor.DetermineFragmentType(fragmentAst);

            // Extract statement text
            string fragmentText = ExtractFragmentText(scriptText, fragmentAst.StartOffset, fragmentAst.EndOffset);
            
            // Determine statement type name
            string statementType = fragmentAst.GetType().Name;
            
            // Create fragment
            var fragment = new SqlFragment
            {
                FragmentType = fragmentType,
                SqlText = fragmentText,
                StartOffset = fragmentAst.StartOffset,
                EndOffset = fragmentAst.EndOffset,
                LineNumber = fragmentAst.StartLine,
                ColumnNumber = fragmentAst.StartColumn,
                Statement = statementType,
                ParsedFragment = fragmentAst,
                Children = [],
                TableReferences = [],
                ColumnReferences = []
            };

            // Extract table references if enabled
            if (options.ExtractTableReferences)
            {
                fragment.TableReferences = ReferenceExtractor.ExtractTableReferences(fragmentAst, options);
            }

            // Extract column references if enabled
            if (options.ExtractColumnReferences)
            {
                fragment.ColumnReferences = ReferenceExtractor.ExtractColumnReferences(fragmentAst, options, fragment.TableReferences);
            }

            return fragment;
        }

        /// <summary>
        /// Extracts the text of a fragment from the original script
        /// </summary>
        private string ExtractFragmentText(string scriptText, int startOffset, int endOffset)
        {
            if (string.IsNullOrEmpty(scriptText) || startOffset < 0 || endOffset <= startOffset || endOffset > scriptText.Length)
                return string.Empty;

            return scriptText.Substring(startOffset, endOffset - startOffset);
        }

        /// <summary>
        /// Counts the number of batches in a token stream
        /// </summary>
        private int CountBatches(IList<TSqlParserToken> tokenStream)
        {
            int batchCount = 1; // At least one batch
            
            foreach (var token in tokenStream)
            {
                if (token.TokenType == TSqlTokenType.Go)
                {
                    batchCount++;
                }
            }
            
            return batchCount;
        }

        /// <summary>
        /// Reports parsing progress
        /// </summary>
        private void ReportProgress(int currentBatch, int totalBatches, int fragmentCount, int errorCount, string message = null)
        {
            if (_progressChanged != null)
            {
                double progressPercentage = totalBatches > 0 ? (double)currentBatch / totalBatches * 100 : 0;
                
                var args = new ParsingProgressEventArgs
                {
                    ProgressPercentage = progressPercentage,
                    CurrentBatchIndex = currentBatch,
                    TotalBatches = totalBatches,
                    FragmentCount = fragmentCount,
                    ErrorCount = errorCount,
                    Message = message ?? $"Processed {currentBatch} of {totalBatches} batches, {fragmentCount} fragments"
                };
                
                _progressChanged(this, args);
            }
        }

        /// <summary>
        /// Gets the context for a fragment 
        /// </summary>
        public static QueryContext CreateQueryContext(SqlFragment fragment, ContextManager contextManager)
        {
            if (fragment == null || contextManager == null)
                return null;

            // Create query context
            var queryContext = new QueryContext(contextManager, fragment.SqlText);

            // Add table references
            foreach (var tableRef in fragment.TableReferences)
            {
                // Create the table in the context if it doesn't exist
                int tableId = contextManager.GetTableId(tableRef.TableName);
                if (tableId < 0)
                {
                    // Create table in the graph
                    tableId = contextManager.Graph.AddTableNode(
                        tableRef.TableName,
                        tableRef.ReferenceType.ToString());

                    // Register it
                    contextManager.RegisterTable(tableRef.TableName, tableId);
                }

                // Register alias if present
                if (!string.IsNullOrEmpty(tableRef.Alias))
                {
                    contextManager.AddTableAlias(tableRef.Alias, tableRef.TableName);
                }
            }

            return queryContext;
        }
    }

    /// <summary>
    /// Event arguments for parsing progress
    /// </summary>
    public class ParsingProgressEventArgs : EventArgs
    {
        /// <summary>
        /// Gets or sets the progress percentage (0-100)
        /// </summary>
        public double ProgressPercentage { get; set; }

        /// <summary>
        /// Gets or sets the current batch index being processed
        /// </summary>
        public int CurrentBatchIndex { get; set; }

        /// <summary>
        /// Gets or sets the total number of batches
        /// </summary>
        public int TotalBatches { get; set; }

        /// <summary>
        /// Gets or sets the number of fragments processed
        /// </summary>
        public int FragmentCount { get; set; }

        /// <summary>
        /// Gets or sets the number of errors encountered
        /// </summary>
        public int ErrorCount { get; set; }

        /// <summary>
        /// Gets or sets the progress message
        /// </summary>
        public string Message { get; set; }
    }
}
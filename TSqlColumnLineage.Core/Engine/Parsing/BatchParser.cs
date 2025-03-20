using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Infrastructure.Concurency;
using TSqlColumnLineage.Core.Infrastructure.Memory;
using TSqlColumnLineage.Core.Infrastructure.Monitoring;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// Performs parallel batch-oriented parsing of large T-SQL scripts.
    /// Optimized for performance using concurrent processing and memory efficiency.
    /// </summary>
    public class BatchParser
    {
        // Default options
        private static readonly ParsingOptions _defaultOptions = new();

        // Infrastructure services
        private readonly BatchOperationManager _batchManager;
        private readonly PerformanceTracker _performanceTracker;
        private readonly MemoryManager _memoryManager;

        // Progress tracking
        private event EventHandler<BatchParsingProgressEventArgs> _progressChanged;

        /// <summary>
        /// Event raised when batch parsing progress changes
        /// </summary>
        public event EventHandler<BatchParsingProgressEventArgs> ProgressChanged
        {
            add => _progressChanged += value;
            remove => _progressChanged -= value;
        }

        /// <summary>
        /// Creates a new batch parser
        /// </summary>
        public BatchParser()
        {
            _batchManager = BatchOperationManager.Instance;
            _performanceTracker = PerformanceTracker.Instance;
            _memoryManager = MemoryManager.Instance;
        }

        /// <summary>
        /// Parses a large T-SQL script in parallel batches
        /// </summary>
        public async Task<ParsedScript> ParseBatchesAsync(string scriptText, string source = "", 
            ParsingOptions options = null, CancellationToken cancellationToken = default)
        {
            options ??= _defaultOptions;

            using var perfTracker = _performanceTracker.TrackOperation("Parsing", "ParseBatches");

            try
            {
                // First, split the script into batches
                var batches = SplitIntoBatches(scriptText);
                
                // Report initial progress
                ReportProgress(0, batches.Count, 0, "Starting parallel batch parsing");

                // Process batches in parallel
                var batchResults = await _batchManager.ProcessBatchAsync(
                    batches,
                    async (batch, token) =>
                    {
                        // Parse the batch
                        var parser = ParserFactory.CreateParser(options);
                        try
                        {
                            using var reader = new StringReader(batch.SqlText);
                            TSqlFragment ast = parser.Parse(reader, out IList<ParseError> errors);

                            // Adjust error positions
                            foreach (var error in errors)
                            {
                                error.Line += batch.LineNumber - 1;
                                error.StartOffset += batch.StartOffset;
                                error.EndOffset += batch.StartOffset;
                            }

                            return new BatchParseResult
                            {
                                BatchIndex = batch.BatchIndex,
                                StartOffset = batch.StartOffset,
                                EndOffset = batch.StartOffset + batch.SqlText.Length,
                                LineNumber = batch.LineNumber,
                                ColumnNumber = batch.ColumnNumber,
                                SqlText = batch.SqlText,
                                Ast = ast,
                                Errors = errors.ToList(),
                                Success = ast != null
                            };
                        }
                        finally
                        {
                            ParserFactory.ReturnParser(parser);
                        }
                    },
                    "ParseBatch",
                    null,
                    cancellationToken
                );

                if (batchResults.ErrorCount > 0)
                {
                    // Log parsing errors
                    _performanceTracker.IncrementCounter("Parsing", "BatchErrors", batchResults.ErrorCount);
                }

                // Post-process batch results
                return await ProcessBatchResultsAsync(scriptText, source, options, batchResults.Results, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                _performanceTracker.IncrementCounter("Parsing", "Cancelled");
                throw;
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("Parsing", "Errors");
                throw new ParsingException($"Error in batch parsing: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Processes batch parsing results into a complete ParsedScript
        /// </summary>
        private async Task<ParsedScript> ProcessBatchResultsAsync(string scriptText, string source, 
            ParsingOptions options, List<BatchParseResult> batchResults, CancellationToken cancellationToken)
        {
            using var perfTracker = _performanceTracker.TrackOperation("Parsing", "ProcessBatchResults");

            // Order results by batch index
            batchResults = batchResults.OrderBy(r => r.BatchIndex).ToList();

            // Collect errors
            var allErrors = new List<ParseError>();
            foreach (var result in batchResults)
            {
                allErrors.AddRange(result.Errors);
            }

            // Process successful batch results
            var sqlParser = new SqlParser();
            var fragments = new List<SqlFragment>();

            // Process each batch
            for (int i = 0; i < batchResults.Count; i++)
            {
                var result = batchResults[i];
                
                // Skip failed batches
                if (!result.Success || result.Ast == null)
                    continue;

                // Extract batch fragments
                var batchText = result.SqlText;
                var batchAst = result.Ast;
                
                // Process batch
                if (batchAst is TSqlBatch batch)
                {
                    var batchFragment = new SqlFragment
                    {
                        FragmentType = SqlFragmentType.Batch,
                        SqlText = batchText,
                        StartOffset = result.StartOffset,
                        EndOffset = result.EndOffset,
                        LineNumber = result.LineNumber,
                        ColumnNumber = result.ColumnNumber,
                        Statement = "BATCH",
                        ParsedFragment = batchAst,
                        Children = []
                    };

                    // Extract table and column references
                    if (options.ExtractTableReferences)
                    {
                        batchFragment.TableReferences = ReferenceExtractor.ExtractTableReferences(batchAst, options);
                    }

                    // Process each statement in the batch
                    foreach (var statement in batch.Statements)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        // Determine fragment type and extract references
                        var fragmentType = ReferenceExtractor.DetermineFragmentType(statement);
                        string statementText = ExtractFragmentText(batchText, statement.StartOffset, statement.EndOffset);
                        string statementType = statement.GetType().Name;

                        var fragment = new SqlFragment
                        {
                            FragmentType = fragmentType,
                            SqlText = statementText,
                            StartOffset = result.StartOffset + statement.StartOffset,
                            EndOffset = result.StartOffset + statement.EndOffset,
                            LineNumber = statement.StartLine,
                            ColumnNumber = statement.StartColumn,
                            Statement = statementType,
                            ParsedFragment = statement,
                            Children = [],
                            ParentBatch = batchFragment
                        };

                        // Extract references
                        if (options.ExtractTableReferences)
                        {
                            fragment.TableReferences = ReferenceExtractor.ExtractTableReferences(statement, options);
                        }

                        if (options.ExtractColumnReferences)
                        {
                            fragment.ColumnReferences = ReferenceExtractor.ExtractColumnReferences(
                                statement, options, fragment.TableReferences);
                        }

                        // Add to batch
                        batchFragment.Children.Add(fragment);
                    }

                    // Add batch fragment
                    fragments.Add(batchFragment);
                }

                // Report progress
                ReportProgress(i + 1, batchResults.Count, fragments.Count, 
                    $"Processed batch {i + 1} of {batchResults.Count}");
            }

            // Create parsed script
            var result = new ParsedScript
            {
                ScriptText = scriptText,
                Source = source,
                Batches = fragments,
                Fragments = CollectAllFragments(fragments),
                Errors = allErrors
            };

            // Create lookup structures
            result.TableReferences = CreateTableReferenceLookup(result.Fragments);
            result.ColumnReferences = CreateColumnReferenceLookup(result.Fragments);

            return result;
        }

        /// <summary>
        /// Splits a SQL script into batches
        /// </summary>
        private List<SqlBatch> SplitIntoBatches(string scriptText)
        {
            var batches = new List<SqlBatch>();
            var batchSql = new StringBuilder();
            int batchStartOffset = 0;
            int batchLineNumber = 1;
            int batchColumnNumber = 1;
            int batchIndex = 0;

            // Use token scanner to identify batch separators
            var parser = ParserFactory.CreateParser(new ParsingOptions());
            try
            {
                using var reader = new StringReader(scriptText);
                var tokenStream = ParserFactory.CreateTokenStream();
                parser.GetTokenStream(reader, tokenStream, out _);

                void AddBatch()
                {
                    if (batchSql.Length > 0)
                    {
                        batches.Add(new SqlBatch
                        {
                            BatchIndex = batchIndex++,
                            SqlText = batchSql.ToString(),
                            StartOffset = batchStartOffset,
                            LineNumber = batchLineNumber,
                            ColumnNumber = batchColumnNumber
                        });
                        batchSql.Clear();
                    }
                }

                // Process each token
                foreach (var token in tokenStream)
                {
                    if (token.TokenType == TSqlTokenType.Go)
                    {
                        // End of batch
                        AddBatch();

                        // Update tracking info for next batch
                        batchStartOffset = token.Offset + token.Text.Length;
                        
                        // Count lines in the GO statement
                        int lineCount = CountLines(token.Text, out int lastLineLength);
                        batchLineNumber = token.Line + lineCount;
                        batchColumnNumber = lineCount > 0 ? lastLineLength + 1 : token.Column + token.Text.Length;
                    }
                    else
                    {
                        // Add token to current batch
                        batchSql.Append(token.Text);
                    }
                }

                // Add the last batch
                AddBatch();
            }
            finally
            {
                ParserFactory.ReturnParser(parser);
            }

            // If no batches were found, add the entire script as a single batch
            if (batches.Count == 0 && !string.IsNullOrWhiteSpace(scriptText))
            {
                batches.Add(new SqlBatch
                {
                    BatchIndex = 0,
                    SqlText = scriptText,
                    StartOffset = 0,
                    LineNumber = 1,
                    ColumnNumber = 1
                });
            }

            return batches;
        }

        /// <summary>
        /// Counts the number of lines in text
        /// </summary>
        private int CountLines(string text, out int lastLineLength)
        {
            if (string.IsNullOrEmpty(text))
            {
                lastLineLength = 0;
                return 0;
            }

            int lines = 0;
            int pos = 0;
            int lineStart = 0;

            while (pos < text.Length)
            {
                if (text[pos] == '\n')
                {
                    lines++;
                    lineStart = pos + 1;
                }
                pos++;
            }

            lastLineLength = pos - lineStart;
            return lines;
        }

        /// <summary>
        /// Collects all fragments from batches recursively
        /// </summary>
        private List<SqlFragment> CollectAllFragments(List<SqlFragment> batches)
        {
            var allFragments = new List<SqlFragment>();
            
            void CollectFragments(SqlFragment fragment)
            {
                allFragments.Add(fragment);
                foreach (var child in fragment.Children)
                {
                    CollectFragments(child);
                }
            }
            
            foreach (var batch in batches)
            {
                CollectFragments(batch);
            }
            
            return allFragments;
        }

        /// <summary>
        /// Creates a lookup table for table references
        /// </summary>
        private Dictionary<string, List<TableReference>> CreateTableReferenceLookup(List<SqlFragment> fragments)
        {
            var lookup = new Dictionary<string, List<TableReference>>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var fragment in fragments)
            {
                foreach (var tableRef in fragment.TableReferences)
                {
                    if (!lookup.TryGetValue(tableRef.TableName, out var tableRefs))
                    {
                        tableRefs = [];
                        lookup[tableRef.TableName] = tableRefs;
                    }
                    
                    tableRefs.Add(tableRef);
                }
            }
            
            return lookup;
        }

        /// <summary>
        /// Creates a lookup table for column references
        /// </summary>
        private Dictionary<string, List<ColumnReference>> CreateColumnReferenceLookup(List<SqlFragment> fragments)
        {
            var lookup = new Dictionary<string, List<ColumnReference>>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var fragment in fragments)
            {
                foreach (var colRef in fragment.ColumnReferences)
                {
                    string key = string.IsNullOrEmpty(colRef.TableName) ? 
                        colRef.ColumnName : $"{colRef.TableName}.{colRef.ColumnName}";
                    
                    if (!lookup.TryGetValue(key, out var colRefs))
                    {
                        colRefs = [];
                        lookup[key] = colRefs;
                    }
                    
                    colRefs.Add(colRef);
                }
            }
            
            return lookup;
        }

        /// <summary>
        /// Extracts a fragment of text
        /// </summary>
        private string ExtractFragmentText(string text, int startOffset, int endOffset)
        {
            if (string.IsNullOrEmpty(text) || startOffset < 0 || endOffset <= startOffset || endOffset > text.Length)
                return string.Empty;

            return text.Substring(startOffset, endOffset - startOffset);
        }

        /// <summary>
        /// Reports parsing progress
        /// </summary>
        private void ReportProgress(int currentBatch, int totalBatches, int fragmentCount, string message)
        {
            if (_progressChanged != null)
            {
                double progress = totalBatches > 0 ? (double)currentBatch / totalBatches * 100 : 0;
                
                var args = new BatchParsingProgressEventArgs
                {
                    ProgressPercentage = progress,
                    CurrentBatch = currentBatch,
                    TotalBatches = totalBatches,
                    FragmentCount = fragmentCount,
                    Message = message
                };
                
                _progressChanged(this, args);
            }
        }

        /// <summary>
        /// Represents a SQL batch for processing
        /// </summary>
        private class SqlBatch
        {
            public int BatchIndex { get; set; }
            public string SqlText { get; set; } = string.Empty;
            public int StartOffset { get; set; }
            public int LineNumber { get; set; }
            public int ColumnNumber { get; set; }
        }

        /// <summary>
        /// Represents the result of parsing a batch
        /// </summary>
        private class BatchParseResult
        {
            public int BatchIndex { get; set; }
            public int StartOffset { get; set; }
            public int EndOffset { get; set; }
            public int LineNumber { get; set; }
            public int ColumnNumber { get; set; }
            public string SqlText { get; set; } = string.Empty;
            public TSqlFragment Ast { get; set; }
            public List<ParseError> Errors { get; set; } = [];
            public bool Success { get; set; }
        }
    }

    /// <summary>
    /// Event arguments for batch parsing progress
    /// </summary>
    public class BatchParsingProgressEventArgs : EventArgs
    {
        /// <summary>
        /// Gets or sets the progress percentage (0-100)
        /// </summary>
        public double ProgressPercentage { get; set; }

        /// <summary>
        /// Gets or sets the current batch being processed
        /// </summary>
        public int CurrentBatch { get; set; }

        /// <summary>
        /// Gets or sets the total number of batches
        /// </summary>
        public int TotalBatches { get; set; }

        /// <summary>
        /// Gets or sets the number of fragments processed
        /// </summary>
        public int FragmentCount { get; set; }

        /// <summary>
        /// Gets or sets the progress message
        /// </summary>
        public string Message { get; set; } = string.Empty;
    }
}
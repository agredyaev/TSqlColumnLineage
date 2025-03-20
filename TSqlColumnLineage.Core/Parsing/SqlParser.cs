using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Parsing.Exceptions;

namespace TSqlColumnLineage.Core.Parsing
{
    /// <summary>
    /// Enhanced parser for T-SQL scripts with improved performance and memory efficiency
    /// </summary>
    public sealed class SqlParser
    {
        private readonly SqlServerVersion _sqlServerVersion;
        private readonly bool _initialQuotedIdentifiers;
        private readonly int _batchSizeLimitBytes;
        private readonly ILogger _logger;
        private readonly StringPool _stringPool;
        
        // Performance statistics
        private int _totalParseCount = 0;
        private long _totalParseTimeMs = 0;
        private int _totalScriptSizeBytes = 0;
        private int _maxScriptSizeBytes = 0;
        
        // Object pool for script generators to reduce allocations
        private readonly ObjectPool<Sql160ScriptGenerator> _scriptGeneratorPool;

        /// <summary>
        /// Initializes a new instance of the SqlParser
        /// </summary>
        /// <param name="sqlServerVersion">SQL Server version for parsing</param>
        /// <param name="stringPool">String pool for memory optimization</param>
        /// <param name="initialQuotedIdentifiers">Indicates if quoted identifiers are initially enabled</param>
        /// <param name="batchSizeLimitBytes">Maximum size of batch in bytes (0 for no limit)</param>
        /// <param name="logger">Logger for diagnostic information</param>
        public SqlParser(
            SqlServerVersion sqlServerVersion,
            StringPool stringPool,
            bool initialQuotedIdentifiers = true,
            int batchSizeLimitBytes = 10 * 1024 * 1024,  // Default 10MB
            ILogger logger = null)
        {
            _sqlServerVersion = sqlServerVersion;
            _initialQuotedIdentifiers = initialQuotedIdentifiers;
            _batchSizeLimitBytes = batchSizeLimitBytes;
            _logger = logger;
            _stringPool = stringPool ?? throw new ArgumentNullException(nameof(stringPool));
            
            // Initialize the script generator pool
            _scriptGeneratorPool = new ObjectPool<Sql160ScriptGenerator>(
                () => new Sql160ScriptGenerator(),
                initialCount: 2,
                maxObjects: 10);
        }
        
        /// <summary>
        /// Gets parser performance statistics
        /// </summary>
        public (int ParseCount, long TotalTimeMs, double AvgTimeMs, int TotalSizeBytes, int MaxSizeBytes) 
            GetPerformanceStatistics() =>
            (
                _totalParseCount,
                _totalParseTimeMs,
                _totalParseCount > 0 ? (double)_totalParseTimeMs / _totalParseCount : 0,
                _totalScriptSizeBytes,
                _maxScriptSizeBytes
            );

        /// <summary>
        /// Parses a SQL query and returns the root AST fragment
        /// </summary>
        /// <param name="sqlQuery">SQL query to parse</param>
        /// <returns>Root fragment of the abstract syntax tree</returns>
        /// <exception cref="SqlParsingException">Thrown when parsing errors occur</exception>
        public TSqlFragment Parse(string sqlQuery)
        {
            if (string.IsNullOrWhiteSpace(sqlQuery))
                throw new ArgumentException("SQL query cannot be null or empty", nameof(sqlQuery));
                
            // Start performance tracking
            var startTime = DateTime.UtcNow;
            var scriptSizeBytes = Encoding.UTF8.GetByteCount(sqlQuery);
            
            // Check batch size limit
            if (_batchSizeLimitBytes > 0 && scriptSizeBytes > _batchSizeLimitBytes)
            {
                throw new SqlParsingException(
                    $"SQL script exceeds maximum batch size of {_batchSizeLimitBytes} bytes",
                    new List<ParseError>(),
                    sqlQuery
                );
            }

            TSqlParser parser = CreateParser();
            IList<ParseError> errors = new List<ParseError>();

            using (TextReader reader = new StringReader(sqlQuery))
            {
                TSqlFragment result = parser.Parse(reader, out errors);
                
                // Update performance stats
                Interlocked.Increment(ref _totalParseCount);
                Interlocked.Add(ref _totalParseTimeMs, (long)(DateTime.UtcNow - startTime).TotalMilliseconds);
                Interlocked.Add(ref _totalScriptSizeBytes, scriptSizeBytes);
                
                // Track maximum script size
                var currentMax = _maxScriptSizeBytes;
                while (scriptSizeBytes > currentMax)
                {
                    var newMax = Interlocked.CompareExchange(ref _maxScriptSizeBytes, scriptSizeBytes, currentMax);
                    if (newMax == currentMax) break;
                    currentMax = newMax;
                }

                if (errors.Count > 0)
                {
                    throw new SqlParsingException("SQL parsing failed", errors, sqlQuery);
                }

                _logger?.LogDebug($"Parsed SQL query ({scriptSizeBytes/1024} KB) in {(DateTime.UtcNow - startTime).TotalMilliseconds}ms");
                return result;
            }
        }
        
        /// <summary>
        /// Asynchronously parses a SQL query
        /// </summary>
        /// <param name="sqlQuery">SQL query to parse</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Task containing the root AST fragment</returns>
        public async Task<TSqlFragment> ParseAsync(string sqlQuery, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(sqlQuery))
                throw new ArgumentException("SQL query cannot be null or empty", nameof(sqlQuery));
                
            // Use Task.Run to run the parse operation on a background thread
            return await Task.Run(() => Parse(sqlQuery), cancellationToken);
        }

        /// <summary>
        /// Tries to parse a SQL query without throwing exceptions
        /// </summary>
        /// <param name="sqlQuery">SQL query to parse</param>
        /// <param name="fragment">Resulting AST fragment</param>
        /// <param name="errors">Parsing errors</param>
        /// <returns>True if parsing is successful; otherwise, false</returns>
        public bool TryParse(string sqlQuery, out TSqlFragment fragment, out IList<ParseError> errors)
        {
            if (string.IsNullOrWhiteSpace(sqlQuery))
            {
                fragment = null;
                errors = new List<ParseError>();
                return false;
            }

            TSqlParser parser = CreateParser();
            errors = new List<ParseError>();
            var startTime = DateTime.UtcNow;
            
            try
            {
                using (TextReader reader = new StringReader(sqlQuery))
                {
                    fragment = parser.Parse(reader, out errors);
                    
                    // Update performance stats
                    Interlocked.Increment(ref _totalParseCount);
                    Interlocked.Add(ref _totalParseTimeMs, (long)(DateTime.UtcNow - startTime).TotalMilliseconds);
                    Interlocked.Add(ref _totalScriptSizeBytes, Encoding.UTF8.GetByteCount(sqlQuery));
                    
                    return errors.Count == 0;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error parsing SQL query");
                fragment = null;
                return false;
            }
        }

        /// <summary>
        /// Creates the appropriate SQL parser based on the specified version
        /// </summary>
        /// <returns>An instance of the SQL parser</returns>
        private TSqlParser CreateParser()
        {
            return _sqlServerVersion switch
            {
                SqlServerVersion.SqlServer2016 => new TSql130Parser(_initialQuotedIdentifiers),
                SqlServerVersion.SqlServer2017 => new TSql140Parser(_initialQuotedIdentifiers),
                SqlServerVersion.SqlServer2019 => new TSql150Parser(_initialQuotedIdentifiers),
                SqlServerVersion.SqlServer2022 => new TSql160Parser(_initialQuotedIdentifiers),
                _ => new TSql160Parser(_initialQuotedIdentifiers)
            };
        }

        /// <summary>
        /// Parses multiple SQL queries from a single string
        /// </summary>
        /// <param name="sqlBatch">SQL batch</param>
        /// <returns>List of AST fragments for each query</returns>
        public IEnumerable<TSqlStatement> ParseBatch(string sqlBatch)
        {
            if (string.IsNullOrWhiteSpace(sqlBatch))
                yield break;

            TSqlParser parser = CreateParser();
            IList<ParseError> errors = new List<ParseError>();
            var startTime = DateTime.UtcNow;

            using (TextReader reader = new StringReader(sqlBatch))
            {
                TSqlFragment fragment = parser.Parse(reader, out errors);
                
                // Update performance stats
                Interlocked.Increment(ref _totalParseCount);
                Interlocked.Add(ref _totalParseTimeMs, (long)(DateTime.UtcNow - startTime).TotalMilliseconds);
                Interlocked.Add(ref _totalScriptSizeBytes, Encoding.UTF8.GetByteCount(sqlBatch));

                if (errors.Count > 0)
                {
                    throw new SqlParsingException("SQL batch parsing failed", errors, sqlBatch);
                }

                if (fragment is TSqlScript script)
                {
                    foreach (TSqlBatch batch in script.Batches)
                    {
                        foreach (TSqlStatement statement in batch.Statements)
                        {
                            yield return statement;
                        }
                    }
                }
                else if (fragment is TSqlStatement statement)
                {
                    yield return statement;
                }
            }
        }
        
        /// <summary>
        /// Parses a large SQL script in parallel batches to reduce memory usage and improve performance
        /// </summary>
        /// <param name="sqlScript">SQL script to parse</param>
        /// <param name="maxDegreeOfParallelism">Maximum number of parallel parsing operations (default: CPU count)</param>
        /// <param name="cancellationToken">Cancellation token for stopping processing</param>
        /// <returns>Collection of statements from the script</returns>
        public async Task<List<TSqlStatement>> ParseLargeScriptParallelAsync(
            string sqlScript, 
            int maxDegreeOfParallelism = 0,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(sqlScript))
                return new List<TSqlStatement>();
            
            // Set default parallelism to processor count
            if (maxDegreeOfParallelism <= 0)
            {
                maxDegreeOfParallelism = Environment.ProcessorCount;
            }
            
            // Split the script into batches (separated by GO statements)
            var batches = SplitSqlIntoBatches(sqlScript).ToList();
            _logger?.LogDebug($"Split script into {batches.Count} batches for parallel processing");
            
            // Create a throttled parallel task scheduler
            var throttler = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new List<Task<IEnumerable<TSqlStatement>>>();
            
            foreach (var batch in batches)
            {
                // Skip empty batches
                if (string.IsNullOrWhiteSpace(batch))
                    continue;
                
                // Wait for throttling semaphore
                await throttler.WaitAsync(cancellationToken);
                
                // Start parallel task
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        // Parse batch on background thread
                        return ParseBatch(batch);
                    }
                    finally
                    {
                        // Release throttling semaphore
                        throttler.Release();
                    }
                }, cancellationToken));
            }
            
            // Wait for all parsing tasks to complete
            var results = await Task.WhenAll(tasks);
            
            // Combine all results
            var allStatements = new List<TSqlStatement>();
            foreach (var batchResults in results)
            {
                allStatements.AddRange(batchResults);
            }
            
            _logger?.LogDebug($"Parallel parsing complete, found {allStatements.Count} statements");
            return allStatements;
        }
        
        /// <summary>
        /// Parses a large SQL script in chunks to reduce memory usage
        /// </summary>
        /// <param name="sqlScript">SQL script to parse</param>
        /// <param name="chunkSize">Size of chunks in characters (default: 10000)</param>
        /// <returns>Collection of statements from the script</returns>
        public IEnumerable<TSqlStatement> ParseLargeScript(string sqlScript, int chunkSize = 10000)
        {
            if (string.IsNullOrWhiteSpace(sqlScript))
                yield break;
                
            var parseResults = new List<TSqlStatement>();
            
            // Split the script at batch separators (GO statements)
            var batches = SplitSqlIntoBatches(sqlScript);
            
            foreach (var batch in batches)
            {
                if (batch.Length <= chunkSize)
                {
                    // Small enough to parse directly
                    foreach (var statement in ParseBatch(batch))
                    {
                        yield return statement;
                    }
                }
                else
                {
                    // Need to further split this large batch
                    var chunks = SplitBatchIntoChunks(batch, chunkSize);
                    _logger?.LogDebug($"Split large batch ({batch.Length} chars) into {chunks.Count()} chunks");
                    
                    foreach (var chunk in chunks)
                    {
                        try
                        {
                            foreach (var statement in ParseBatch(chunk))
                            {
                                yield return statement;
                            }
                        }
                        catch (SqlParsingException ex)
                        {
                            // Log the error but continue with next chunk
                            _logger?.LogWarning($"Error parsing chunk: {ex.Message}");
                        }
                    }
                }
            }
        }
        
        /// <summary>
        /// Splits a SQL script into batches by GO statements using StringPool for memory efficiency
        /// </summary>
        private IEnumerable<string> SplitSqlIntoBatches(string sqlScript)
        {
            var lines = sqlScript.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.None);
            var currentBatch = new StringBuilder();
            
            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();
                
                // Check if line is a GO statement
                if (trimmedLine.Equals("GO", StringComparison.OrdinalIgnoreCase) || 
                    trimmedLine.StartsWith("GO ", StringComparison.OrdinalIgnoreCase))
                {
                    string batch = currentBatch.ToString().Trim();
                    if (!string.IsNullOrWhiteSpace(batch))
                    {
                        yield return _stringPool.Intern(batch);
                    }
                    
                    currentBatch.Clear();
                }
                else
                {
                    currentBatch.AppendLine(line);
                }
            }
            
            // Return any remaining SQL as the final batch
            string finalBatch = currentBatch.ToString().Trim();
            if (!string.IsNullOrWhiteSpace(finalBatch))
            {
                yield return _stringPool.Intern(finalBatch);
            }
        }
        
        /// <summary>
        /// Splits a large batch into smaller chunks at statement boundaries for more efficient parsing
        /// </summary>
        private IEnumerable<string> SplitBatchIntoChunks(string batch, int chunkSize)
        {
            if (batch.Length <= chunkSize)
            {
                yield return batch;
                yield break;
            }
            
            // Common statement terminators to split on
            string[] terminators = { ";", "END", "END;", "GO", "RETURN", "RETURN;" };
            
            int startPos = 0;
            var currentChunk = new StringBuilder();
            
            while (startPos < batch.Length)
            {
                // Find the next terminator after the current position
                int nextTerminatorPos = int.MaxValue;
                string matchedTerminator = null;
                
                foreach (var terminator in terminators)
                {
                    var pos = batch.IndexOf(terminator, startPos, StringComparison.OrdinalIgnoreCase);
                    if (pos >= 0 && pos < nextTerminatorPos)
                    {
                        nextTerminatorPos = pos;
                        matchedTerminator = terminator;
                    }
                }
                
                // If no terminator found, or it would make chunk too large
                if (nextTerminatorPos == int.MaxValue || 
                    nextTerminatorPos - startPos > chunkSize)
                {
                    // Find a good break point based on statement boundaries
                    int endPos = Math.Min(startPos + chunkSize, batch.Length);
                    
                    // Try to break at whitespace
                    int breakPos = batch.LastIndexOfAny(new[] { ' ', '\t', '\r', '\n' }, endPos - 1, Math.Min(50, endPos - startPos));
                    if (breakPos > startPos)
                    {
                        endPos = breakPos + 1;  // Include the whitespace character
                    }
                    
                    // Add the chunk
                    currentChunk.Append(batch.Substring(startPos, endPos - startPos));
                    var chunk = _stringPool.Intern(currentChunk.ToString());
                    currentChunk.Clear();
                    
                    yield return chunk;
                    startPos = endPos;
                }
                else
                {
                    // Add the statement up to and including the terminator
                    int statementLength = (nextTerminatorPos + matchedTerminator.Length) - startPos;
                    
                    currentChunk.Append(batch.Substring(startPos, statementLength));
                    
                    // If adding this statement would exceed the chunk size, yield the current chunk
                    if (currentChunk.Length >= chunkSize)
                    {
                        var chunk = _stringPool.Intern(currentChunk.ToString());
                        currentChunk.Clear();
                        
                        yield return chunk;
                    }
                    
                    startPos += statementLength;
                }
            }
            
            // Return any remaining content
            if (currentChunk.Length > 0)
            {
                yield return _stringPool.Intern(currentChunk.ToString());
            }
        }

        /// <summary>
        /// Gets the original SQL text from an AST fragment using a pooled script generator
        /// </summary>
        /// <param name="fragment">AST fragment</param>
        /// <returns>Original SQL text</returns>
        public string GetSql(TSqlFragment fragment)
        {
            if (fragment == null)
                return string.Empty;

            // Get a script generator from the pool
            var scriptGenerator = _scriptGeneratorPool.Get();
            var sb = new StringBuilder();

            try
            {
                using (StringWriter writer = new StringWriter(sb))
                {
                    scriptGenerator.GenerateScript(fragment, writer);
                }
                
                return _stringPool.Intern(sb.ToString());
            }
            finally
            {
                // Return the script generator to the pool
                _scriptGeneratorPool.Return(scriptGenerator);
            }
        }
    }
}
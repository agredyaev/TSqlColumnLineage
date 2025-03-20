using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Parsing.Exceptions;

namespace TSqlColumnLineage.Core.Parsing
{
    /// <summary>
    /// Class for parsing T-SQL queries using ScriptDom
    /// </summary>
    public class SqlParser
    {
        private readonly SqlServerVersion _sqlServerVersion;
        private readonly bool _initialQuotedIdentifiers;
        private readonly int _batchSizeLimitBytes;
        
        // Performance statistics
        private int _totalParseCount = 0;
        private long _totalParseTimeMs = 0;
        private int _totalScriptSizeBytes = 0;

        /// <summary>
        /// Initializes a new instance of the SqlParser
        /// </summary>
        /// <param name="sqlServerVersion">SQL Server version for parsing</param>
        /// <param name="initialQuotedIdentifiers">Indicates if quoted identifiers are initially enabled</param>
        /// <param name="batchSizeLimitBytes">Maximum size of batch in bytes (0 for no limit)</param>
        public SqlParser(
            SqlServerVersion sqlServerVersion = SqlServerVersion.Latest,
            bool initialQuotedIdentifiers = true,
            int batchSizeLimitBytes = 10 * 1024 * 1024)  // Default 10MB
        {
            _sqlServerVersion = sqlServerVersion;
            _initialQuotedIdentifiers = initialQuotedIdentifiers;
            _batchSizeLimitBytes = batchSizeLimitBytes;
        }
        
        /// <summary>
        /// Gets parser performance statistics
        /// </summary>
        public (int ParseCount, long TotalTimeMs, double AvgTimeMs, int TotalSizeBytes) 
            GetPerformanceStatistics() =>
            (
                _totalParseCount,
                _totalParseTimeMs,
                _totalParseCount > 0 ? (double)_totalParseTimeMs / _totalParseCount : 0,
                _totalScriptSizeBytes
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
                _totalParseCount++;
                _totalParseTimeMs += (long)(DateTime.UtcNow - startTime).TotalMilliseconds;
                _totalScriptSizeBytes += scriptSizeBytes;

                if (errors.Count > 0)
                {
                    throw new SqlParsingException("SQL parsing failed", errors, sqlQuery);
                }

                return result;
            }
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
                    _totalParseCount++;
                    _totalParseTimeMs += (long)(DateTime.UtcNow - startTime).TotalMilliseconds;
                    _totalScriptSizeBytes += Encoding.UTF8.GetByteCount(sqlQuery);
                    
                    return errors.Count == 0;
                }
            }
            catch (Exception)
            {
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
                _totalParseCount++;
                _totalParseTimeMs += (long)(DateTime.UtcNow - startTime).TotalMilliseconds;
                _totalScriptSizeBytes += Encoding.UTF8.GetByteCount(sqlBatch);

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
            
            // Try to split the script at batch separators (GO statements)
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
                    // Note: This is a simplistic approach and might not work for all scripts
                    var chunks = SplitBatchIntoChunks(batch, chunkSize);
                    
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
                            Console.Error.WriteLine($"Error parsing chunk: {ex.Message}");
                        }
                    }
                }
            }
        }
        
        /// <summary>
        /// Splits a SQL script into batches by GO statements
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
                        yield return batch;
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
                yield return finalBatch;
            }
        }
        
        /// <summary>
        /// Splits a large batch into smaller chunks at statement boundaries
        /// </summary>
        private IEnumerable<string> SplitBatchIntoChunks(string batch, int chunkSize)
        {
            if (batch.Length <= chunkSize)
            {
                yield return batch;
                yield break;
            }
            
            // Try to split at statement boundaries (;)
            int startPos = 0;
            int nextSemicolon;
            
            var currentChunk = new StringBuilder();
            
            while (startPos < batch.Length)
            {
                // Find next statement boundary
                nextSemicolon = batch.IndexOf(';', startPos);
                
                if (nextSemicolon == -1 || nextSemicolon - startPos > chunkSize)
                {
                    // No semicolon found or too far away, need to find another boundary
                    int endPos = Math.Min(startPos + chunkSize, batch.Length);
                    
                    // Try to find a good break point (whitespace after certain keywords)
                    string[] breakKeywords = { "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP" };
                    int bestBreakPoint = -1;
                    
                    foreach (var keyword in breakKeywords)
                    {
                        // Look for keyword followed by whitespace
                        string pattern = keyword + " ";
                        int keywordPos = batch.LastIndexOf(pattern, endPos, StringComparison.OrdinalIgnoreCase);
                        
                        if (keywordPos > startPos && keywordPos > bestBreakPoint)
                        {
                            bestBreakPoint = keywordPos;
                        }
                    }
                    
                    if (bestBreakPoint > startPos)
                    {
                        // Found a keyword to break at
                        currentChunk.Append(batch.Substring(startPos, bestBreakPoint - startPos));
                        yield return currentChunk.ToString();
                        currentChunk.Clear();
                        startPos = bestBreakPoint;
                    }
                    else if (endPos < batch.Length)
                    {
                        // Just break at the character limit
                        currentChunk.Append(batch.Substring(startPos, endPos - startPos));
                        yield return currentChunk.ToString();
                        currentChunk.Clear();
                        startPos = endPos;
                    }
                    else
                    {
                        // This is the last piece
                        currentChunk.Append(batch.Substring(startPos));
                        yield return currentChunk.ToString();
                        break;
                    }
                }
                else
                {
                    // Found a semicolon, include it in the chunk
                    int statementLength = (nextSemicolon + 1) - startPos;
                    string statement = batch.Substring(startPos, statementLength);
                    
                    if (currentChunk.Length + statementLength > chunkSize)
                    {
                        // Current statement would make chunk too large, yield current chunk first
                        if (currentChunk.Length > 0)
                        {
                            yield return currentChunk.ToString();
                            currentChunk.Clear();
                        }
                        
                        // Now add this statement to the new chunk
                        currentChunk.Append(statement);
                    }
                    else
                    {
                        // Add to current chunk
                        currentChunk.Append(statement);
                    }
                    
                    startPos = nextSemicolon + 1;
                    
                    // If chunk is getting large, yield it
                    if (currentChunk.Length >= chunkSize)
                    {
                        yield return currentChunk.ToString();
                        currentChunk.Clear();
                    }
                }
            }
            
            // Return any remaining text
            if (currentChunk.Length > 0)
            {
                yield return currentChunk.ToString();
            }
        }

        /// <summary>
        /// Gets the original SQL text from an AST fragment
        /// </summary>
        /// <param name="fragment">AST fragment</param>
        /// <returns>Original SQL text</returns>
        public string GetSql(TSqlFragment fragment)
        {
            if (fragment == null)
                return string.Empty;

            Sql160ScriptGenerator scriptGenerator = new Sql160ScriptGenerator();
            StringBuilder sb = new StringBuilder();

            using (StringWriter writer = new StringWriter(sb))
            {
                scriptGenerator.GenerateScript(fragment, writer);
            }

            return sb.ToString();
        }
    }
}
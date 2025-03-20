using System;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Common.Logging;

namespace TSqlColumnLineage.Core.Parsing
{
    /// <summary>
    /// Factory for creating SQL parser instances
    /// </summary>
    public static class SqlParserFactory
    {
        // Default batch size limit of 100MB (0 for no limit)
        private const int DefaultBatchSizeLimit = 100 * 1024 * 1024;
        
        /// <summary>
        /// Creates a SQL parser for the specified SQL Server version
        /// </summary>
        /// <param name="version">SQL Server version</param>
        /// <param name="stringPool">String pool for memory optimization</param>
        /// <param name="quotedIdentifiers">Indicates whether quoted identifiers are enabled</param>
        /// <param name="batchSizeLimitBytes">Maximum size of batch in bytes (0 for no limit)</param>
        /// <param name="logger">Logger for diagnostic information</param>
        /// <returns>SqlParser instance</returns>
        public static SqlParser CreateParser(
            SqlServerVersion version = SqlServerVersion.Latest,
            StringPool stringPool = null,
            bool quotedIdentifiers = true,
            int batchSizeLimitBytes = DefaultBatchSizeLimit,
            ILogger logger = null)
        {
            // Create a string pool if one wasn't provided
            stringPool = stringPool ?? new StringPool();
            
            return new SqlParser(version, stringPool, quotedIdentifiers, batchSizeLimitBytes, logger);
        }

        /// <summary>
        /// Creates a SQL parser for the latest supported SQL Server version
        /// </summary>
        /// <param name="stringPool">String pool for memory optimization</param>
        /// <param name="quotedIdentifiers">Indicates whether quoted identifiers are enabled</param>
        /// <param name="batchSizeLimitBytes">Maximum size of batch in bytes (0 for no limit)</param>
        /// <param name="logger">Logger for diagnostic information</param>
        /// <returns>SqlParser instance</returns>
        public static SqlParser CreateLatestParser(
            StringPool stringPool = null,
            bool quotedIdentifiers = true,
            int batchSizeLimitBytes = DefaultBatchSizeLimit,
            ILogger logger = null)
        {
            // Create a string pool if one wasn't provided
            stringPool = stringPool ?? new StringPool();
            
            return new SqlParser(SqlServerVersion.Latest, stringPool, quotedIdentifiers, batchSizeLimitBytes, logger);
        }
        
        /// <summary>
        /// Creates a SQL parser optimized for large scripts
        /// </summary>
        /// <param name="version">SQL Server version</param>
        /// <param name="stringPool">String pool for memory optimization</param>
        /// <param name="quotedIdentifiers">Indicates whether quoted identifiers are enabled</param>
        /// <param name="logger">Logger for diagnostic information</param>
        /// <returns>SqlParser instance with no batch size limit</returns>
        public static SqlParser CreateLargeScriptParser(
            SqlServerVersion version = SqlServerVersion.Latest,
            StringPool stringPool = null,
            bool quotedIdentifiers = true,
            ILogger logger = null)
        {
            // Create a string pool if one wasn't provided
            stringPool = stringPool ?? new StringPool();
            
            // No batch size limit (0) for large scripts
            return new SqlParser(version, stringPool, quotedIdentifiers, 0, logger);
        }
        
        /// <summary>
        /// Creates a SQL parser optimized for memory efficiency (smaller batch size limit)
        /// </summary>
        /// <param name="version">SQL Server version</param>
        /// <param name="stringPool">String pool for memory optimization</param>
        /// <param name="quotedIdentifiers">Indicates whether quoted identifiers are enabled</param>
        /// <param name="logger">Logger for diagnostic information</param>
        /// <returns>SqlParser instance with reduced batch size limit</returns>
        public static SqlParser CreateMemoryEfficientParser(
            SqlServerVersion version = SqlServerVersion.Latest,
            StringPool stringPool = null,
            bool quotedIdentifiers = true,
            ILogger logger = null)
        {
            // Create a string pool if one wasn't provided
            stringPool = stringPool ?? new StringPool();
            
            // Use a smaller batch size limit (10MB) for memory efficiency
            const int smallerBatchLimit = 10 * 1024 * 1024;
            return new SqlParser(version, stringPool, quotedIdentifiers, smallerBatchLimit, logger);
        }
    }
}
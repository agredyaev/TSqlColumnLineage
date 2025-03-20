using System;

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
        /// <param name="quotedIdentifiers">Indicates whether quoted identifiers are enabled</param>
        /// <param name="batchSizeLimitBytes">Maximum size of batch in bytes (0 for no limit)</param>
        /// <returns>SqlParser instance</returns>
        public static SqlParser CreateParser(
            SqlServerVersion version = SqlServerVersion.Latest,
            bool quotedIdentifiers = true,
            int batchSizeLimitBytes = DefaultBatchSizeLimit)
        {
            return new SqlParser(version, quotedIdentifiers, batchSizeLimitBytes);
        }

        /// <summary>
        /// Creates a SQL parser for the latest supported SQL Server version
        /// </summary>
        /// <param name="quotedIdentifiers">Indicates whether quoted identifiers are enabled</param>
        /// <param name="batchSizeLimitBytes">Maximum size of batch in bytes (0 for no limit)</param>
        /// <returns>SqlParser instance</returns>
        public static SqlParser CreateLatestParser(
            bool quotedIdentifiers = true,
            int batchSizeLimitBytes = DefaultBatchSizeLimit)
        {
            return new SqlParser(SqlServerVersion.Latest, quotedIdentifiers, batchSizeLimitBytes);
        }
        
        /// <summary>
        /// Creates a SQL parser optimized for large scripts
        /// </summary>
        /// <param name="version">SQL Server version</param>
        /// <param name="quotedIdentifiers">Indicates whether quoted identifiers are enabled</param>
        /// <returns>SqlParser instance with no batch size limit</returns>
        public static SqlParser CreateLargeScriptParser(
            SqlServerVersion version = SqlServerVersion.Latest,
            bool quotedIdentifiers = true)
        {
            // No batch size limit (0) for large scripts
            return new SqlParser(version, quotedIdentifiers, 0);
        }
        
        /// <summary>
        /// Creates a SQL parser optimized for memory efficiency (smaller batch size limit)
        /// </summary>
        /// <param name="version">SQL Server version</param>
        /// <param name="quotedIdentifiers">Indicates whether quoted identifiers are enabled</param>
        /// <returns>SqlParser instance with reduced batch size limit</returns>
        public static SqlParser CreateMemoryEfficientParser(
            SqlServerVersion version = SqlServerVersion.Latest,
            bool quotedIdentifiers = true)
        {
            // Use a smaller batch size limit (10MB) for memory efficiency
            const int smallerBatchLimit = 10 * 1024 * 1024;
            return new SqlParser(version, quotedIdentifiers, smallerBatchLimit);
        }
    }
}
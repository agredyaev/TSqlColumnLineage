namespace TSqlColumnLineage.Core.Parsing
{
    /// <summary>
    /// Factory for creating SqlParser instances
    /// </summary>
    public class SqlParserFactory
    {
        /// <summary>
        /// Creates a SQL parser for the specified SQL Server version
        /// </summary>
        /// <param name="version">SQL Server version</param>
        /// <param name="quotedIdentifiers">Indicates whether quoted identifiers are enabled</param>
        /// <returns>SqlParser instance</returns>
        public static SqlParser CreateParser(SqlServerVersion version = SqlServerVersion.Latest,
                                            bool quotedIdentifiers = true)
        {
            return new SqlParser(version, quotedIdentifiers);
        }

        /// <summary>
        /// Creates a SQL parser for the latest supported SQL Server version
        /// </summary>
        /// <param name="quotedIdentifiers">Indicates whether quoted identifiers are enabled</param>
        /// <returns>SqlParser instance</returns>
        public static SqlParser CreateLatestParser(bool quotedIdentifiers = true)
        {
            return new SqlParser(SqlServerVersion.Latest, quotedIdentifiers);
        }
    }
}

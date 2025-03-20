using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Engine.Parsing.Models;
using TSqlColumnLineage.Core.Infrastructure.Memory;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// Factory for creating SQL parsers based on configuration.
    /// Implements memory optimization using object pooling.
    /// </summary>
    public static class ParserFactory
    {
        // Memory-optimized parser pool
        private static readonly ObjectPool<TSql160Parser> _parserPool;

        /// <summary>
        /// Static constructor to initialize the parser pool
        /// </summary>
        static ParserFactory()
        {
            // Initialize parser pool using memory manager
            _parserPool = MemoryManager.Instance.GetOrCreateObjectPool<TSql160Parser>(
                () => new TSql160Parser(true), // Default to quoted identifiers
                parser => { /* No reset needed */ },
                Environment.ProcessorCount * 2); // Initial capacity
        }

        /// <summary>
        /// Creates a TSqlParser instance based on the specified options
        /// </summary>
        public static TSqlParser CreateParser(ParsingOptions options)
        {
            // Currently only supporting SQL Server 2022 (160)
            if (options.CompatibilityLevel != 160)
            {
                throw new NotSupportedException($"SQL Server compatibility level {options.CompatibilityLevel} is not supported. Only 160 (SQL Server 2022) is currently supported.");
            }

            // Get parser from pool
            var parser = _parserPool.Get();
            
            // Configure the parser
            parser.QuotedIdentifier = options.UseQuotedIdentifiers;
            parser.CollationOptions = options.CollationOptions;
            parser.IdentityInserts = options.IdentityInserts;
            parser.SetPartialParser(false); // Always use full parser for accuracy

            return parser;
        }

        /// <summary>
        /// Returns a parser to the pool
        /// </summary>
        public static void ReturnParser(TSqlParser parser)
        {
            if (parser is TSql160Parser sql160Parser)
            {
                _parserPool.Return(sql160Parser);
            }
        }

        /// <summary>
        /// Creates a ScriptTokenStream with optimized memory usage
        /// </summary>
        public static IList<TSqlParserToken> CreateTokenStream()
        {
            // Create a pre-sized token list to reduce reallocations
            return new List<TSqlParserToken>(1024); // Reasonable initial capacity
        }
    }
}
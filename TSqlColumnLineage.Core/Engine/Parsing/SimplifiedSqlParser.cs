using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Domain.Graph;
using TSqlColumnLineage.Core.Infrastructure;
using TSqlColumnLineage.Core.Infrastructure.Memory;
using TSqlColumnLineage.Core.Infrastructure.Monitoring;

namespace TSqlColumnLineage.Core.Engine.Parsing
{
    /// <summary>
    /// Simplified SQL parser that directly leverages ScriptDom functionality
    /// with infrastructure integration
    /// </summary>
    public class SimplifiedSqlParser
    {
        private readonly bool _quotedIdentifier;
        private readonly MemoryManager _memoryManager;
        private readonly PerformanceTracker _performanceTracker;
        private readonly InfrastructureService _infrastructureService;
        
        public SimplifiedSqlParser(bool quotedIdentifier = true)
        {
            _quotedIdentifier = quotedIdentifier;
            _memoryManager = MemoryManager.Instance;
            _performanceTracker = PerformanceTracker.Instance;
            _infrastructureService = InfrastructureService.Instance;
            
            // Initialize infrastructure if needed
            if (!_infrastructureService.IsInitialized)
            {
                _infrastructureService.Initialize();
            }
        }
        
        /// <summary>
        /// Parses SQL text synchronously
        /// </summary>
        public TSqlFragment Parse(string sqlText, out IList<ParseError> errors)
        {
            using var tracker = _performanceTracker.TrackOperation("Parsing", "ParseSql");
            
            // Optimize memory by interning common strings
            sqlText = _memoryManager.InternString(sqlText);
            
            using var reader = new StringReader(sqlText);
            
            // Get parser from pool via infrastructure
            var parser = CreateParser();
            try
            {
                var fragment = parser.Parse(reader, out errors);
                
                // Track errors
                if (errors.Count > 0)
                {
                    _performanceTracker.IncrementCounter("Parsing", "ErrorCount", errors.Count);
                }
                
                return fragment;
            }
            catch (Exception ex)
            {
                _performanceTracker.IncrementCounter("Parsing", "ExceptionErrors");
                errors = new List<ParseError>
                {
                    new ParseError { 
                        Message = ex.Message,
                        Number = -1,
                        Line = 0,
                        Column = 0
                    }
                };
                return null;
            }
        }
        
        /// <summary>
        /// Parses SQL text asynchronously
        /// </summary>
        public Task<(TSqlFragment Fragment, IList<ParseError> Errors)> ParseAsync(
            string sqlText, CancellationToken cancellationToken = default)
        {
            // Use infrastructure for threaded operations
            return _infrastructureService.ProcessBatch(
                new List<string> { sqlText },
                (text) => {
                    var fragment = Parse(text, out var errors);
                    return (fragment, errors);
                },
                "ParseSql",
                1,
                cancellationToken
            ).ContinueWith(t => {
                if (t.IsCompletedSuccessfully && t.Result.Results.Count > 0)
                {
                    return t.Result.Results[0];
                }
                return (null, new List<ParseError>() as IList<ParseError>);
            }, cancellationToken);
        }
        
        /// <summary>
        /// Parses SQL and extracts lineage in one operation
        /// </summary>
        public LineageGraph ExtractLineage(string sqlText)
        {
            using var outerTracker = _performanceTracker.TrackOperation("Lineage", "ExtractLineage");
            
            // Parse the SQL
            var fragment = Parse(sqlText, out var errors);
            if (fragment == null || errors.Count > 0)
            {
                throw new ArgumentException($"SQL parsing failed with {errors.Count} errors");
            }
            
            // Extract lineage
            var graph = new LineageGraph();
            var visitor = new LineageVisitor(graph);
            
            // Track visitor progress
            using (_performanceTracker.TrackOperation("Lineage", "VisitAst"))
            {
                fragment.Accept(visitor);
            }
            
            // Optimize graph storage
            using (_performanceTracker.TrackOperation("Lineage", "CompactGraph"))
            {
                graph.Compact();
            }
            
            // Store source SQL
            graph.SourceSql = sqlText;
            
            return graph;
        }
        
        /// <summary>
        /// Creates a ScriptDom parser
        /// </summary>
        private TSqlParser CreateParser()
        {
            return new TSql150Parser(_quotedIdentifier);
        }
    }
}
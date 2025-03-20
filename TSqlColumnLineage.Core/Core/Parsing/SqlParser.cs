using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using TSqlColumnLineage.Core.Visitors;

namespace TSqlColumnLineage.Core.Parsing
{
    /// <summary>
    /// Class for parsing T-SQL queries using ScriptDom
    /// </summary>
    public class SqlParser
    {
        private readonly SqlServerVersion _sqlServerVersion;
        private readonly bool _initialQuotedIdentifiers;

        /// <summary>
        /// Initializes a new instance of the SqlParser
        /// </summary>
        /// <param name="sqlServerVersion">SQL Server version for parsing</param>
        /// <param name="initialQuotedIdentifiers">Indicates if quoted identifiers are initially enabled</param>
        public SqlParser(SqlServerVersion sqlServerVersion = SqlServerVersion.Latest,
                         bool initialQuotedIdentifiers = true)
        {
            _sqlServerVersion = sqlServerVersion;
            _initialQuotedIdentifiers = initialQuotedIdentifiers;
        }

        /// <summary>
        /// Parses a SQL query and returns the root AST fragment
        /// </summary>
        /// <param name="sqlQuery">SQL query to parse</param>
        /// <returns>Root fragment of the abstract syntax tree</returns>
        /// <exception cref="SqlParsingException">Thrown when parsing errors occur</exception>
        private LineageContext _lineageContext;

        public LineageContext LineageContext => _lineageContext;

        public TSqlFragment Parse(string sqlQuery)
        {
            if (string.IsNullOrWhiteSpace(sqlQuery))
                throw new ArgumentException("SQL query cannot be null or empty", nameof(sqlQuery));

            TSqlParser parser = CreateParser();
            IList<ParseError> errors = new List<ParseError>();
            var nodeFactory = new LineageNodeFactory();
            var edgeFactory = new LineageEdgeFactory();
            var graph = new LineageGraph(nodeFactory, edgeFactory);
            _lineageContext = new LineageContext(graph);

            using (TextReader reader = new StringReader(sqlQuery))
            {
                TSqlFragment result = parser.Parse(reader, out errors);

                if (errors.Count > 0)
                {
                    throw new SqlParsingException("SQL parsing failed", errors, sqlQuery);
                }

                // Analyze lineage after successful parse
                var visitor = new ColumnLineageVisitor(
                    graph, // Pass graph instance
                    _lineageContext,
                    new ConsoleLogger());
                
                result.Accept(visitor);

                return result;
            }
        }

        /// <summary>
        /// Tries to parse a SQL query
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

            using (TextReader reader = new StringReader(sqlQuery))
            {
                fragment = parser.Parse(reader, out errors);
                return errors.Count == 0;
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

            using (TextReader reader = new StringReader(sqlBatch))
            {
                TSqlFragment fragment = parser.Parse(reader, out errors);

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

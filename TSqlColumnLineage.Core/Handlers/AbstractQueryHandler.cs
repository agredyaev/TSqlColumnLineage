using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Visitors;
using System;

namespace TSqlColumnLineage.Core.Handlers
{
    /// <summary>
    /// Abstract base class for SQL query handlers that implements common functionality
    /// </summary>
    public abstract class AbstractQueryHandler : IQueryHandler
    {
        protected readonly ColumnLineageVisitor Visitor;
        protected readonly LineageGraph Graph;
        protected readonly LineageContext Context;
        protected readonly ILogger Logger;

        protected AbstractQueryHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger logger)
        {
            Visitor = visitor ?? throw new ArgumentNullException(nameof(visitor));
            Graph = graph ?? throw new ArgumentNullException(nameof(graph));
            Context = context ?? throw new ArgumentNullException(nameof(context));
            Logger = logger;
        }

        /// <summary>
        /// Process a SQL fragment
        /// </summary>
        /// <param name="fragment">The SQL fragment to process</param>
        /// <returns>True if the handler processed the fragment; otherwise, false</returns>
        public abstract bool Process(TSqlFragment fragment);

        /// <summary>
        /// Log debug information if logger is available
        /// </summary>
        /// <param name="message">Message to log</param>
        protected void LogDebug(string message)
        {
            Logger?.LogDebug(message);
        }

        /// <summary>
        /// Gets SQL text representation of a fragment
        /// </summary>
        /// <param name="fragment">Fragment to get SQL text for</param>
        /// <returns>SQL text</returns>
        protected string GetSqlText(TSqlFragment fragment)
        {
            return Visitor.GetSqlText(fragment);
        }

        /// <summary>
        /// Creates a node ID with the specified type and name
        /// </summary>
        /// <param name="type">Node type</param>
        /// <param name="name">Node name</param>
        /// <returns>Node ID</returns>
        protected string CreateNodeId(string type, string name)
        {
            return Visitor.CreateNodeId(type, name);
        }
    }
}

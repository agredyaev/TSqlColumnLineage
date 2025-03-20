using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Visitors;

namespace TSqlColumnLineage.Core.Handlers
{
    /// <summary>
    /// Interface for SQL query fragment handlers
    /// </summary>
    public interface IQueryHandler
    {
        /// <summary>
        /// Process a SQL fragment
        /// </summary>
        /// <param name="fragment">The SQL fragment to process</param>
        /// <returns>True if the handler processed the fragment; otherwise, false</returns>
        bool Process(TSqlFragment fragment);
    }
}

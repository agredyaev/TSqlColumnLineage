using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Base
{
    /// <summary>
    /// Interface for SQL fragment handlers
    /// </summary>
    public interface IQueryHandler
    {
        /// <summary>
        /// Checks if this handler can process the specified fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <returns>True if the handler can process the fragment; otherwise, false</returns>
        bool CanHandle(TSqlFragment fragment);
        
        /// <summary>
        /// Processes the SQL fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <param name="context">Visitor context</param>
        /// <returns>True if the fragment was fully processed; otherwise, false</returns>
        bool Handle(TSqlFragment fragment, VisitorContext context);
    }
}
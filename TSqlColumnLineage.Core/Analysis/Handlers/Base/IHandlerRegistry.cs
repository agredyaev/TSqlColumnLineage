using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Base
{
    /// <summary>
    /// Registry for SQL fragment handlers
    /// </summary>
    public interface IHandlerRegistry
    {
        /// <summary>
        /// Registers a handler
        /// </summary>
        /// <param name="handler">Handler to register</param>
        void RegisterHandler(IQueryHandler handler);
        
        /// <summary>
        /// Gets a handler for the specified fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <returns>Handler for the fragment or null if no handler is found</returns>
        IQueryHandler GetHandler(TSqlFragment fragment);
    }
}
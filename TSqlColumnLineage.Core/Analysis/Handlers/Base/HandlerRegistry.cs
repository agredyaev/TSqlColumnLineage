using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Base
{
    /// <summary>
    /// Registry for SQL fragment handlers
    /// </summary>
    public class HandlerRegistry : IHandlerRegistry
    {
        private readonly List<IQueryHandler> _handlers = new();
        
        // Cache of fragment type to handler for faster lookups
        private readonly Dictionary<Type, List<IQueryHandler>> _handlerCache = new();
        
        /// <summary>
        /// Registers a handler
        /// </summary>
        /// <param name="handler">Handler to register</param>
        public void RegisterHandler(IQueryHandler handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            
            _handlers.Add(handler);
            
            // Clear the cache when a handler is added
            _handlerCache.Clear();
        }
        
        /// <summary>
        /// Gets a handler for the specified fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <returns>Handler for the fragment or null if no handler is found</returns>
        public IQueryHandler GetHandler(TSqlFragment fragment)
        {
            if (fragment == null) return null;
            
            var fragmentType = fragment.GetType();
            
            // Check cache first
            if (_handlerCache.TryGetValue(fragmentType, out var cachedHandlers))
            {
                return cachedHandlers.FirstOrDefault(h => h.CanHandle(fragment));
            }
            
            // Find handlers for this type
            var handlers = _handlers.Where(h => h.CanHandle(fragment)).ToList();
            
            // Cache the result
            _handlerCache[fragmentType] = handlers;
            
            return handlers.FirstOrDefault();
        }
        
        /// <summary>
        /// Gets all handlers for the specified fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <returns>Handlers for the fragment</returns>
        public IEnumerable<IQueryHandler> GetHandlers(TSqlFragment fragment)
        {
            if (fragment == null) return Enumerable.Empty<IQueryHandler>();
            
            var fragmentType = fragment.GetType();
            
            // Check cache first
            if (_handlerCache.TryGetValue(fragmentType, out var cachedHandlers))
            {
                return cachedHandlers.Where(h => h.CanHandle(fragment));
            }
            
            // Find handlers for this type
            var handlers = _handlers.Where(h => h.CanHandle(fragment)).ToList();
            
            // Cache the result
            _handlerCache[fragmentType] = handlers;
            
            return handlers;
        }
        
        /// <summary>
        /// Creates a default handler registry with common handlers
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="logger">Logger (optional)</param>
        /// <returns>Handler registry initialized with default handlers</returns>
        public static HandlerRegistry CreateDefault(VisitorContext context, ILogger logger = null)
        {
            var registry = new HandlerRegistry();
            
            // Register handlers
            registry.RegisterHandler(new Tables.CommonTableExpressionHandler(context, logger));
            registry.RegisterHandler(new Tables.TempTableHandler(context, logger));
            registry.RegisterHandler(new Statements.InsertStatementHandler(context, logger));
            registry.RegisterHandler(new Statements.StoredProcedureHandler(context, logger));
            registry.RegisterHandler(new Expressions.CaseExpressionHandler(context, logger));
            registry.RegisterHandler(new Expressions.WindowFunctionHandler(context, logger));
            
            return registry;
        }
    }
}
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TSqlColumnLineage.Core.Analysis.Visitors.Base;
using TSqlColumnLineage.Core.Common.Utils;

namespace TSqlColumnLineage.Core.Analysis.Handlers.Base
{
    /// <summary>
    /// Registry for SQL fragment handlers with improved lookup performance and thread safety
    /// </summary>
    public sealed class HandlerRegistry : IHandlerRegistry
    {
        private readonly List<IQueryHandler> _handlers = new List<IQueryHandler>();
        
        // Cache of fragment type to handler for faster lookups
        private readonly ConcurrentDictionary<Type, List<IQueryHandler>> _handlerCache = new ConcurrentDictionary<Type, List<IQueryHandler>>();
        
        // Cache of handler by fragment instance for faster repeated lookups
        private readonly ConcurrentDictionary<int, IQueryHandler> _instanceCache = new ConcurrentDictionary<int, IQueryHandler>();
        
        // Pooled lists for reduced allocation
        private readonly ObjectPool<List<IQueryHandler>> _handlerListPool;
        
        // Cache hit statistics
        private int _typeCacheHits;
        private int _instanceCacheHits;
        private int _cacheMisses;
        private readonly object _statsLock = new object();
        
        /// <summary>
        /// Creates a new handler registry with optimized caching
        /// </summary>
        /// <param name="stringPool">String pool for memory optimization</param>
        public HandlerRegistry(StringPool stringPool)
        {
            if (stringPool == null) throw new ArgumentNullException(nameof(stringPool));
            
            // Initialize object pool for handler lists
            _handlerListPool = new ObjectPool<List<IQueryHandler>>(
                () => new List<IQueryHandler>(),
                list => list.Clear(),
                initialCount: 10,
                maxObjects: 100);
        }
        
        /// <summary>
        /// Registers a handler
        /// </summary>
        /// <param name="handler">Handler to register</param>
        public void RegisterHandler(IQueryHandler handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            
            lock (_handlers)
            {
                _handlers.Add(handler);
                
                // Clear the caches when a handler is added
                _handlerCache.Clear();
                _instanceCache.Clear();
            }
        }
        
        /// <summary>
        /// Registers multiple handlers at once
        /// </summary>
        /// <param name="handlers">Handlers to register</param>
        public void RegisterHandlers(IEnumerable<IQueryHandler> handlers)
        {
            if (handlers == null) throw new ArgumentNullException(nameof(handlers));
            
            lock (_handlers)
            {
                _handlers.AddRange(handlers);
                
                // Clear the caches when handlers are added
                _handlerCache.Clear();
                _instanceCache.Clear();
            }
        }
        
        /// <summary>
        /// Gets a handler for the specified fragment
        /// </summary>
        /// <param name="fragment">SQL fragment</param>
        /// <returns>Handler for the fragment or null if no handler is found</returns>
        public IQueryHandler GetHandler(TSqlFragment fragment)
        {
            if (fragment == null) return null;
            
            // Try instance cache first (fastest)
            int fragmentId = System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(fragment);
            if (_instanceCache.TryGetValue(fragmentId, out var cachedHandler))
            {
                Interlocked.Increment(ref _instanceCacheHits);
                return cachedHandler;
            }
            
            // Try type cache next
            var fragmentType = fragment.GetType();
            if (_handlerCache.TryGetValue(fragmentType, out var cachedHandlers))
            {
                Interlocked.Increment(ref _typeCacheHits);
                
                // Find a handler that can handle this specific fragment
                var handler = cachedHandlers.FirstOrDefault(h => h.CanHandle(fragment));
                
                // Cache the result for this specific instance
                if (handler != null)
                {
                    _instanceCache.TryAdd(fragmentId, handler);
                }
                
                return handler;
            }
            
            // Cache miss - find handlers for this type
            Interlocked.Increment(ref _cacheMisses);
            
            List<IQueryHandler> handlers;
            lock (_handlers)
            {
                handlers = _handlers.Where(h => h.CanHandle(fragment)).ToList();
            }
            
            // Cache the result
            _handlerCache[fragmentType] = handlers;
            
            // Get the first matching handler
            var firstHandler = handlers.FirstOrDefault();
            
            // Cache for this specific instance
            if (firstHandler != null)
            {
                _instanceCache.TryAdd(fragmentId, firstHandler);
            }
            
            return firstHandler;
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
                Interlocked.Increment(ref _typeCacheHits);
                return cachedHandlers.Where(h => h.CanHandle(fragment));
            }
            
            // Cache miss
            Interlocked.Increment(ref _cacheMisses);
            
            // Get a pooled list
            var handlerList = _handlerListPool.Get();
            
            try
            {
                // Find handlers for this type
                lock (_handlers)
                {
                    foreach (var handler in _handlers)
                    {
                        if (handler.CanHandle(fragment))
                        {
                            handlerList.Add(handler);
                        }
                    }
                }
                
                // Cache the result
                _handlerCache[fragmentType] = new List<IQueryHandler>(handlerList);
                
                // Convert to array for thread safety (prevent modification)
                return handlerList.ToArray();
            }
            finally
            {
                // Return the list to the pool
                _handlerListPool.Return(handlerList);
            }
        }
        
        /// <summary>
        /// Gets cache statistics for debugging and performance tuning
        /// </summary>
        public (int TypeCacheHits, int InstanceCacheHits, int CacheMisses) GetCacheStatistics()
        {
            lock (_statsLock)
            {
                return (_typeCacheHits, _instanceCacheHits, _cacheMisses);
            }
        }
        
        /// <summary>
        /// Clears all caches to release memory
        /// </summary>
        public void ClearCaches()
        {
            _handlerCache.Clear();
            _instanceCache.Clear();
        }
        
        /// <summary>
        /// Creates a default handler registry with common handlers
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="stringPool">String pool</param>
        /// <param name="idGenerator">ID generator</param>
        /// <param name="logger">Logger (optional)</param>
        /// <returns>Handler registry initialized with default handlers</returns>
        public static HandlerRegistry CreateDefault(
            VisitorContext context, 
            StringPool stringPool,
            IdGenerator idGenerator,
            ILogger logger = null)
        {
            var registry = new HandlerRegistry(stringPool);
            
            // Register handlers
            registry.RegisterHandlers(new IQueryHandler[] 
            {
                new Tables.CommonTableExpressionHandler(context, stringPool, idGenerator, logger),
                new Tables.TempTableHandler(context, stringPool, idGenerator, logger),
                new Statements.InsertStatementHandler(context, stringPool, idGenerator, logger),
                new Statements.StoredProcedureHandler(context, stringPool, idGenerator, logger),
                new Expressions.CaseExpressionHandler(context, stringPool, idGenerator, logger),
                new Expressions.WindowFunctionHandler(context, stringPool, idGenerator, logger)
            });
            
            return registry;
        }
    }
}
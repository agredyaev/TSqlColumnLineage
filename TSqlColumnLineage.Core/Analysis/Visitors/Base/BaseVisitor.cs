using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Common.Utils;
using TSqlColumnLineage.Core.Models.Edges;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Visitors.Base
{
    /// <summary>
    /// Base visitor for traversing the T-SQL AST with improved performance and memory efficiency.
    /// Uses a non-recursive traversal algorithm to prevent stack overflows with complex queries.
    /// </summary>
    public abstract class BaseVisitor : TSqlFragmentVisitor
    {
        // Visited fragments set using object IDs for better performance
        private readonly HashSet<int> _visitedFragments = new HashSet<int>();
        
        // Fragment processing queue for non-recursive traversal
        private readonly Queue<TSqlFragment> _processingQueue = new Queue<TSqlFragment>();
        
        /// <summary>
        /// Visitor context for tracking state
        /// </summary>
        protected readonly VisitorContext Context;
        
        /// <summary>
        /// Lineage graph being constructed
        /// </summary>
        protected LineageGraph Graph => Context.LineageContext.Graph;
        
        /// <summary>
        /// Lineage context
        /// </summary>
        protected LineageContext LineageContext => Context.LineageContext;
        
        /// <summary>
        /// Logger
        /// </summary>
        protected readonly ILogger Logger;
        
        /// <summary>
        /// String pool for memory optimization
        /// </summary>
        private readonly StringPool _stringPool;
        
        /// <summary>
        /// ID generator for creating consistent IDs
        /// </summary>
        private readonly IdGenerator _idGenerator;
        
        /// <summary>
        /// Counter for fragment processing to avoid excessive logging
        /// </summary>
        private int _fragmentCounter;
        
        /// <summary>
        /// Cancellation token to support stopping long-running operations
        /// </summary>
        private readonly CancellationToken _cancellationToken;
        
        /// <summary>
        /// Creates a new base visitor
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="stringPool">String pool for memory optimization</param>
        /// <param name="idGenerator">ID generator</param>
        /// <param name="logger">Logger (optional)</param>
        /// <param name="cancellationToken">Cancellation token for stopping processing</param>
        protected BaseVisitor(
            VisitorContext context, 
            StringPool stringPool,
            IdGenerator idGenerator,
            ILogger logger = null,
            CancellationToken cancellationToken = default)
        {
            Context = context ?? throw new ArgumentNullException(nameof(context));
            _stringPool = stringPool ?? throw new ArgumentNullException(nameof(stringPool));
            _idGenerator = idGenerator ?? throw new ArgumentNullException(nameof(idGenerator));
            Logger = logger;
            _cancellationToken = cancellationToken;
        }
        
        /// <summary>
        /// Starts visiting the AST fragment in a non-recursive manner to prevent stack overflow
        /// </summary>
        public void VisitNonRecursive(TSqlFragment root)
        {
            if (root == null) return;
            
            // Clear state
            _visitedFragments.Clear();
            _processingQueue.Clear();
            _fragmentCounter = 0;
            
            // Start with the root fragment
            _processingQueue.Enqueue(root);
            
            // Process all fragments in the queue
            while (_processingQueue.Count > 0 && !Context.ShouldStop && !_cancellationToken.IsCancellationRequested)
            {
                var fragment = _processingQueue.Dequeue();
                
                // Skip if already visited
                int fragmentId = RuntimeHelpers.GetHashCode(fragment);
                if (_visitedFragments.Contains(fragmentId))
                    continue;
                
                // Mark as visited
                _visitedFragments.Add(fragmentId);
                
                // Push the current fragment to context
                using (Context.CreateFragmentScope(fragment))
                {
                    try
                    {
                        // Process this fragment
                        _fragmentCounter++;
                        fragment.Accept(this);
                        
                        // Periodic logging
                        if (_fragmentCounter % 1000 == 0)
                        {
                            LogDebug($"Processed {_fragmentCounter} fragments, queue size: {_processingQueue.Count}");
                        }
                        
                        // Add children to the queue
                        foreach (var child in GetChildren(fragment))
                        {
                            if (child != null)
                            {
                                _processingQueue.Enqueue(child);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Log the error but continue processing
                        LogError($"Error processing {fragment.GetType().Name}", ex);
                    }
                }
            }
            
            // Log if processing was stopped
            if (Context.ShouldStop || _cancellationToken.IsCancellationRequested)
            {
                LogWarning($"Processing stopped after {_fragmentCounter} fragments " +
                           $"({(DateTime.UtcNow - Context.StartTime).TotalSeconds:F1} seconds)");
            }
            else
            {
                LogInfo($"Completed processing {_fragmentCounter} fragments in " +
                        $"{(DateTime.UtcNow - Context.StartTime).TotalSeconds:F1} seconds");
            }
        }
        
        /// <summary>
        /// Default implementation of Visit for recursive traversal
        /// This should be used with caution for large queries due to potential stack overflow
        /// </summary>
        public override void Visit(TSqlFragment fragment)
        {
            if (fragment == null) return;
            
            // Check if we should stop processing
            Context.CheckProcessingLimits();
            if (Context.ShouldStop || _cancellationToken.IsCancellationRequested) return;
            
            // Check if already visited
            int fragmentId = RuntimeHelpers.GetHashCode(fragment);
            if (_visitedFragments.Contains(fragmentId)) return;
            
            // Mark as visited
            _visitedFragments.Add(fragmentId);
            
            // Use the context scope to manage the fragment stack
            using (Context.CreateFragmentScope(fragment))
            {
                try
                {
                    // Process this fragment normally
                    _fragmentCounter++;
                    fragment.Accept(this);
                    
                    // Periodic logging
                    if (_fragmentCounter % 1000 == 0)
                    {
                        LogDebug($"Processed {_fragmentCounter} fragments");
                    }
                }
                catch (Exception ex)
                {
                    // Log the error but continue processing
                    LogError($"Error processing {fragment.GetType().Name}", ex);
                }
            }
        }
        
        /// <summary>
        /// Gets child elements of an AST fragment using reflection for flexibility
        /// </summary>
        protected IEnumerable<TSqlFragment> GetChildren(TSqlFragment fragment)
        {
            if (fragment == null)
                yield break;

            var type = fragment.GetType();
            var properties = type.GetProperties()
                .Where(p => typeof(TSqlFragment).IsAssignableFrom(p.PropertyType) ||
                            typeof(IList<TSqlFragment>).IsAssignableFrom(p.PropertyType) ||
                            (p.PropertyType.IsGenericType &&
                             p.PropertyType.GetGenericTypeDefinition() == typeof(IList<>) &&
                             typeof(TSqlFragment).IsAssignableFrom(p.PropertyType.GetGenericArguments()[0])));

            foreach (var property in properties)
            {
                var value = property.GetValue(fragment);

                if (value == null)
                    continue;

                if (value is TSqlFragment childFragment)
                {
                    yield return childFragment;
                }
                else if (value is IEnumerable<TSqlFragment> childFragments)
                {
                    foreach (var child in childFragments.Where(c => c != null))
                    {
                        yield return child;
                    }
                }
                else if (value is System.Collections.IEnumerable enumerable)
                {
                    foreach (var item in enumerable)
                    {
                        if (item is TSqlFragment child && child != null)
                            yield return child;
                    }
                }
            }
        }
        
        /// <summary>
        /// Gets the SQL text of a fragment
        /// </summary>
        protected string GetSqlText(TSqlFragment fragment)
        {
            return Context.GetSqlText(fragment);
        }
        
        /// <summary>
        /// Creates a unique ID for a node
        /// </summary>
        protected string CreateNodeId(string prefix, string name)
        {
            return _idGenerator.CreateNodeId(prefix, name);
        }
        
        /// <summary>
        /// Creates a random ID
        /// </summary>
        protected string CreateRandomId()
        {
            return _idGenerator.CreateGuidId("ID");
        }
        
        /// <summary>
        /// Creates a direct edge between two nodes
        /// </summary>
        protected LineageEdge CreateDirectEdge(string sourceId, string targetId, string operation, string sqlExpression = "")
        {
            var id = _idGenerator.CreateGuidId("EDGE");
            operation = _stringPool.Intern(operation);
            
            return new LineageEdge(
                id,
                sourceId,
                targetId,
                EdgeType.Direct.ToString(),
                operation,
                sqlExpression);
        }
        
        /// <summary>
        /// Creates an indirect edge between two nodes
        /// </summary>
        protected LineageEdge CreateIndirectEdge(string sourceId, string targetId, string operation, string sqlExpression = "")
        {
            var id = _idGenerator.CreateGuidId("EDGE");
            operation = _stringPool.Intern(operation);
            
            return new LineageEdge(
                id,
                sourceId,
                targetId,
                EdgeType.Indirect.ToString(),
                operation,
                sqlExpression);
        }
        
        /// <summary>
        /// Interns a string to reduce memory usage
        /// </summary>
        protected string InternString(string str)
        {
            return _stringPool.Intern(str);
        }
        
        /// <summary>
        /// Logs an error message
        /// </summary>
        protected void LogError(string message, Exception ex = null)
        {
            Logger?.LogError(ex, message);
        }
        
        /// <summary>
        /// Logs an information message
        /// </summary>
        protected void LogInfo(string message)
        {
            Logger?.LogInformation(message);
        }
        
        /// <summary>
        /// Logs a debug message
        /// </summary>
        protected void LogDebug(string message)
        {
            Logger?.LogDebug(message);
        }
        
        /// <summary>
        /// Logs a warning message
        /// </summary>
        protected void LogWarning(string message)
        {
            Logger?.LogWarning(message);
        }
    }
}
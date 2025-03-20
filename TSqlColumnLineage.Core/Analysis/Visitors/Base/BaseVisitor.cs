using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using TSqlColumnLineage.Core.Common.Logging;
using TSqlColumnLineage.Core.Models.Edges;
using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Models.Nodes;

namespace TSqlColumnLineage.Core.Analysis.Visitors.Base
{
    /// <summary>
    /// Base visitor for traversing the T-SQL AST with improved performance and memory efficiency
    /// </summary>
    public abstract class BaseVisitor : TSqlFragmentVisitor
    {
        // Visited fragments set using object IDs for better performance
        private readonly HashSet<int> _visitedFragments = new();
        
        // Fragment processing stack for non-recursive traversal
        private readonly Stack<TSqlFragment> _processingStack = new();
        
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
        private readonly Dictionary<string, string> _stringPool = new();
        
        /// <summary>
        /// Creates a new base visitor
        /// </summary>
        /// <param name="context">Visitor context</param>
        /// <param name="logger">Logger (optional)</param>
        protected BaseVisitor(VisitorContext context, ILogger logger = null)
        {
            Context = context ?? throw new ArgumentNullException(nameof(context));
            Logger = logger;
        }
        
        /// <summary>
        /// Starts visiting the AST fragment in a non-recursive manner
        /// </summary>
        public void VisitNonRecursive(TSqlFragment root)
        {
            if (root == null) return;
            
            // Clear state
            _visitedFragments.Clear();
            _processingStack.Clear();
            
            // Start with the root fragment
            _processingStack.Push(root);
            
            // Process all fragments in the stack
            while (_processingStack.Count > 0 && !Context.ShouldStop)
            {
                var fragment = _processingStack.Pop();
                
                // Skip if already visited
                int fragmentId = RuntimeHelpers.GetHashCode(fragment);
                if (_visitedFragments.Contains(fragmentId))
                    continue;
                
                // Mark as visited
                _visitedFragments.Add(fragmentId);
                
                // Push the current fragment to context
                Context.PushFragment(fragment);
                
                try
                {
                    // Process this fragment
                    fragment.Accept(this);
                    
                    // Add children to the stack in reverse order
                    // so they're processed in the original order
                    foreach (var child in GetChildren(fragment).Reverse())
                    {
                        if (child != null)
                        {
                            _processingStack.Push(child);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Log the error but continue processing
                    LogError($"Error processing {fragment.GetType().Name}", ex);
                }
                finally
                {
                    // Pop the current fragment from context
                    Context.PopFragment();
                }
            }
            
            // Log if processing was stopped
            if (Context.ShouldStop)
            {
                LogWarning($"Processing stopped after {Context.FragmentVisitCount} fragments " +
                           $"({(DateTime.UtcNow - Context.StartTime).TotalSeconds:F1} seconds)");
            }
        }
        
        /// <summary>
        /// Default implementation of Visit for recursive traversal
        /// </summary>
        public override void Visit(TSqlFragment fragment)
        {
            if (fragment == null) return;
            
            // Check if we should stop processing
            Context.CheckProcessingLimits();
            if (Context.ShouldStop) return;
            
            // Check if already visited
            int fragmentId = RuntimeHelpers.GetHashCode(fragment);
            if (_visitedFragments.Contains(fragmentId)) return;
            
            // Mark as visited
            _visitedFragments.Add(fragmentId);
            
            // Push the current fragment to context
            Context.PushFragment(fragment);
            
            try
            {
                // Process this fragment normally
                fragment.Accept(this);
            }
            catch (Exception ex)
            {
                // Log the error but continue processing
                LogError($"Error processing {fragment.GetType().Name}", ex);
            }
            finally
            {
                // Pop the current fragment from context
                Context.PopFragment();
            }
        }
        
        /// <summary>
        /// Gets child elements of an AST fragment
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
            if (fragment == null)
                return string.Empty;

            var generator = new Sql160ScriptGenerator();
            var builder = new System.Text.StringBuilder();

            using (var writer = new System.IO.StringWriter(builder))
            {
                generator.GenerateScript(fragment, writer);
            }

            return builder.ToString();
        }
        
        /// <summary>
        /// Creates a unique ID for a node
        /// </summary>
        protected string CreateNodeId(string prefix, string name)
        {
            return $"{prefix}_{Guid.NewGuid():N}_{InternString(name).Replace(".", "_")}";
        }
        
        /// <summary>
        /// Creates a random ID
        /// </summary>
        protected string CreateRandomId()
        {
            return Guid.NewGuid().ToString("N");
        }
        
        /// <summary>
        /// Creates a direct edge between two nodes
        /// </summary>
        protected LineageEdge CreateDirectEdge(string sourceId, string targetId, string operation, string sqlExpression = "")
        {
            return new LineageEdge
            {
                Id = CreateRandomId(),
                SourceId = sourceId,
                TargetId = targetId,
                Type = EdgeType.Direct.ToString(),
                Operation = InternString(operation),
                SqlExpression = sqlExpression ?? string.Empty
            };
        }
        
        /// <summary>
        /// Creates an indirect edge between two nodes
        /// </summary>
        protected LineageEdge CreateIndirectEdge(string sourceId, string targetId, string operation, string sqlExpression = "")
        {
            return new LineageEdge
            {
                Id = CreateRandomId(),
                SourceId = sourceId,
                TargetId = targetId,
                Type = EdgeType.Indirect.ToString(),
                Operation = InternString(operation),
                SqlExpression = sqlExpression ?? string.Empty
            };
        }
        
        /// <summary>
        /// Interns a string to reduce memory usage
        /// </summary>
        protected string InternString(string str)
        {
            if (string.IsNullOrEmpty(str))
                return str;
                
            if (!_stringPool.TryGetValue(str, out var internedString))
            {
                _stringPool[str] = str;
                return str;
            }
            return internedString;
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
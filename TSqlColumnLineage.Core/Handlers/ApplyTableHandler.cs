using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Visitors;
using System;
using System.Reflection;

namespace TSqlColumnLineage.Core.Handlers
{
    /// <summary>
    /// Handler for APPLY operations (CROSS APPLY, OUTER APPLY)
    /// </summary>
    public class ApplyTableHandler
    {
        private readonly ColumnLineageVisitor _visitor;
        private readonly LineageGraph _graph;
        private readonly LineageContext _context;
        private readonly ILogger _logger;

        public ApplyTableHandler(ColumnLineageVisitor visitor, LineageGraph graph, LineageContext context, ILogger logger)
        {
            _visitor = visitor ?? throw new ArgumentNullException(nameof(visitor));
            _graph = graph ?? throw new ArgumentNullException(nameof(graph));
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _logger = logger;
        }

        /// <summary>
        /// Processes APPLY operation
        /// </summary>
        /// <param name="node">APPLY operation node</param>
        public void ProcessApply(QualifiedJoin node)
        {
            // Check if it's an APPLY operation - using a version-agnostic approach
            bool isApply = IsApplyJoin(node);
            if (!isApply)
            {
                // Not an APPLY operation, return
                return;
            }

            string applyType = GetJoinTypeName(node);
            _logger?.LogDebug($"Processing {applyType} at Line {node.StartLine}");

            // Process left table
            if (node.FirstTableReference != null)
            {
                _visitor.Visit(node.FirstTableReference);
            }

            // Set special context flag for APPLY
            _context.Metadata["inApply"] = true;
            _context.Metadata["applyType"] = applyType;

            try
            {
                // Process right table (usually a table-valued function or subquery)
                if (node.SecondTableReference != null)
                {
                    _visitor.Visit(node.SecondTableReference);
                }
            }
            finally
            {
                _context.Metadata.Remove("inApply");
                _context.Metadata.Remove("applyType");
            }

            // In case of CROSS APPLY or OUTER APPLY, relationships between columns will be
            // established during processing of column references from the left table
            // in the right table (in subquery or table-valued function)
        }
        
        /// <summary>
        /// Checks if the join is an APPLY operation using reflection for version compatibility
        /// </summary>
        private bool IsApplyJoin(QualifiedJoin node)
        {
            try
            {
                // Different versions of ScriptDom use different enumerations for join types
                // Try different approaches to determine if it's an APPLY operation
                
                // Method 1: Try direct enum comparison if "Outer" value exists
                var joinTypeValue = node.QualifiedJoinType.ToString();
                if (joinTypeValue.Contains("Apply"))
                {
                    return true;
                }
                
                // Method 2: Try to get the join hint property if it exists
                var applyProperty = node.GetType().GetProperty("ApplyType") ?? 
                                    node.GetType().GetProperty("ApplyKind");
                if (applyProperty != null)
                {
                    var applyValue = applyProperty.GetValue(node);
                    return applyValue != null && 
                           !Equals(applyValue, Activator.CreateInstance(applyValue.GetType()));
                }
                
                // Method 3: Check for specific join syntax indicator
                var scriptGen = node.ScriptTokenStream;
                if (scriptGen != null)
                {
                    foreach (var token in scriptGen)
                    {
                        if (token.Text.EndsWith("APPLY", StringComparison.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"Error checking APPLY join type: {ex.Message}");
            }
            
            return false;
        }
        
        /// <summary>
        /// Gets the join type name in a version-agnostic way
        /// </summary>
        private string GetJoinTypeName(QualifiedJoin node)
        {
            try
            {
                return node.QualifiedJoinType.ToString();
            }
            catch
            {
                // Fallback
                return "Apply";
            }
        }
    }
}

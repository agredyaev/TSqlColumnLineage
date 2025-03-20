using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;
using System.Linq;

namespace TSqlColumnLineage.Core.Common.Extensions
{
    public static class TSqlFragmentExtensions
    {
        /// <summary>
        /// Gets all descendant TSqlFragments of a specific type.
        /// </summary>
        /// <typeparam name="T">The type of TSqlFragment to find.</typeparam>
        /// <param name="fragment">The root TSqlFragment to search within.</param>
        /// <returns>A collection of descendant TSqlFragments of the specified type.</returns>
        public static IEnumerable<T> GetDescendantsOfType<T>(this TSqlFragment fragment) where T : TSqlFragment
        {
            var descendants = new List<T>();
            var visitor = new DescendantFragmentVisitor<T>(descendants);
            fragment.Accept(visitor);
            return descendants;
        }

        private class DescendantFragmentVisitor<T> : TSqlFragmentVisitor where T : TSqlFragment
        {
            private readonly List<T> _descendants;

            public DescendantFragmentVisitor(List<T> descendants)
            {
                _descendants = descendants;
            }

            public override void Visit(TSqlFragment fragment)
            {
                if (fragment is T descendant)
                {
                    _descendants.Add(descendant);
                }
                base.Visit(fragment);
            }
        }
    }
}

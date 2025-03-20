using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TSqlColumnLineage.Core.Parsing
{
    /// <summary>
    /// Utility methods for working with ScriptDom
    /// </summary>
    public static class ScriptDomUtils
    {
        /// <summary>
        /// Finds all fragments of a specific type in the AST tree
        /// </summary>
        /// <typeparam name="T">Type of fragments to find</typeparam>
        /// <param name="root">Root fragment</param>
        /// <returns>List of found fragments</returns>
        public static IEnumerable<T> FindAllFragments<T>(TSqlFragment root) where T : TSqlFragment
        {
            List<T> results = new List<T>();
            CollectFragments(root, results);
            return results;
        }

        private static void CollectFragments<T>(TSqlFragment fragment, List<T> results) where T : TSqlFragment
        {
            if (fragment == null)
                return;

            if (fragment is T match)
            {
                results.Add(match);
            }

            foreach (var child in GetChildren(fragment))
            {
                CollectFragments(child, results);
            }
        }

        /// <summary>
        /// Gets child elements of an AST fragment
        /// </summary>
        /// <param name="fragment">Parent fragment</param>
        /// <returns>List of child fragments</returns>
        public static IEnumerable<TSqlFragment> GetChildren(TSqlFragment fragment)
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
                    foreach (var child in childFragments)
                    {
                        if (child != null)
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
        /// Gets the text representation of an AST fragment
        /// </summary>
        /// <param name="fragment">AST fragment</param>
        /// <returns>Text representation</returns>
        public static string GetFragmentSql(TSqlFragment fragment)
        {
            if (fragment == null)
                return string.Empty;

            var generator = new Sql160ScriptGenerator();
            var builder = new StringBuilder();

            using (var writer = new System.IO.StringWriter(builder))
            {
                generator.GenerateScript(fragment, writer);
            }

            return builder.ToString();
        }

        /// <summary>
        /// Gets the full column name with table alias (if any)
        /// </summary>
        /// <param name="column">Column reference</param>
        /// <returns>Full column name</returns>
        public static string GetFullColumnName(ColumnReferenceExpression column)
        {
            if (column == null)
                return string.Empty;

            string multiPartIdentifier = string.Empty;

            if (column.MultiPartIdentifier != null && column.MultiPartIdentifier.Identifiers.Count > 0)
            {
                multiPartIdentifier = string.Join(".",
                    column.MultiPartIdentifier.Identifiers.Select(i => i.Value));
            }

            return multiPartIdentifier;
        }

        /// <summary>
        /// Gets the table name from a table reference
        /// </summary>
        /// <param name="tableReference">Table reference</param>
        /// <returns>Table name</returns>
        public static string GetTableName(TableReference tableReference)
        {
            if (tableReference == null)
                return string.Empty;

            if (tableReference is NamedTableReference namedTable)
            {
                string tableName = string.Empty;

                if (namedTable.SchemaObject != null && namedTable.SchemaObject.Identifiers.Count > 0)
                {
                    tableName = string.Join(".",
                        namedTable.SchemaObject.Identifiers.Select(i => i.Value));
                }

                return tableName;
            }

            return string.Empty;
        }

        /// <summary>
        /// Gets the table alias (if any)
        /// </summary>
        /// <param name="tableReference">Table reference</param>
        /// <returns>Table alias or null if no alias is specified</returns>
        public static string GetTableAlias(TableReference tableReference)
        {
            if (tableReference == null)
                return null;

            if (tableReference is TableReferenceWithAlias tableWithAlias &&
                tableWithAlias.Alias != null)
            {
                return tableWithAlias.Alias.Value;
            }

            return null;
        }
    }
}

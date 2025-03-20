using Microsoft.SqlServer.TransactSql.ScriptDom;
using TSqlColumnLineage.Core.Models;
using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Visitors
{
    /// <summary>
    /// Context for processing SELECT statements
    /// </summary>
    public class SelectStatementContext
    {
        /// <summary>
        /// Parent SELECT statement
        /// </summary>
        public SelectStatement SelectStatement { get; set; }

        /// <summary>
        /// Output columns of the query
        /// </summary>
        public Dictionary<string, ColumnNode> OutputColumns { get; set; } = new Dictionary<string, ColumnNode>();

        /// <summary>
        /// Alias of the current query (if it's a subquery)
        /// </summary>
        public string QueryAlias { get; set; }
    }
}

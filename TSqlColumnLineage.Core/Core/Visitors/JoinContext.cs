using TSqlColumnLineage.Core.Models;

namespace TSqlColumnLineage.Core.Visitors
{
    /// <summary>
    /// Context for processing JOIN clauses
    /// </summary>
    public class JoinContext
    {
        /// <summary>
        /// Type of JOIN (INNER, LEFT, RIGHT, FULL, CROSS)
        /// </summary>
        public string JoinType { get; set; }

        /// <summary>
        /// Left table in the JOIN
        /// </summary>
        public TableNode LeftTable { get; set; }

        /// <summary>
        /// Right table in the JOIN
        /// </summary>
        public TableNode RightTable { get; set; }
    }
}

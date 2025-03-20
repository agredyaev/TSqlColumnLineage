using TSqlColumnLineage.Core.Models;

namespace TSqlColumnLineage.Core.Visitors
{
    /// <summary>
    /// Context for processing column-related expressions
    /// </summary>
    public class ColumnLineageContext
    {
        /// <summary>
        /// Target column for which lineage is being built
        /// </summary>
        public ColumnNode? TargetColumn { get; set; }

        /// <summary>
        /// Type of dependency (direct, transform, filter, join)
        /// </summary>
        public string DependencyType { get; set; } = "direct";
    }
}

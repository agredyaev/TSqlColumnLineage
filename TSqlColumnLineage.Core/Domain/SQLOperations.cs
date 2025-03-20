using System;
using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Domain
{
    /// <summary>
    /// Represents SQL operation types for lineage tracking
    /// </summary>
    public enum SqlOperationType
    {
        Select,
        Insert,
        Update,
        Delete,
        Merge,
        Join,
        SubQuery,
        Cte,
        Union,
        Intersect,
        Except,
        Pivot,
        Unpivot,
        Apply,
        GroupBy,
        OrderBy,
        Window,
        Case,
        Cast,
        Convert,
        Coalesce,
        Function,
        Expression,
        Parameter,
        Variable,
        Unknown
    }

    /// <summary>
    /// Represents a SQL operation that contributes to column lineage
    /// Optimized for memory efficiency using data-oriented design principles
    /// </summary>
    /// <remarks>
    /// Creates a new SQL operation
    /// </remarks>
    public sealed class SqlOperation(int id, SqlOperationType type, string name, string sqlText = "", string sourceLocation = "")
    {
        // Identity
        public int Id { get; } = id;
        public SqlOperationType Type { get; } = type;

        // Operation details
        public string Name { get; } = name ?? string.Empty;
        public string SqlText { get; } = sqlText ?? string.Empty;

        // Lineage relationships
        public List<int> SourceColumns { get; } = [];
        public List<int> TargetColumns { get; } = [];

        // Context information
        public string SourceLocation { get; } = sourceLocation ?? string.Empty;
        public Dictionary<string, object> Metadata { get; } = [];

        /// <summary>
        /// Adds a source column to this operation
        /// </summary>
        public void AddSourceColumn(int columnId)
        {
            if (!SourceColumns.Contains(columnId))
            {
                SourceColumns.Add(columnId);
            }
        }

        /// <summary>
        /// Adds a target column to this operation
        /// </summary>
        public void AddTargetColumn(int columnId)
        {
            if (!TargetColumns.Contains(columnId))
            {
                TargetColumns.Add(columnId);
            }
        }

        /// <summary>
        /// Sets a metadata value
        /// </summary>
        public void SetMetadata(string key, object value)
        {
            if (!string.IsNullOrEmpty(key))
            {
                Metadata[key] = value;
            }
        }

        /// <summary>
        /// Gets a metadata value
        /// </summary>
        public object? GetMetadata(string key)
        {
            if (string.IsNullOrEmpty(key) || !Metadata.TryGetValue(key, out var value))
            {
                return null;
            }

            return value;
        }

        public override string ToString()
        {
            return $"{Type} '{Name}' (Sources: {SourceColumns.Count}, Targets: {TargetColumns.Count})";
        }
    }
}
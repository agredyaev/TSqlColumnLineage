using System;
using System.Collections.Generic;

namespace TSqlColumnLineage.Domain
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
    public sealed class SqlOperation
    {
        // Identity
        public int Id { get; }
        public SqlOperationType Type { get; }
        
        // Operation details
        public string Name { get; }
        public string SqlText { get; }
        
        // Lineage relationships
        public List<int> SourceColumns { get; } = new List<int>();
        public List<int> TargetColumns { get; } = new List<int>();
        
        // Context information
        public string SourceLocation { get; }
        public Dictionary<string, object> Metadata { get; } = new Dictionary<string, object>();
        
        /// <summary>
        /// Creates a new SQL operation
        /// </summary>
        public SqlOperation(int id, SqlOperationType type, string name, string sqlText = "", string sourceLocation = "")
        {
            Id = id;
            Type = type;
            Name = name ?? string.Empty;
            SqlText = sqlText ?? string.Empty;
            SourceLocation = sourceLocation ?? string.Empty;
        }
        
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
        public object GetMetadata(string key)
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
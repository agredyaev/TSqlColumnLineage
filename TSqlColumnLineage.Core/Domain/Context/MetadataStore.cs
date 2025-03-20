using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace TSqlColumnLineage.Core.Domain.Context
{
    /// <summary>
    /// Stores and manages schema metadata for SQL objects during lineage analysis.
    /// Optimized for concurrent access and memory efficiency using data-oriented design.
    /// </summary>
    public sealed class MetadataStore
    {
        // Tables metadata
        private readonly ConcurrentDictionary<string, TableMetadata> _tables =
            new(StringComparer.OrdinalIgnoreCase);

        // Columns metadata
        private readonly ConcurrentDictionary<(string TableName, string ColumnName), ColumnMetadata> _columns =
            new();

        // Types metadata
        private readonly ConcurrentDictionary<string, string> _typeAliases =
            new(StringComparer.OrdinalIgnoreCase);

        // String pool for memory optimization
        private readonly StringPool _stringPool = new();

        /// <summary>
        /// Creates a new metadata store with default type mappings
        /// </summary>
        public MetadataStore()
        {
            InitializeTypeAliases();
        }

        /// <summary>
        /// Adds table metadata
        /// </summary>
        public void AddTable(string tableName, string schema = "dbo", string database = "")
        {
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));

            tableName = _stringPool.Intern(tableName);
            schema = _stringPool.Intern(schema ?? "dbo");
            database = _stringPool.Intern(database);

            _tables[tableName] = new TableMetadata
            {
                Name = tableName,
                Schema = schema,
                Database = database
            };
        }

        /// <summary>
        /// Adds column metadata
        /// </summary>
        public void AddColumn(string tableName, string columnName, string dataType, bool isNullable = true, bool isPrimaryKey = false)
        {
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentException("Table name cannot be null or empty", nameof(tableName));

            if (string.IsNullOrEmpty(columnName))
                throw new ArgumentException("Column name cannot be null or empty", nameof(columnName));

            tableName = _stringPool.Intern(tableName);
            columnName = _stringPool.Intern(columnName);
            dataType = _stringPool.Intern(dataType);

            _columns[(tableName, columnName)] = new ColumnMetadata
            {
                TableName = tableName,
                Name = columnName,
                DataType = ResolveType(dataType),
                IsNullable = isNullable,
                IsPrimaryKey = isPrimaryKey
            };

            // Ensure table exists
            if (!_tables.ContainsKey(tableName))
            {
                AddTable(tableName);
            }
        }

        /// <summary>
        /// Gets table metadata
        /// </summary>
        public TableMetadata? GetTable(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return null;

            tableName = _stringPool.Intern(tableName);

            if (_tables.TryGetValue(tableName, out var metadata))
            {
                return metadata;
            }

            return null;
        }

        /// <summary>
        /// Gets column metadata
        /// </summary>
        public ColumnMetadata? GetColumn(string tableName, string columnName)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(columnName))
                return null;

            tableName = _stringPool.Intern(tableName);
            columnName = _stringPool.Intern(columnName);

            if (_columns.TryGetValue((tableName, columnName), out var metadata))
            {
                return metadata;
            }

            return null;
        }

        /// <summary>
        /// Gets all columns for a table
        /// </summary>
        public List<ColumnMetadata> GetTableColumns(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return [];

            tableName = _stringPool.Intern(tableName);

            var result = new List<ColumnMetadata>();

            foreach (var key in _columns.Keys)
            {
                if (key.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase))
                {
                    if (_columns.TryGetValue(key, out var metadata))
                    {
                        result.Add(metadata);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Gets all tables
        /// </summary>
        public List<TableMetadata> GetAllTables()
        {
            return [.. _tables.Values];
        }

        /// <summary>
        /// Adds a type alias
        /// </summary>
        public void AddTypeAlias(string alias, string actualType)
        {
            if (string.IsNullOrEmpty(alias) || string.IsNullOrEmpty(actualType))
                return;

            alias = _stringPool.Intern(alias);
            actualType = _stringPool.Intern(actualType);

            _typeAliases[alias] = actualType;
        }

        /// <summary>
        /// Resolves a type alias to the actual type
        /// </summary>
        public string ResolveType(string type)
        {
            if (string.IsNullOrEmpty(type))
                return "unknown";

            type = _stringPool.Intern(type);

            // Normalize the type (remove parentheses and parameters)
            int parenIndex = type.IndexOf('(');
            if (parenIndex > 0)
            {
                type = type[..parenIndex].Trim();
            }

            // Resolve alias
            if (_typeAliases.TryGetValue(type, out var actualType))
            {
                return actualType;
            }

            return type;
        }

        /// <summary>
        /// Clears all metadata
        /// </summary>
        public void Clear()
        {
            _tables.Clear();
            _columns.Clear();
        }

        /// <summary>
        /// Initializes common SQL Server type aliases
        /// </summary>
        private void InitializeTypeAliases()
        {
            // Numeric types
            AddTypeAlias("bit", "boolean");
            AddTypeAlias("tinyint", "byte");
            AddTypeAlias("smallint", "int16");
            AddTypeAlias("int", "int32");
            AddTypeAlias("bigint", "int64");
            AddTypeAlias("decimal", "decimal");
            AddTypeAlias("numeric", "decimal");
            AddTypeAlias("money", "decimal");
            AddTypeAlias("smallmoney", "decimal");
            AddTypeAlias("float", "double");
            AddTypeAlias("real", "float");

            // String types
            AddTypeAlias("char", "string");
            AddTypeAlias("varchar", "string");
            AddTypeAlias("nchar", "string");
            AddTypeAlias("nvarchar", "string");
            AddTypeAlias("text", "string");
            AddTypeAlias("ntext", "string");

            // Date/time types
            AddTypeAlias("date", "date");
            AddTypeAlias("time", "time");
            AddTypeAlias("datetime", "datetime");
            AddTypeAlias("datetime2", "datetime");
            AddTypeAlias("smalldatetime", "datetime");
            AddTypeAlias("datetimeoffset", "datetimeoffset");

            // Binary types
            AddTypeAlias("binary", "binary");
            AddTypeAlias("varbinary", "binary");
            AddTypeAlias("image", "binary");

            // Other types
            AddTypeAlias("uniqueidentifier", "guid");
            AddTypeAlias("xml", "xml");
            AddTypeAlias("sql_variant", "object");
            AddTypeAlias("hierarchyid", "hierarchyid");
            AddTypeAlias("geometry", "geometry");
            AddTypeAlias("geography", "geography");
        }

        /// <summary>
        /// Simple string pool for memory optimization
        /// </summary>
        private class StringPool
        {
            private readonly ConcurrentDictionary<string, string> _pool =
                new(StringComparer.Ordinal);

            public string Intern(string str)
            {
                if (string.IsNullOrEmpty(str))
                    return str;

                return _pool.GetOrAdd(str, str);
            }
        }
    }

    /// <summary>
    /// Table metadata
    /// </summary>
    public class TableMetadata
    {
        public string Name { get; set; } = string.Empty;
        public string Schema { get; set; } = string.Empty;
        public string Database { get; set; } = string.Empty;
        public bool IsTemporary => Name?.StartsWith("#") ?? false;

        public override string ToString()
        {
            if (string.IsNullOrEmpty(Database))
            {
                return $"{Schema}.{Name}";
            }

            return $"{Database}.{Schema}.{Name}";
        }
    }

    /// <summary>
    /// Column metadata
    /// </summary>
    public class ColumnMetadata
    {
        public string TableName { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public bool IsNullable { get; set; }
        public bool IsPrimaryKey { get; set; }

        public override string ToString()
        {
            return $"{TableName}.{Name} ({DataType}){(IsNullable ? " NULL" : " NOT NULL")}{(IsPrimaryKey ? " PK" : "")}";
        }
    }
}
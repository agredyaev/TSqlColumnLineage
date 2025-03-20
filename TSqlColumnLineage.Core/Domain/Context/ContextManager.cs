using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using TSqlColumnLineage.Core.Domain.Graph;

namespace TSqlColumnLineage.Core.Domain.Context
{
    /// <summary>
    /// Manages execution context for SQL queries during lineage analysis.
    /// Optimized for memory efficiency and concurrent access using data-oriented design.
    /// </summary>
    public sealed class ContextManager
    {
        // String pool for memory optimization
        private readonly StringPool _stringPool = new();

        // Scopes stack for each processing thread
        private readonly ThreadLocal<Stack<ScopeFrame>> _scopeStacks =
            new(() => new Stack<ScopeFrame>());

        // Global variable declarations (thread-safe)
        private readonly ConcurrentDictionary<string, VariableInfo> _globalVariables =
            new(StringComparer.OrdinalIgnoreCase);

        // Table aliases (thread-safe)
        private readonly ConcurrentDictionary<string, string> _tableAliases =
            new(StringComparer.OrdinalIgnoreCase);

        // Known tables and columns (thread-safe)
        private readonly ConcurrentDictionary<string, int> _tables =
            new(StringComparer.OrdinalIgnoreCase);

        // Temporary tables (thread-safe)
        private readonly ConcurrentDictionary<string, int> _tempTables =
            new(StringComparer.OrdinalIgnoreCase);

        // Common table expressions (thread-safe)
        private readonly ConcurrentDictionary<string, int> _cteDefinitions =
            new(StringComparer.OrdinalIgnoreCase);

        // Table variable declarations (thread-safe)
        private readonly ConcurrentDictionary<string, int> _tableVariables =
            new(StringComparer.OrdinalIgnoreCase);

        // Procedures and functions (thread-safe)
        private readonly ConcurrentDictionary<string, int> _procedures =
            new(StringComparer.OrdinalIgnoreCase);

        // Global flags and state
        private readonly ConcurrentDictionary<string, object> _state =
            new();

        // CancellationToken source for stopping processing
        private readonly CancellationTokenSource _cancellationSource = new();

        // The lineage graph being built
        private readonly LineageGraph _graph;

        /// <summary>
        /// Gets whether processing should be stopped
        /// </summary>
        public bool ShouldStop => _cancellationSource.IsCancellationRequested;

        /// <summary>
        /// Gets the cancellation token
        /// </summary>
        public CancellationToken CancellationToken => _cancellationSource.Token;

        /// <summary>
        /// Gets the lineage graph
        /// </summary>
        public LineageGraph Graph => _graph;

        /// <summary>
        /// Creates a new context manager
        /// </summary>
        public ContextManager(LineageGraph graph)
        {
            _graph = graph ?? throw new ArgumentNullException(nameof(graph));

            // Push the global scope
            EnsureScopeStack().Push(new ScopeFrame(ScopeType.Global));
        }

        /// <summary>
        /// Pushes a new scope onto the stack
        /// </summary>
        public void PushScope(ScopeType scopeType, string name = "")
        {
            name = _stringPool.Intern(name);
            EnsureScopeStack().Push(new ScopeFrame(scopeType, name));
        }

        /// <summary>
        /// Pops the current scope from the stack
        /// </summary>
        public void PopScope()
        {
            var stack = EnsureScopeStack();
            if (stack.Count <= 1)
            {
                // Don't pop the global scope
                return;
            }

            stack.Pop();
        }

        /// <summary>
        /// Creates a scope that will be automatically popped when disposed
        /// </summary>
        public IDisposable CreateScope(ScopeType scopeType, string name = "")
        {
            PushScope(scopeType, name);
            return new ScopeDisposer(this);
        }

        /// <summary>
        /// Declares a variable in the current scope
        /// </summary>
        public void DeclareVariable(string name, string dataType, object value = null, bool isParameter = false)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Variable name cannot be null or empty", nameof(name));

            name = _stringPool.Intern(name);
            dataType = _stringPool.Intern(dataType);

            var scope = GetCurrentScope();

            if (scope.ScopeType == ScopeType.Global)
            {
                // Global variable
                _globalVariables[name] = new VariableInfo
                {
                    Name = name,
                    DataType = dataType,
                    Value = value,
                    IsParameter = isParameter
                };
            }
            else
            {
                // Local variable
                scope.Variables[name] = new VariableInfo
                {
                    Name = name,
                    DataType = dataType,
                    Value = value,
                    IsParameter = isParameter
                };
            }
        }

        /// <summary>
        /// Sets a variable value in the current scope chain
        /// </summary>
        public void SetVariable(string name, object value)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Variable name cannot be null or empty", nameof(name));

            name = _stringPool.Intern(name);

            // Try to find variable in current scope chain
            var stack = EnsureScopeStack();
            for (int i = stack.Count - 1; i >= 0; i--)
            {
                var scope = stack.ElementAt(i);
                if (scope.Variables.TryGetValue(name, out var varInfo))
                {
                    scope.Variables[name] = new VariableInfo
                    {
                        Name = varInfo.Name,
                        DataType = varInfo.DataType,
                        Value = value,
                        IsParameter = varInfo.IsParameter
                    };
                    return;
                }
            }

            // Check global variables
            if (_globalVariables.TryGetValue(name, out var globalVarInfo))
            {
                _globalVariables[name] = new VariableInfo
                {
                    Name = globalVarInfo.Name,
                    DataType = globalVarInfo.DataType,
                    Value = value,
                    IsParameter = globalVarInfo.IsParameter
                };
                return;
            }

            // Variable not found, create in current scope
            var currentScope = GetCurrentScope();
            currentScope.Variables[name] = new VariableInfo
            {
                Name = name,
                DataType = "unknown",
                Value = value,
                IsParameter = false
            };
        }

        /// <summary>
        /// Gets a variable value from the current scope chain
        /// </summary>
        public VariableInfo? GetVariable(string name)
        {
            if (string.IsNullOrEmpty(name))
                return null;

            name = _stringPool.Intern(name);

            // Try to find variable in current scope chain
            var stack = EnsureScopeStack();
            for (int i = stack.Count - 1; i >= 0; i--)
            {
                var scope = stack.ElementAt(i);
                if (scope.Variables.TryGetValue(name, out var varInfo))
                {
                    return varInfo;
                }
            }

            // Check global variables
            if (_globalVariables.TryGetValue(name, out var globalVarInfo))
            {
                return globalVarInfo;
            }

            return null;
        }

        /// <summary>
        /// Adds a table alias
        /// </summary>
        public void AddTableAlias(string alias, string tableName)
        {
            if (string.IsNullOrEmpty(alias) || string.IsNullOrEmpty(tableName))
                return;

            alias = _stringPool.Intern(alias);
            tableName = _stringPool.Intern(tableName);

            _tableAliases[alias] = tableName;
        }

        /// <summary>
        /// Resolves a table name from an alias
        /// </summary>
        public string ResolveTableAlias(string nameOrAlias)
        {
            if (string.IsNullOrEmpty(nameOrAlias))
                return null;

            nameOrAlias = _stringPool.Intern(nameOrAlias);

            if (_tableAliases.TryGetValue(nameOrAlias, out var tableName))
            {
                return tableName;
            }

            return nameOrAlias;
        }

        /// <summary>
        /// Registers a table
        /// </summary>
        public void RegisterTable(string tableName, int tableId)
        {
            if (string.IsNullOrEmpty(tableName))
                return;

            tableName = _stringPool.Intern(tableName);

            if (tableName.StartsWith("#"))
            {
                // Temporary table
                _tempTables[tableName] = tableId;
            }
            else if (tableName.StartsWith("@"))
            {
                // Table variable
                _tableVariables[tableName] = tableId;
            }
            else
            {
                // Regular table
                _tables[tableName] = tableId;
            }
        }

        /// <summary>
        /// Registers a CTE
        /// </summary>
        public void RegisterCte(string cteName, int tableId)
        {
            if (string.IsNullOrEmpty(cteName))
                return;

            cteName = _stringPool.Intern(cteName);
            _cteDefinitions[cteName] = tableId;
        }

        /// <summary>
        /// Registers a procedure
        /// </summary>
        public void RegisterProcedure(string procedureName, int procedureId)
        {
            if (string.IsNullOrEmpty(procedureName))
                return;

            procedureName = _stringPool.Intern(procedureName);
            _procedures[procedureName] = procedureId;
        }

        /// <summary>
        /// Gets a table ID
        /// </summary>
        public int GetTableId(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return -1;

            tableName = _stringPool.Intern(tableName);
            tableName = ResolveTableAlias(tableName);

            // Check temporary tables
            if (tableName.StartsWith("#") && _tempTables.TryGetValue(tableName, out var tempId))
            {
                return tempId;
            }

            // Check table variables
            if (tableName.StartsWith("@") && _tableVariables.TryGetValue(tableName, out var varId))
            {
                return varId;
            }

            // Check CTEs
            if (_cteDefinitions.TryGetValue(tableName, out var cteId))
            {
                return cteId;
            }

            // Check regular tables
            if (_tables.TryGetValue(tableName, out var tableId))
            {
                return tableId;
            }

            return -1;
        }

        /// <summary>
        /// Gets a procedure ID
        /// </summary>
        public int GetProcedureId(string procedureName)
        {
            if (string.IsNullOrEmpty(procedureName))
                return -1;

            procedureName = _stringPool.Intern(procedureName);

            if (_procedures.TryGetValue(procedureName, out var procId))
            {
                return procId;
            }

            return -1;
        }

        /// <summary>
        /// Gets a column node
        /// </summary>
        public int GetColumnNode(string tableName, string columnName)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(columnName))
                return -1;

            tableName = _stringPool.Intern(tableName);
            columnName = _stringPool.Intern(columnName);

            // Resolve alias
            tableName = ResolveTableAlias(tableName);

            return _graph.GetColumnNode(tableName, columnName);
        }

        /// <summary>
        /// Sets a state value
        /// </summary>
        public void SetState(string key, object value)
        {
            if (string.IsNullOrEmpty(key))
                return;

            key = _stringPool.Intern(key);

            if (value == null)
            {
                _state.TryRemove(key, out _);
            }
            else
            {
                _state[key] = value;
            }
        }

        /// <summary>
        /// Gets a state value
        /// </summary>
        public object GetState(string key)
        {
            if (string.IsNullOrEmpty(key))
                return null;

            key = _stringPool.Intern(key);

            if (_state.TryGetValue(key, out var value))
            {
                return value;
            }

            return null;
        }

        /// <summary>
        /// Gets a boolean state value
        /// </summary>
        public bool GetBoolState(string key, bool defaultValue = false)
        {
            var value = GetState(key);

            if (value == null)
                return defaultValue;

            if (value is bool boolValue)
                return boolValue;

            if (value is string strValue)
                return !string.IsNullOrEmpty(strValue) && strValue.Equals("true", StringComparison.OrdinalIgnoreCase);

            return defaultValue;
        }

        /// <summary>
        /// Stops all processing
        /// </summary>
        public void StopProcessing()
        {
            _cancellationSource.Cancel();
        }

        /// <summary>
        /// Gets all tables
        /// </summary>
        public Dictionary<string, int> GetAllTables()
        {
            var result = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            // Add all regular tables
            foreach (var kvp in _tables)
            {
                result[kvp.Key] = kvp.Value;
            }

            // Add temporary tables
            foreach (var kvp in _tempTables)
            {
                result[kvp.Key] = kvp.Value;
            }

            // Add table variables
            foreach (var kvp in _tableVariables)
            {
                result[kvp.Key] = kvp.Value;
            }

            // Add CTEs
            foreach (var kvp in _cteDefinitions)
            {
                result[kvp.Key] = kvp.Value;
            }

            return result;
        }

        /// <summary>
        /// Ensures the scope stack exists for the current thread
        /// </summary>
        private Stack<ScopeFrame> EnsureScopeStack()
        {
            var stack = _scopeStacks.Value;

            if (stack.Count == 0)
            {
                // Initialize with global scope
                stack.Push(new ScopeFrame(ScopeType.Global));
            }

            return stack;
        }

        /// <summary>
        /// Gets the current scope
        /// </summary>
        private ScopeFrame GetCurrentScope()
        {
            return EnsureScopeStack().Peek();
        }

        /// <summary>
        /// Disposes resources
        /// </summary>
        public void Dispose()
        {
            _scopeStacks.Dispose();
            _cancellationSource.Dispose();
        }

        /// <summary>
        /// Disposable class for automatic scope management
        /// </summary>
        private class ScopeDisposer(ContextManager manager) : IDisposable
        {
            public void Dispose()
            {
                manager.PopScope();
            }
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
    /// Types of execution scopes
    /// </summary>
    public enum ScopeType
    {
        Global,         // Global scope
        Batch,          // Batch scope
        Procedure,      // Stored procedure scope
        Function,       // Function scope
        Block,          // BEGIN/END block scope
        If,             // IF statement scope
        While,          // WHILE loop scope
        TryCatch,       // TRY/CATCH block scope
        Query           // Query scope
    }

    /// <summary>
    /// Represents a scope frame
    /// </summary>
    internal class ScopeFrame(ScopeType scopeType, string name = "")
    {
        public ScopeType ScopeType { get; } = scopeType;
        public string Name { get; } = name;
        public Dictionary<string, VariableInfo> Variables { get; } = new Dictionary<string, VariableInfo>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Information about a variable
    /// </summary>
    public class VariableInfo
    {
        public string Name { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public object Value { get; set; } = string.Empty;
        public bool IsParameter { get; set; }

        public override string ToString()
        {
            return $"{Name} ({DataType}){(IsParameter ? " [Parameter]" : "")}";
        }
    }
}
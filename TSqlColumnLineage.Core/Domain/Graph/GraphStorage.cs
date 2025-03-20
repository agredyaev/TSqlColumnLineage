using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace TSqlColumnLineage.Core.Domain.Graph
{
    /// <summary>
    /// Structure-of-Arrays based graph storage optimized for column lineage processing.
    /// Implements data-oriented design principles for better cache locality and memory efficiency.
    /// </summary>
    public sealed class GraphStorage
    {
        // Node storage arrays
        private int _nodeCapacity;
        private int _nodeCount;
        private int[] _nodeIds = [];
        private NodeType[] _nodeTypes = [];
        private string[] _nodeNames = [];
        private string[] _nodeObjectNames = [];
        private string[] _nodeSchemaNames = [];
        private string[] _nodeDatabaseNames = [];
        private Dictionary<string, object>[] _nodeMetadata = [];

        // Column-specific arrays
        private string[] _columnDataTypes = [];
        private string[] _columnTableOwners = [];
        private bool[] _columnIsNullable = [];
        private bool[] _columnIsComputed = [];

        // Table-specific arrays
        private string[] _tableTableTypes = [];
        private string[] _tableAliases = [];
        private string[] _tableDefinitions = [];
        private int[][] _tableColumns = [];

        // Expression-specific arrays
        private string[] _expressionExpressionTypes = [];
        private string[] _expressionExpressions = [];
        private string[] _expressionResultTypes = [];
        private string[] _expressionTableOwners = [];

        // Edge storage arrays
        private int _edgeCapacity;
        private int _edgeCount;
        private int[] _edgeIds = [];
        private int[] _edgeSourceIds = [];
        private int[] _edgeTargetIds = [];
        private EdgeType[] _edgeTypes = [];
        private string[] _edgeOperations = [];
        private string[] _edgeSqlExpressions = [];

        // Lookup dictionaries for fast access
        private Dictionary<string, int> _nodeNameToId = [];
        private Dictionary<(string TableName, string ColumnName), int> _columnLookup = [];

        // Adjacency lists for fast traversal
        private List<int>[] _outgoingEdges = [];
        private List<int>[] _incomingEdges = [];

        // String pool for memory optimization
        private readonly StringPool _stringPool;

        // Thread synchronization
        private readonly ReaderWriterLockSlim[] _nodeLocks;
        private readonly ReaderWriterLockSlim[] _edgeLocks;
        private const int LockPartitions = 32;

        /// <summary>
        /// Initializes a new graph storage with specified initial capacity
        /// </summary>
        public GraphStorage(int initialNodeCapacity = 1024, int initialEdgeCapacity = 2048)
        {
            _nodeCapacity = initialNodeCapacity;
            _edgeCapacity = initialEdgeCapacity;
            _nodeCount = 0;
            _edgeCount = 0;

            // Initialize string pool
            _stringPool = new StringPool();

            // Initialize locks
            _nodeLocks = new ReaderWriterLockSlim[LockPartitions];
            _edgeLocks = new ReaderWriterLockSlim[LockPartitions];
            for (int i = 0; i < LockPartitions; i++)
            {
                _nodeLocks[i] = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
                _edgeLocks[i] = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
            }

            // Initialize storage arrays
            InitializeArrays();
        }

        /// <summary>
        /// Initializes all storage arrays with initial capacity
        /// </summary>
        private void InitializeArrays()
        {
            // Node storage
            _nodeIds = new int[_nodeCapacity];
            _nodeTypes = new NodeType[_nodeCapacity];
            _nodeNames = new string[_nodeCapacity];
            _nodeObjectNames = new string[_nodeCapacity];
            _nodeSchemaNames = new string[_nodeCapacity];
            _nodeDatabaseNames = new string[_nodeCapacity];
            _nodeMetadata = new Dictionary<string, object>[_nodeCapacity];

            // Column-specific storage
            _columnDataTypes = new string[_nodeCapacity];
            _columnTableOwners = new string[_nodeCapacity];
            _columnIsNullable = new bool[_nodeCapacity];
            _columnIsComputed = new bool[_nodeCapacity];

            // Table-specific storage
            _tableTableTypes = new string[_nodeCapacity];
            _tableAliases = new string[_nodeCapacity];
            _tableDefinitions = new string[_nodeCapacity];
            _tableColumns = new int[_nodeCapacity][];

            // Expression-specific storage
            _expressionExpressionTypes = new string[_nodeCapacity];
            _expressionExpressions = new string[_nodeCapacity];
            _expressionResultTypes = new string[_nodeCapacity];
            _expressionTableOwners = new string[_nodeCapacity];

            // Edge storage
            _edgeIds = new int[_edgeCapacity];
            _edgeSourceIds = new int[_edgeCapacity];
            _edgeTargetIds = new int[_edgeCapacity];
            _edgeTypes = new EdgeType[_edgeCapacity];
            _edgeOperations = new string[_edgeCapacity];
            _edgeSqlExpressions = new string[_edgeCapacity];

            // Lookup dictionaries
            _nodeNameToId = new Dictionary<string, int>(_nodeCapacity);
            _columnLookup = new Dictionary<(string, string), int>(_nodeCapacity);

            // Adjacency lists
            _outgoingEdges = new List<int>[_nodeCapacity];
            _incomingEdges = new List<int>[_nodeCapacity];

            // Initialize adjacency lists
            for (int i = 0; i < _nodeCapacity; i++)
            {
                _outgoingEdges[i] = [];
                _incomingEdges[i] = [];
                _nodeMetadata[i] = [];
            }
        }

        /// <summary>
        /// Ensures node storage has enough capacity
        /// </summary>
        private void EnsureNodeCapacity(int additionalNodes = 1)
        {
            if (_nodeCount + additionalNodes <= _nodeCapacity) return;

            // Double the capacity
            int newCapacity = Math.Max(_nodeCapacity * 2, _nodeCount + additionalNodes);

            // Resize all node arrays
            Array.Resize(ref _nodeIds, newCapacity);
            Array.Resize(ref _nodeTypes, newCapacity);
            Array.Resize(ref _nodeNames, newCapacity);
            Array.Resize(ref _nodeObjectNames, newCapacity);
            Array.Resize(ref _nodeSchemaNames, newCapacity);
            Array.Resize(ref _nodeDatabaseNames, newCapacity);
            Array.Resize(ref _nodeMetadata, newCapacity);

            Array.Resize(ref _columnDataTypes, newCapacity);
            Array.Resize(ref _columnTableOwners, newCapacity);
            Array.Resize(ref _columnIsNullable, newCapacity);
            Array.Resize(ref _columnIsComputed, newCapacity);

            Array.Resize(ref _tableTableTypes, newCapacity);
            Array.Resize(ref _tableAliases, newCapacity);
            Array.Resize(ref _tableDefinitions, newCapacity);
            Array.Resize(ref _tableColumns, newCapacity);

            Array.Resize(ref _expressionExpressionTypes, newCapacity);
            Array.Resize(ref _expressionExpressions, newCapacity);
            Array.Resize(ref _expressionResultTypes, newCapacity);
            Array.Resize(ref _expressionTableOwners, newCapacity);

            Array.Resize(ref _outgoingEdges, newCapacity);
            Array.Resize(ref _incomingEdges, newCapacity);

            // Initialize new items
            for (int i = _nodeCapacity; i < newCapacity; i++)
            {
                _outgoingEdges[i] = [];
                _incomingEdges[i] = [];
                _nodeMetadata[i] = [];
            }

            _nodeCapacity = newCapacity;
        }

        /// <summary>
        /// Ensures edge storage has enough capacity
        /// </summary>
        private void EnsureEdgeCapacity(int additionalEdges = 1)
        {
            if (_edgeCount + additionalEdges <= _edgeCapacity) return;

            // Double the capacity
            int newCapacity = Math.Max(_edgeCapacity * 2, _edgeCount + additionalEdges);

            // Resize all edge arrays
            Array.Resize(ref _edgeIds, newCapacity);
            Array.Resize(ref _edgeSourceIds, newCapacity);
            Array.Resize(ref _edgeTargetIds, newCapacity);
            Array.Resize(ref _edgeTypes, newCapacity);
            Array.Resize(ref _edgeOperations, newCapacity);
            Array.Resize(ref _edgeSqlExpressions, newCapacity);

            _edgeCapacity = newCapacity;
        }

        /// <summary>
        /// Adds a column node to the graph
        /// </summary>
        public int AddColumnNode(string name, string tableName, string dataType, bool isNullable = false, bool isComputed = false, string objectName = "", string schemaName = "", string databaseName = "")
        {
            name = _stringPool.Intern(name);
            tableName = _stringPool.Intern(tableName);
            dataType = _stringPool.Intern(dataType);
            objectName = _stringPool.Intern(objectName ?? name);
            schemaName = _stringPool.Intern(schemaName);
            databaseName = _stringPool.Intern(databaseName);

            int lockIndex = GetPartitionIndex(name);
            _nodeLocks[lockIndex].EnterWriteLock();

            try
            {
                // Check if the column already exists
                if (_columnLookup.TryGetValue((tableName, name), out int existingId))
                {
                    return existingId;
                }

                EnsureNodeCapacity();

                int nodeId = _nodeCount++;

                // Set node properties
                _nodeIds[nodeId] = nodeId;
                _nodeTypes[nodeId] = NodeType.Column;
                _nodeNames[nodeId] = name;
                _nodeObjectNames[nodeId] = objectName;
                _nodeSchemaNames[nodeId] = schemaName;
                _nodeDatabaseNames[nodeId] = databaseName;

                // Set column-specific properties
                _columnDataTypes[nodeId] = dataType;
                _columnTableOwners[nodeId] = tableName;
                _columnIsNullable[nodeId] = isNullable;
                _columnIsComputed[nodeId] = isComputed;

                // Update lookup dictionaries
                _nodeNameToId[name] = nodeId;
                _columnLookup[(tableName, name)] = nodeId;

                return nodeId;
            }
            finally
            {
                _nodeLocks[lockIndex].ExitWriteLock();
            }
        }

        /// <summary>
        /// Adds a table node to the graph
        /// </summary>
        public int AddTableNode(string name, string tableType, string alias = "", string definition = "", string objectName = "", string schemaName = "", string databaseName = "")
        {
            name = _stringPool.Intern(name);
            tableType = _stringPool.Intern(tableType);
            alias = _stringPool.Intern(alias);
            definition = _stringPool.Intern(definition);
            objectName = _stringPool.Intern(objectName ?? name);
            schemaName = _stringPool.Intern(schemaName);
            databaseName = _stringPool.Intern(databaseName);

            int lockIndex = GetPartitionIndex(name);
            _nodeLocks[lockIndex].EnterWriteLock();

            try
            {
                // Check if the table already exists
                if (_nodeNameToId.TryGetValue(name, out int existingId) && _nodeTypes[existingId] == NodeType.Table)
                {
                    return existingId;
                }

                EnsureNodeCapacity();

                int nodeId = _nodeCount++;

                // Set node properties
                _nodeIds[nodeId] = nodeId;
                _nodeTypes[nodeId] = NodeType.Table;
                _nodeNames[nodeId] = name;
                _nodeObjectNames[nodeId] = objectName;
                _nodeSchemaNames[nodeId] = schemaName;
                _nodeDatabaseNames[nodeId] = databaseName;

                // Set table-specific properties
                _tableTableTypes[nodeId] = tableType;
                _tableAliases[nodeId] = alias;
                _tableDefinitions[nodeId] = definition;
                _tableColumns[nodeId] = [];

                // Update lookup dictionary
                _nodeNameToId[name] = nodeId;

                return nodeId;
            }
            finally
            {
                _nodeLocks[lockIndex].ExitWriteLock();
            }
        }

        /// <summary>
        /// Adds an expression node to the graph
        /// </summary>
        public int AddExpressionNode(string name, string expressionText, string expressionType, string resultType = "", string tableOwner = "", string objectName = "", string schemaName = "", string databaseName = "")
        {
            name = _stringPool.Intern(name);
            // Don't intern the expression text as it can be large and unique
            expressionType = _stringPool.Intern(expressionType);
            resultType = _stringPool.Intern(resultType);
            tableOwner = _stringPool.Intern(tableOwner);
            objectName = _stringPool.Intern(objectName ?? name);
            schemaName = _stringPool.Intern(schemaName);
            databaseName = _stringPool.Intern(databaseName);

            int lockIndex = GetPartitionIndex(name);
            _nodeLocks[lockIndex].EnterWriteLock();

            try
            {
                EnsureNodeCapacity();

                int nodeId = _nodeCount++;

                // Set node properties
                _nodeIds[nodeId] = nodeId;
                _nodeTypes[nodeId] = NodeType.Expression;
                _nodeNames[nodeId] = name;
                _nodeObjectNames[nodeId] = objectName;
                _nodeSchemaNames[nodeId] = schemaName;
                _nodeDatabaseNames[nodeId] = databaseName;

                // Set expression-specific properties
                _expressionExpressionTypes[nodeId] = expressionType;
                _expressionExpressions[nodeId] = expressionText;
                _expressionResultTypes[nodeId] = resultType;
                _expressionTableOwners[nodeId] = tableOwner;

                // Update lookup dictionary
                _nodeNameToId[name] = nodeId;

                return nodeId;
            }
            finally
            {
                _nodeLocks[lockIndex].ExitWriteLock();
            }
        }

        /// <summary>
        /// Adds a column to a table
        /// </summary>
        public void AddColumnToTable(int tableId, int columnId)
        {
            if (tableId < 0 || tableId >= _nodeCount || _nodeTypes[tableId] != NodeType.Table)
                throw new ArgumentException("Invalid table ID");

            if (columnId < 0 || columnId >= _nodeCount || _nodeTypes[columnId] != NodeType.Column)
                throw new ArgumentException("Invalid column ID");

            int lockIndex = GetPartitionIndex(_nodeNames[tableId]);
            _nodeLocks[lockIndex].EnterWriteLock();

            try
            {
                // Get current columns and add the new one
                var currentColumns = _tableColumns[tableId];

                // Check if column is already in the table
                foreach (var id in currentColumns)
                {
                    if (id == columnId) return;
                }

                // Create new array with additional column
                var newColumns = new int[currentColumns.Length + 1];
                Array.Copy(currentColumns, newColumns, currentColumns.Length);
                newColumns[currentColumns.Length] = columnId;

                // Update table columns
                _tableColumns[tableId] = newColumns;
            }
            finally
            {
                _nodeLocks[lockIndex].ExitWriteLock();
            }
        }

        /// <summary>
        /// Adds an edge between two nodes
        /// </summary>
        public int AddEdge(int sourceId, int targetId, EdgeType type, string operation, string sqlExpression = "")
        {
            if (sourceId < 0 || sourceId >= _nodeCount)
                throw new ArgumentException("Invalid source node ID");

            if (targetId < 0 || targetId >= _nodeCount)
                throw new ArgumentException("Invalid target node ID");

            operation = _stringPool.Intern(operation);
            // Don't intern SQL expression as it can be large and unique

            int lockIndex = GetPartitionIndex(sourceId + targetId);
            _edgeLocks[lockIndex].EnterWriteLock();

            try
            {
                // Check if edge already exists
                foreach (var existingEdgeId in _outgoingEdges[sourceId])
                {
                    if (_edgeTargetIds[existingEdgeId] == targetId && _edgeTypes[existingEdgeId] == type)
                    {
                        return existingEdgeId;
                    }
                }

                EnsureEdgeCapacity();

                int edgeId = _edgeCount++;

                // Set edge properties
                _edgeIds[edgeId] = edgeId;
                _edgeSourceIds[edgeId] = sourceId;
                _edgeTargetIds[edgeId] = targetId;
                _edgeTypes[edgeId] = type;
                _edgeOperations[edgeId] = operation;
                _edgeSqlExpressions[edgeId] = sqlExpression;

                // Update adjacency lists
                _outgoingEdges[sourceId].Add(edgeId);
                _incomingEdges[targetId].Add(edgeId);

                return edgeId;
            }
            finally
            {
                _edgeLocks[lockIndex].ExitWriteLock();
            }
        }

        /// <summary>
        /// Gets a column node by table and column name
        /// </summary>
        public int GetColumnNode(string tableName, string columnName)
        {
            tableName = _stringPool.Intern(tableName);
            columnName = _stringPool.Intern(columnName);

            int lockIndex = GetPartitionIndex(columnName);
            _nodeLocks[lockIndex].EnterReadLock();

            try
            {
                if (_columnLookup.TryGetValue((tableName, columnName), out int nodeId))
                {
                    return nodeId;
                }

                return -1;
            }
            finally
            {
                _nodeLocks[lockIndex].ExitReadLock();
            }
        }

        /// <summary>
        /// Gets all column nodes for a table
        /// </summary>
        public int[] GetTableColumns(int tableId)
        {
            if (tableId < 0 || tableId >= _nodeCount || _nodeTypes[tableId] != NodeType.Table)
                return [];

            int lockIndex = GetPartitionIndex(_nodeNames[tableId]);
            _nodeLocks[lockIndex].EnterReadLock();

            try
            {
                // Return a copy of the columns array
                var columns = _tableColumns[tableId];
                var result = new int[columns.Length];
                Array.Copy(columns, result, columns.Length);
                return result;
            }
            finally
            {
                _nodeLocks[lockIndex].ExitReadLock();
            }
        }

        /// <summary>
        /// Gets all outgoing edges for a node
        /// </summary>
        public int[] GetOutgoingEdges(int nodeId)
        {
            if (nodeId < 0 || nodeId >= _nodeCount)
                return [];

            int lockIndex = GetPartitionIndex(nodeId);
            _nodeLocks[lockIndex].EnterReadLock();

            try
            {
                var edges = _outgoingEdges[nodeId];
                var result = new int[edges.Count];
                edges.CopyTo(result);
                return result;
            }
            finally
            {
                _nodeLocks[lockIndex].ExitReadLock();
            }
        }

        /// <summary>
        /// Gets all incoming edges for a node
        /// </summary>
        public int[] GetIncomingEdges(int nodeId)
        {
            if (nodeId < 0 || nodeId >= _nodeCount)
                return [];

            int lockIndex = GetPartitionIndex(nodeId);
            _nodeLocks[lockIndex].EnterReadLock();

            try
            {
                var edges = _incomingEdges[nodeId];
                var result = new int[edges.Count];
                edges.CopyTo(result);
                return result;
            }
            finally
            {
                _nodeLocks[lockIndex].ExitReadLock();
            }
        }

        /// <summary>
        /// Gets node data by id
        /// </summary>
        public NodeData GetNodeData(int nodeId)
        {
            if (nodeId < 0 || nodeId >= _nodeCount)
                throw new ArgumentException("Invalid node ID");

            int lockIndex = GetPartitionIndex(nodeId);
            _nodeLocks[lockIndex].EnterReadLock();

            try
            {
                var nodeType = _nodeTypes[nodeId];
                var result = new NodeData
                {
                    Id = nodeId,
                    Type = nodeType,
                    Name = _nodeNames[nodeId],
                    ObjectName = _nodeObjectNames[nodeId],
                    SchemaName = _nodeSchemaNames[nodeId],
                    DatabaseName = _nodeDatabaseNames[nodeId]
                };

                // Copy metadata
                foreach (var kvp in _nodeMetadata[nodeId])
                {
                    result.Metadata[kvp.Key] = kvp.Value;
                }

                // Add type-specific data
                switch (nodeType)
                {
                    case NodeType.Column:
                        result.ColumnData = new ColumnData
                        {
                            DataType = _columnDataTypes[nodeId],
                            TableOwner = _columnTableOwners[nodeId],
                            IsNullable = _columnIsNullable[nodeId],
                            IsComputed = _columnIsComputed[nodeId]
                        };
                        break;

                    case NodeType.Table:
                        result.TableData = new TableData
                        {
                            TableType = _tableTableTypes[nodeId],
                            Alias = _tableAliases[nodeId],
                            Definition = _tableDefinitions[nodeId],
                            ColumnIds = GetTableColumns(nodeId)
                        };
                        break;

                    case NodeType.Expression:
                        result.ExpressionData = new ExpressionData
                        {
                            ExpressionType = _expressionExpressionTypes[nodeId],
                            Expression = _expressionExpressions[nodeId],
                            ResultType = _expressionResultTypes[nodeId],
                            TableOwner = _expressionTableOwners[nodeId]
                        };
                        break;
                }

                return result;
            }
            finally
            {
                _nodeLocks[lockIndex].ExitReadLock();
            }
        }

        /// <summary>
        /// Gets edge data by id
        /// </summary>
        public EdgeData GetEdgeData(int edgeId)
        {
            if (edgeId < 0 || edgeId >= _edgeCount)
                throw new ArgumentException("Invalid edge ID");

            int lockIndex = GetPartitionIndex(edgeId);
            _edgeLocks[lockIndex].EnterReadLock();

            try
            {
                return new EdgeData
                {
                    Id = edgeId,
                    SourceId = _edgeSourceIds[edgeId],
                    TargetId = _edgeTargetIds[edgeId],
                    Type = _edgeTypes[edgeId],
                    Operation = _edgeOperations[edgeId],
                    SqlExpression = _edgeSqlExpressions[edgeId]
                };
            }
            finally
            {
                _edgeLocks[lockIndex].ExitReadLock();
            }
        }

        /// <summary>
        /// Gets all paths between two nodes using breadth-first search
        /// </summary>
        public List<List<int>> FindPaths(int sourceId, int targetId, int maxDepth = 10)
        {
            if (sourceId < 0 || sourceId >= _nodeCount)
                throw new ArgumentException("Invalid source node ID");

            if (targetId < 0 || targetId >= _nodeCount)
                throw new ArgumentException("Invalid target node ID");

            var result = new List<List<int>>();

            if (sourceId == targetId)
            {
                result.Add([]);
                return result;
            }

            // Use breadth-first search for path finding
            var queue = new Queue<PathState>();
            var visited = new HashSet<int>();

            queue.Enqueue(new PathState { NodeId = sourceId, Path = [] });
            visited.Add(sourceId);

            while (queue.Count > 0)
            {
                var state = queue.Dequeue();

                // Check if we've reached the target
                if (state.NodeId == targetId)
                {
                    result.Add(new List<int>(state.Path));
                    continue;
                }

                // Skip if we've reached maximum depth
                if (state.Path.Count >= maxDepth)
                    continue;

                // Get outgoing edges
                var outEdges = GetOutgoingEdges(state.NodeId);

                foreach (var edgeId in outEdges)
                {
                    int nextNodeId = _edgeTargetIds[edgeId];

                    // Skip visited nodes to prevent cycles
                    if (visited.Contains(nextNodeId))
                        continue;

                    // Create new path
                    var newPath = new List<int>(state.Path)
                    {
                        edgeId
                    };

                    // Add to queue and mark as visited
                    queue.Enqueue(new PathState { NodeId = nextNodeId, Path = newPath });
                    visited.Add(nextNodeId);
                }
            }

            return result;
        }

        /// <summary>
        /// Optimizes the graph by compacting arrays and removing unused space
        /// </summary>
        public void Compact()
        {
            // Take write locks on all partitions
            for (int i = 0; i < LockPartitions; i++)
            {
                _nodeLocks[i].EnterWriteLock();
                _edgeLocks[i].EnterWriteLock();
            }

            try
            {
                // Resize arrays to actual size
                if (_nodeCount < _nodeCapacity)
                {
                    _nodeCapacity = _nodeCount;

                    Array.Resize(ref _nodeIds, _nodeCapacity);
                    Array.Resize(ref _nodeTypes, _nodeCapacity);
                    Array.Resize(ref _nodeNames, _nodeCapacity);
                    Array.Resize(ref _nodeObjectNames, _nodeCapacity);
                    Array.Resize(ref _nodeSchemaNames, _nodeCapacity);
                    Array.Resize(ref _nodeDatabaseNames, _nodeCapacity);
                    Array.Resize(ref _nodeMetadata, _nodeCapacity);

                    Array.Resize(ref _columnDataTypes, _nodeCapacity);
                    Array.Resize(ref _columnTableOwners, _nodeCapacity);
                    Array.Resize(ref _columnIsNullable, _nodeCapacity);
                    Array.Resize(ref _columnIsComputed, _nodeCapacity);

                    Array.Resize(ref _tableTableTypes, _nodeCapacity);
                    Array.Resize(ref _tableAliases, _nodeCapacity);
                    Array.Resize(ref _tableDefinitions, _nodeCapacity);
                    Array.Resize(ref _tableColumns, _nodeCapacity);

                    Array.Resize(ref _expressionExpressionTypes, _nodeCapacity);
                    Array.Resize(ref _expressionExpressions, _nodeCapacity);
                    Array.Resize(ref _expressionResultTypes, _nodeCapacity);
                    Array.Resize(ref _expressionTableOwners, _nodeCapacity);

                    Array.Resize(ref _outgoingEdges, _nodeCapacity);
                    Array.Resize(ref _incomingEdges, _nodeCapacity);
                }

                if (_edgeCount < _edgeCapacity)
                {
                    _edgeCapacity = _edgeCount;

                    Array.Resize(ref _edgeIds, _edgeCapacity);
                    Array.Resize(ref _edgeSourceIds, _edgeCapacity);
                    Array.Resize(ref _edgeTargetIds, _edgeCapacity);
                    Array.Resize(ref _edgeTypes, _edgeCapacity);
                    Array.Resize(ref _edgeOperations, _edgeCapacity);
                    Array.Resize(ref _edgeSqlExpressions, _edgeCapacity);
                }
            }
            finally
            {
                // Release all locks
                for (int i = LockPartitions - 1; i >= 0; i--)
                {
                    _edgeLocks[i].ExitWriteLock();
                    _nodeLocks[i].ExitWriteLock();
                }
            }
        }

        /// <summary>
        /// Gets partition index for locking based on hash of key
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetPartitionIndex(string key)
        {
            return (key?.GetHashCode() ?? 0) & LockPartitions - 1;
        }

        /// <summary>
        /// Gets partition index for locking based on integer key
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetPartitionIndex(int key)
        {
            return key & LockPartitions - 1;
        }

        /// <summary>
        /// Gets node statistics
        /// </summary>
        public (int TotalNodes, int ColumnNodes, int TableNodes, int ExpressionNodes) GetNodeStatistics()
        {
            int columnCount = 0;
            int tableCount = 0;
            int expressionCount = 0;

            for (int i = 0; i < _nodeCount; i++)
            {
                switch (_nodeTypes[i])
                {
                    case NodeType.Column:
                        columnCount++;
                        break;
                    case NodeType.Table:
                        tableCount++;
                        break;
                    case NodeType.Expression:
                        expressionCount++;
                        break;
                }
            }

            return (_nodeCount, columnCount, tableCount, expressionCount);
        }

        /// <summary>
        /// Gets edge statistics
        /// </summary>
        public (int TotalEdges, int DirectEdges, int IndirectEdges, int JoinEdges) GetEdgeStatistics()
        {
            int directCount = 0;
            int indirectCount = 0;
            int joinCount = 0;

            for (int i = 0; i < _edgeCount; i++)
            {
                switch (_edgeTypes[i])
                {
                    case EdgeType.Direct:
                        directCount++;
                        break;
                    case EdgeType.Indirect:
                        indirectCount++;
                        break;
                    case EdgeType.Join:
                        joinCount++;
                        break;
                }
            }

            return (_edgeCount, directCount, indirectCount, joinCount);
        }

        /// <summary>
        /// String pool for memory optimization
        /// </summary>
        private class StringPool
        {
            private readonly Dictionary<string, string> _pool = new(StringComparer.Ordinal);
            private readonly ReaderWriterLockSlim _lock = new();

            public string Intern(string str)
            {
                if (string.IsNullOrEmpty(str))
                    return str;

                _lock.EnterUpgradeableReadLock();
                try
                {
                    if (_pool.TryGetValue(str, out var internedStr))
                    {
                        return internedStr;
                    }

                    _lock.EnterWriteLock();
                    try
                    {
                        _pool[str] = str;
                        return str;
                    }
                    finally
                    {
                        _lock.ExitWriteLock();
                    }
                }
                finally
                {
                    _lock.ExitUpgradeableReadLock();
                }
            }
        }

        /// <summary>
        /// Helper class for tracking path state during BFS
        /// </summary>
        private class PathState
        {
            public int NodeId { get; set; }
            public List<int> Path { get; set; } = [];
        }
    }

    /// <summary>
    /// Node types
    /// </summary>
    public enum NodeType
    {
        Column,
        Table,
        Expression
    }

    /// <summary>
    /// Edge types
    /// </summary>
    public enum EdgeType
    {
        Direct,      // Direct column to column
        Indirect,    // Via expression
        Join,        // Join relationship
        GroupBy,     // Grouping relationship
        Filter,      // Filter relationship
        Parameter    // Parameter relationship
    }

    /// <summary>
    /// Node data structure
    /// </summary>
    public class NodeData
    {
        public int Id { get; set; }
        public NodeType Type { get; set; }
        public string Name { get; set; } = string.Empty;
        public string ObjectName { get; set; } = string.Empty;
        public string SchemaName { get; set; } = string.Empty;
        public string DatabaseName { get; set; } = string.Empty;
        public Dictionary<string, object> Metadata { get; set; } = [];

        public ColumnData? ColumnData { get; set; }
        public TableData? TableData { get; set; }
        public ExpressionData? ExpressionData { get; set; }
    }

    /// <summary>
    /// Column-specific data
    /// </summary>
    public class ColumnData
    {
        public string DataType { get; set; } = string.Empty;
        public string TableOwner { get; set; } = string.Empty;
        public bool IsNullable { get; set; }
        public bool IsComputed { get; set; }
    }

    /// <summary>
    /// Table-specific data
    /// </summary>
    public class TableData
    {
        public string TableType { get; set; } = string.Empty;
        public string Alias { get; set; } = string.Empty;
        public string Definition { get; set; } = string.Empty;
        public int[] ColumnIds { get; set; } = [];
    }

    /// <summary>
    /// Expression-specific data
    /// </summary>
    public class ExpressionData
    {
        public string ExpressionType { get; set; } = string.Empty;
        public string Expression { get; set; } = string.Empty;
        public string ResultType { get; set; } = string.Empty;
        public string TableOwner { get; set; } = string.Empty;
    }

    /// <summary>
    /// Edge data structure
    /// </summary>
    public class EdgeData
    {
        public int Id { get; set; }
        public int SourceId { get; set; }
        public int TargetId { get; set; }
        public EdgeType Type { get; set; }
        public string Operation { get; set; } = string.Empty;
        public string SqlExpression { get; set; } = string.Empty;
    }
}
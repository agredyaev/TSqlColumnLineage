using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace TSqlColumnLineage.Core.Infrastructure.Memory
{
    /// <summary>
    /// Provides memory optimization strategies for the lineage tracking system
    /// Implements object pooling and SoA pattern support
    /// </summary>
    public sealed class MemoryManager
    {
        // Singleton instance
        private static readonly Lazy<MemoryManager> _instance =
            new(() => new MemoryManager());

        // Thread-safe string pool
        private readonly ConcurrentDictionary<string, string> _stringPool =
            new(StringComparer.Ordinal);

        // Various object pools for reuse
        private readonly ConcurrentDictionary<Type, ObjectPool> _objectPools =
            new();

        // Array pools for commonly used array types
        private readonly ConcurrentDictionary<Type, ArrayPool> _arrayPools =
            new();

        // Lock for stats updates
        private readonly ReaderWriterLockSlim _statsLock = new();

        // Memory stats
        private long _pooledStringsCount;
        private long _pooledStringsSizeBytes;
        private long _pooledObjectsCount;
        private long _pooledArraysCount;

        /// <summary>
        /// Gets the singleton instance
        /// </summary>
        public static MemoryManager Instance => _instance.Value;

        /// <summary>
        /// Interns a string to reduce memory usage
        /// </summary>
        public string InternString(string str)
        {
            if (string.IsNullOrEmpty(str))
                return str;

            string result = _stringPool.GetOrAdd(str, str);

            // Update stats if we added a new string
            if (ReferenceEquals(result, str))
            {
                _statsLock.EnterWriteLock();
                try
                {
                    _pooledStringsCount++;
                    _pooledStringsSizeBytes += str.Length * sizeof(char);
                }
                finally
                {
                    _statsLock.ExitWriteLock();
                }
            }

            return result;
        }

        /// <summary>
        /// Gets or creates an object pool for the specified type
        /// </summary>
        public ObjectPool<T> GetOrCreateObjectPool<T>(Func<T> factory, Action<T>? reset = null, int initialCapacity = 128)
            where T : class
        {
            var type = typeof(T);

            return (ObjectPool<T>)_objectPools.GetOrAdd(
                type,
                _ => new ObjectPool<T>(factory, reset, initialCapacity));
        }

        /// <summary>
        /// Gets or creates an array pool for the specified type
        /// </summary>
        public ArrayPool<T> GetOrCreateArrayPool<T>(int maxArrayLength = 1024, int maxArraysPerBucket = 50)
        {
            var type = typeof(T);

            return (ArrayPool<T>)_arrayPools.GetOrAdd(
                type,
                _ => new ArrayPool<T>(maxArrayLength, maxArraysPerBucket));
        }

        /// <summary>
        /// Allocates a block of memory of the specified size
        /// </summary>
        public T[] AllocateArray<T>(int length)
        {
            if (length <= 0)
                return [];

            // For small arrays, get from pool
            if (length <= 1024)
            {
                var pool = GetOrCreateArrayPool<T>();
                var array = pool.Rent(length);

                _statsLock.EnterWriteLock();
                try
                {
                    _pooledArraysCount++;
                }
                finally
                {
                    _statsLock.ExitWriteLock();
                }

                return array;
            }

            // For large arrays, allocate directly
            return new T[length];
        }

        /// <summary>
        /// Returns an array to the pool
        /// </summary>
        public void ReturnArray<T>(T[] array)
        {
            if (array == null || array.Length == 0 || array.Length > 1024)
                return;

            var pool = GetOrCreateArrayPool<T>();
            pool.Return(array);

            _statsLock.EnterWriteLock();
            try
            {
                _pooledArraysCount--;
            }
            finally
            {
                _statsLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Gets memory statistics
        /// </summary>
        public MemoryStatistics GetStatistics()
        {
            _statsLock.EnterReadLock();
            try
            {
                return new MemoryStatistics
                {
                    PooledStringsCount = _pooledStringsCount,
                    PooledStringsSizeBytes = _pooledStringsSizeBytes,
                    PooledObjectsCount = _pooledObjectsCount,
                    PooledArraysCount = _pooledArraysCount
                };
            }
            finally
            {
                _statsLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Base class for object pools
        /// </summary>
        public abstract class ObjectPool
        {
        }

        /// <summary>
        /// Pool of reusable objects
        /// </summary>
        public sealed class ObjectPool<T> : ObjectPool where T : class
        {
            private readonly ConcurrentBag<T> _objects;
            private readonly Func<T> _factory;
            private readonly Action<T> _reset;

            public ObjectPool(Func<T> factory, Action<T> reset, int initialCapacity)
            {
                _factory = factory ?? throw new ArgumentNullException(nameof(factory));
                _reset = reset;
                _objects = [];

                // Create initial objects
                for (int i = 0; i < initialCapacity; i++)
                {
                    _objects.Add(_factory());
                }
            }

            /// <summary>
            /// Gets an object from the pool
            /// </summary>
            public T Get()
            {
                if (_objects.TryTake(out var item))
                {
                    Instance._statsLock.EnterWriteLock();
                    try
                    {
                        Instance._pooledObjectsCount--;
                    }
                    finally
                    {
                        Instance._statsLock.ExitWriteLock();
                    }

                    return item;
                }

                return _factory();
            }

            /// <summary>
            /// Returns an object to the pool
            /// </summary>
            public void Return(T item)
            {
                if (item == null)
                    return;

                // Reset the object if needed
                _reset?.Invoke(item);

                // Add to pool
                _objects.Add(item);

                Instance._statsLock.EnterWriteLock();
                try
                {
                    Instance._pooledObjectsCount++;
                }
                finally
                {
                    Instance._statsLock.ExitWriteLock();
                }
            }

            internal void Return(TSql160Parser sql150Parser)
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Base class for array pools
        /// </summary>
        public abstract class ArrayPool
        {
        }

        /// <summary>
        /// Pool of reusable arrays
        /// </summary>
        public sealed class ArrayPool<T> : ArrayPool
        {
            private readonly T[][] _buckets;
            private readonly int[] _bucketArrayLength;
            private readonly ConcurrentStack<T[]>[] _arrays;

            public ArrayPool(int maxArrayLength, int maxArraysPerBucket)
            {
                // Determine bucket sizes (powers of 2)
                int bucketsCount = 0;
                int size = 1;

                while (size <= maxArrayLength)
                {
                    size *= 2;
                    bucketsCount++;
                }

                _buckets = new T[bucketsCount][];
                _bucketArrayLength = new int[bucketsCount];
                _arrays = new ConcurrentStack<T[]>[bucketsCount];

                size = 1;
                for (int i = 0; i < bucketsCount; i++)
                {
                    size *= 2;
                    _bucketArrayLength[i] = size;
                    _arrays[i] = new ConcurrentStack<T[]>();
                }
            }

            /// <summary>
            /// Rents an array of at least the specified length
            /// </summary>
            public T[] Rent(int minimumLength)
            {
                if (minimumLength <= 0)
                    return [];

                // Find appropriate bucket
                int bucketIndex = GetBucketIndex(minimumLength);

                if (bucketIndex < 0 || bucketIndex >= _arrays.Length)
                {
                    // Too large for pool
                    return new T[minimumLength];
                }

                // Try to get from pool
                var arrays = _arrays[bucketIndex];
                if (arrays.TryPop(out var result))
                {
                    // Clear the array
                    Array.Clear(result, 0, result.Length);
                    return result;
                }

                // Create new array of bucket size
                return new T[_bucketArrayLength[bucketIndex]];
            }

            /// <summary>
            /// Returns an array to the pool
            /// </summary>
            public void Return(T[] array)
            {
                if (array == null || array.Length == 0)
                    return;

                // Find appropriate bucket
                int bucketIndex = GetBucketIndex(array.Length);

                if (bucketIndex < 0 || bucketIndex >= _arrays.Length)
                {
                    // Too large for pool
                    return;
                }

                // Only pool arrays of exact bucket size
                if (array.Length != _bucketArrayLength[bucketIndex])
                {
                    return;
                }

                // Add to pool
                _arrays[bucketIndex].Push(array);
            }

            /// <summary>
            /// Gets the bucket index for the specified length
            /// </summary>
            private int GetBucketIndex(int length)
            {
                int index = 0;
                int size = 1;

                while (size < length && index < _bucketArrayLength.Length)
                {
                    size *= 2;
                    index++;
                }

                return index;
            }
        }
    }

    /// <summary>
    /// Memory statistics
    /// </summary>
    public class MemoryStatistics
    {
        public long PooledStringsCount { get; set; }
        public long PooledStringsSizeBytes { get; set; }
        public long PooledObjectsCount { get; set; }
        public long PooledArraysCount { get; set; }

        public override string ToString()
        {
            return $"Pooled strings: {PooledStringsCount} ({PooledStringsSizeBytes / 1024} KB), " +
                   $"Pooled objects: {PooledObjectsCount}, " +
                   $"Pooled arrays: {PooledArraysCount}";
        }
    }
}
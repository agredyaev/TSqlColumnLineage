using System;
using System.Collections.Concurrent;

namespace TSqlColumnLineage.Core.Common.Utils
{
    /// <summary>
    /// Generic object pool to reduce garbage collection pressure by reusing objects.
    /// </summary>
    /// <typeparam name="T">Type of objects to pool</typeparam>
    public sealed class ObjectPool<T> where T : class
    {
        private readonly ConcurrentBag<T> _objects;
        private readonly Func<T> _objectGenerator;
        private readonly Action<T> _objectReset;
        private readonly int _maxObjects;
        
        // Statistics
        private int _objectsCreated;
        private int _objectsReused;
        
        /// <summary>
        /// Gets the number of objects currently in the pool
        /// </summary>
        public int Count => _objects.Count;
        
        /// <summary>
        /// Gets the total number of objects created by this pool
        /// </summary>
        public int ObjectsCreated => _objectsCreated;
        
        /// <summary>
        /// Gets the number of times objects were reused from the pool
        /// </summary>
        public int ObjectsReused => _objectsReused;

        /// <summary>
        /// Initializes a new instance of the object pool
        /// </summary>
        /// <param name="objectGenerator">Factory function to create new objects</param>
        /// <param name="objectReset">Optional function to reset objects before reuse</param>
        /// <param name="initialCount">Initial number of objects to create</param>
        /// <param name="maxObjects">Maximum number of objects to keep in the pool</param>
        public ObjectPool(Func<T> objectGenerator, Action<T> objectReset = null, int initialCount = 0, int maxObjects = 1000)
        {
            _objectGenerator = objectGenerator ?? throw new ArgumentNullException(nameof(objectGenerator));
            _objectReset = objectReset;
            _maxObjects = maxObjects;
            _objects = new ConcurrentBag<T>();
            
            // Pre-populate the pool
            for (int i = 0; i < initialCount; i++)
            {
                _objects.Add(objectGenerator());
                _objectsCreated++;
            }
        }

        /// <summary>
        /// Gets an object from the pool or creates a new one if needed
        /// </summary>
        /// <returns>An instance of T</returns>
        public T Get()
        {
            if (_objects.TryTake(out T item))
            {
                _objectsReused++;
                _objectReset?.Invoke(item);
                return item;
            }
            
            _objectsCreated++;
            return _objectGenerator();
        }

        /// <summary>
        /// Returns an object to the pool
        /// </summary>
        /// <param name="item">The object to return</param>
        public void Return(T item)
        {
            if (item == null) 
                throw new ArgumentNullException(nameof(item));
                
            // Only add to the pool if we're under the maximum size
            if (_objects.Count < _maxObjects)
            {
                _objects.Add(item);
            }
        }
        
        /// <summary>
        /// Clears the pool, allowing all objects to be garbage collected
        /// </summary>
        public void Clear()
        {
            while (_objects.TryTake(out _)) { }
        }
        
        /// <summary>
        /// Gets statistics about the object pool usage
        /// </summary>
        /// <returns>A string with usage information</returns>
        public string GetStatistics()
        {
            return $"Object Pool: {_objects.Count} objects available, {_objectsCreated} created, {_objectsReused} reused";
        }
    }
}
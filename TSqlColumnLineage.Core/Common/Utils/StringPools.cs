using System;
using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Common.Utils
{
    /// <summary>
    /// Provides efficient string pooling to reduce memory consumption by reusing identical strings.
    /// </summary>
    public sealed class StringPool
    {
        private readonly Dictionary<string, string> _pool = new Dictionary<string, string>(StringComparer.Ordinal);
        private readonly object _lock = new object();
        
        // Statistics for monitoring
        private int _internedCount;
        private int _totalStringCount;
        private int _bytesInPool;
        private int _bytesSaved;
        
        /// <summary>
        /// Gets the number of strings that have been interned and reused
        /// </summary>
        public int InternedCount => _internedCount;
        
        /// <summary>
        /// Gets the total number of strings processed
        /// </summary>
        public int TotalStringCount => _totalStringCount;
        
        /// <summary>
        /// Gets the total bytes used by strings in the pool
        /// </summary>
        public int BytesInPool => _bytesInPool;
        
        /// <summary>
        /// Gets an estimate of bytes saved through string pooling
        /// </summary>
        public int BytesSaved => _bytesSaved;

        /// <summary>
        /// Interns a string, returning an existing instance if the string already exists in the pool.
        /// </summary>
        /// <param name="str">The string to intern</param>
        /// <returns>The interned string instance, or the original if str is null or empty</returns>
        public string Intern(string str)
        {
            if (string.IsNullOrEmpty(str))
                return str;
                
            _totalStringCount++;
            
            lock (_lock)
            {
                if (_pool.TryGetValue(str, out var internedString))
                {
                    _internedCount++;
                    _bytesSaved += str.Length * sizeof(char);
                    return internedString;
                }
                
                _pool[str] = str;
                _bytesInPool += str.Length * sizeof(char);
                return str;
            }
        }
        
        /// <summary>
        /// Clears the string pool to free memory
        /// </summary>
        public void Clear()
        {
            lock (_lock)
            {
                _pool.Clear();
                _bytesInPool = 0;
            }
        }
        
        /// <summary>
        /// Gets statistics about the string pool usage
        /// </summary>
        /// <returns>A string with usage information</returns>
        public string GetStatistics()
        {
            return $"String Pool: {_pool.Count} unique strings, {_bytesInPool / 1024} KB used, " +
                   $"{_bytesSaved / 1024} KB saved, {_internedCount}/{_totalStringCount} strings interned";
        }
    }
}
using System;
using System.Threading;

namespace TSqlColumnLineage.Core.Common.Utils
{
    /// <summary>
    /// Provides centralized ID generation facilities with various strategies
    /// </summary>
    public sealed class IdGenerator
    {
        private int _sequentialCounter;
        private readonly StringPool _stringPool;
        
        /// <summary>
        /// Initializes a new instance of the ID generator
        /// </summary>
        /// <param name="stringPool">String pool for interning generated IDs</param>
        public IdGenerator(StringPool stringPool)
        {
            _stringPool = stringPool ?? throw new ArgumentNullException(nameof(stringPool));
        }

        /// <summary>
        /// Creates a new sequential ID with the given prefix
        /// </summary>
        /// <param name="prefix">Prefix for the ID</param>
        /// <returns>A unique ID string</returns>
        public string CreateSequentialId(string prefix)
        {
            int id = Interlocked.Increment(ref _sequentialCounter);
            return _stringPool.Intern($"{prefix}_{id}");
        }
        
        /// <summary>
        /// Creates a new GUID-based ID with the given prefix
        /// </summary>
        /// <param name="prefix">Prefix for the ID</param>
        /// <returns>A unique ID string</returns>
        public string CreateGuidId(string prefix)
        {
            return _stringPool.Intern($"{prefix}_{Guid.NewGuid():N}");
        }
        
        /// <summary>
        /// Creates a new ID for a node with the specified type and name
        /// </summary>
        /// <param name="type">Node type (e.g., "TABLE", "COLUMN")</param>
        /// <param name="name">Node name</param>
        /// <returns>A unique ID string</returns>
        public string CreateNodeId(string type, string name)
        {
            var sanitizedName = SanitizeName(name);
            return _stringPool.Intern($"{type}_{Guid.NewGuid():N}_{sanitizedName}");
        }
        
        /// <summary>
        /// Creates a hashed ID based on components to ensure consistent IDs for the same input
        /// </summary>
        /// <param name="components">Components to include in the hash</param>
        /// <returns>A deterministic ID string</returns>
        public string CreateHashedId(params string[] components)
        {
            string combined = string.Join("_", components);
            unchecked
            {
                // Simple, fast hash algorithm (FNV-1a)
                const uint fnvPrime = 16777619;
                const uint fnvOffsetBasis = 2166136261;
                
                uint hash = fnvOffsetBasis;
                foreach (char c in combined)
                {
                    hash = hash ^ c;
                    hash = hash * fnvPrime;
                }
                
                return _stringPool.Intern($"H{hash:X8}");
            }
        }
        
        /// <summary>
        /// Sanitizes a name for use in an ID by replacing invalid characters
        /// </summary>
        /// <param name="name">Name to sanitize</param>
        /// <returns>Sanitized name string</returns>
        private string SanitizeName(string name)
        {
            if (string.IsNullOrEmpty(name))
                return "unnamed";
                
            // Replace common problematic characters
            return name.Replace(".", "_")
                       .Replace(" ", "_")
                       .Replace("(", "")
                       .Replace(")", "")
                       .Replace("[", "")
                       .Replace("]", "")
                       .Replace("'", "")
                       .Replace("\"", "")
                       .Replace(",", "_");
        }
        
        /// <summary>
        /// Resets the sequential counter (primarily for testing)
        /// </summary>
        public void ResetSequentialCounter()
        {
            Interlocked.Exchange(ref _sequentialCounter, 0);
        }
    }
}
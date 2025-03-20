using System;
using System.Collections.Generic;
using System.Threading;
using TSqlColumnLineage.Core.Infrastructure.Concurency;
using TSqlColumnLineage.Core.Infrastructure.Memory;

namespace TSqlColumnLineage.Core.Infrastructure.Configuration
{
    /// <summary>
    /// Configuration settings for the infrastructure layer
    /// </summary>
    public sealed class InfrastructureConfig
    {
        // Singleton instance
        private static readonly Lazy<InfrastructureConfig> _instance =
            new(() => new InfrastructureConfig());

        // Lock for config updates
        private readonly ReaderWriterLockSlim _configLock = new();

        // Configuration properties with defaults
        private int _maxConcurrency = Environment.ProcessorCount;
        private int _parserPoolSize = 10;
        private int _batchSize = 1000;
        private int _lockPartitions = 32;
        private int _maxGraphCapacity = 100000;
        private bool _enableMemoryMonitoring = true;
        private bool _enablePerformanceTracking = true;
        private TimeSpan _memoryMonitoringInterval = TimeSpan.FromSeconds(10);
        private long _memoryThresholdBytes = 1024L * 1024L * 1024L; // 1 GB
        private bool _adaptiveConcurrency = true;
        private readonly Dictionary<string, string> _customSettings = [];

        /// <summary>
        /// Gets the singleton instance
        /// </summary>
        public static InfrastructureConfig Instance => _instance.Value;

        /// <summary>
        /// Gets or sets the maximum concurrency level
        /// </summary>
        public int MaxConcurrency
        {
            get
            {
                _configLock.EnterReadLock();
                try { return _maxConcurrency; }
                finally { _configLock.ExitReadLock(); }
            }
            set
            {
                _configLock.EnterWriteLock();
                try { _maxConcurrency = Math.Max(1, value); }
                finally { _configLock.ExitWriteLock(); }
            }
        }

        /// <summary>
        /// Gets or sets the SQL parser pool size
        /// </summary>
        public int ParserPoolSize
        {
            get
            {
                _configLock.EnterReadLock();
                try { return _parserPoolSize; }
                finally { _configLock.ExitReadLock(); }
            }
            set
            {
                _configLock.EnterWriteLock();
                try { _parserPoolSize = Math.Max(1, value); }
                finally { _configLock.ExitWriteLock(); }
            }
        }

        /// <summary>
        /// Gets or sets the default batch size
        /// </summary>
        public int BatchSize
        {
            get
            {
                _configLock.EnterReadLock();
                try { return _batchSize; }
                finally { _configLock.ExitReadLock(); }
            }
            set
            {
                _configLock.EnterWriteLock();
                try { _batchSize = Math.Max(1, value); }
                finally { _configLock.ExitWriteLock(); }
            }
        }

        /// <summary>
        /// Gets or sets the number of lock partitions
        /// </summary>
        public int LockPartitions
        {
            get
            {
                _configLock.EnterReadLock();
                try { return _lockPartitions; }
                finally { _configLock.ExitReadLock(); }
            }
            set
            {
                _configLock.EnterWriteLock();
                try { _lockPartitions = Math.Max(1, value); }
                finally { _configLock.ExitWriteLock(); }
            }
        }

        /// <summary>
        /// Gets or sets the maximum graph capacity
        /// </summary>
        public int MaxGraphCapacity
        {
            get
            {
                _configLock.EnterReadLock();
                try { return _maxGraphCapacity; }
                finally { _configLock.ExitReadLock(); }
            }
            set
            {
                _configLock.EnterWriteLock();
                try { _maxGraphCapacity = Math.Max(1000, value); }
                finally { _configLock.ExitWriteLock(); }
            }
        }

        /// <summary>
        /// Gets or sets whether memory monitoring is enabled
        /// </summary>
        public bool EnableMemoryMonitoring
        {
            get
            {
                _configLock.EnterReadLock();
                try { return _enableMemoryMonitoring; }
                finally { _configLock.ExitReadLock(); }
            }
            set
            {
                _configLock.EnterWriteLock();
                try { _enableMemoryMonitoring = value; }
                finally { _configLock.ExitWriteLock(); }
            }
        }

        /// <summary>
        /// Gets or sets whether performance tracking is enabled
        /// </summary>
        public bool EnablePerformanceTracking
        {
            get
            {
                _configLock.EnterReadLock();
                try { return _enablePerformanceTracking; }
                finally { _configLock.ExitReadLock(); }
            }
            set
            {
                _configLock.EnterWriteLock();
                try { _enablePerformanceTracking = value; }
                finally { _configLock.ExitWriteLock(); }
            }
        }

        /// <summary>
        /// Gets or sets the memory monitoring interval
        /// </summary>
        public TimeSpan MemoryMonitoringInterval
        {
            get
            {
                _configLock.EnterReadLock();
                try { return _memoryMonitoringInterval; }
                finally { _configLock.ExitReadLock(); }
            }
            set
            {
                _configLock.EnterWriteLock();
                try
                {
                    _memoryMonitoringInterval = value.TotalMilliseconds < 100 ?
                        TimeSpan.FromMilliseconds(100) : value;
                }
                finally { _configLock.ExitWriteLock(); }
            }
        }

        /// <summary>
        /// Gets or sets the memory threshold in bytes
        /// </summary>
        public long MemoryThresholdBytes
        {
            get
            {
                _configLock.EnterReadLock();
                try { return _memoryThresholdBytes; }
                finally { _configLock.ExitReadLock(); }
            }
            set
            {
                _configLock.EnterWriteLock();
                try { _memoryThresholdBytes = Math.Max(1024 * 1024 * 100, value); }
                finally { _configLock.ExitWriteLock(); }
            }
        }

        /// <summary>
        /// Gets or sets whether adaptive concurrency is enabled
        /// </summary>
        public bool AdaptiveConcurrency
        {
            get
            {
                _configLock.EnterReadLock();
                try { return _adaptiveConcurrency; }
                finally { _configLock.ExitReadLock(); }
            }
            set
            {
                _configLock.EnterWriteLock();
                try { _adaptiveConcurrency = value; }
                finally { _configLock.ExitWriteLock(); }
            }
        }

        /// <summary>
        /// Gets a custom setting
        /// </summary>
        public string GetCustomSetting(string key, string defaultValue = "")
        {
            if (string.IsNullOrEmpty(key))
                return defaultValue;

            _configLock.EnterReadLock();
            try
            {
                if (_customSettings.TryGetValue(key, out var value))
                {
                    return value;
                }

                return defaultValue;
            }
            finally
            {
                _configLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Sets a custom setting
        /// </summary>
        public void SetCustomSetting(string key, string value)
        {
            if (string.IsNullOrEmpty(key))
                return;

            _configLock.EnterWriteLock();
            try
            {
                _customSettings[key] = value;
            }
            finally
            {
                _configLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Gets all custom settings
        /// </summary>
        public Dictionary<string, string> GetAllCustomSettings()
        {
            _configLock.EnterReadLock();
            try
            {
                return new Dictionary<string, string>(_customSettings);
            }
            finally
            {
                _configLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Applies the configuration to the infrastructure components
        /// </summary>
        public void ApplyConfiguration()
        {
            // Configure memory monitoring
            if (EnableMemoryMonitoring)
            {
                var monitor = MemoryPressureMonitor.Instance;
                monitor.MonitoringInterval = MemoryMonitoringInterval;

                if (!monitor.IsMonitoring)
                {
                    monitor.StartMonitoring();
                }
            }
            else
            {
                var monitor = MemoryPressureMonitor.Instance;
                if (monitor.IsMonitoring)
                {
                    monitor.StopMonitoring();
                }
            }

            // Configure batch operation manager
            var batchManager = BatchOperationManager.Instance;
            batchManager.DefaultBatchSize = BatchSize;
            batchManager.MaxConcurrentBatches = MaxConcurrency;
        }

        /// <summary>
        /// Optimizes the configuration for the current environment
        /// </summary>
        public void OptimizeForEnvironment()
        {
            // Get available processor count
            int processorCount = Environment.ProcessorCount;

            // Configure based on processor count
            MaxConcurrency = processorCount;
            LockPartitions = NextPowerOfTwo(processorCount * 4);
            ParserPoolSize = Math.Max(2, processorCount / 2);

            // Configure based on available memory
            try
            {
                var gcMemoryInfo = GC.GetGCMemoryInfo();
                long totalAvailableMemoryBytes = gcMemoryInfo.TotalAvailableMemoryBytes;

                // Set memory threshold to 80% of available memory
                MemoryThresholdBytes = (long)(totalAvailableMemoryBytes * 0.8);

                // Adjust batch size based on available memory
                if (totalAvailableMemoryBytes < 4L * 1024L * 1024L * 1024L) // 4 GB
                {
                    BatchSize = 500;
                }
                else if (totalAvailableMemoryBytes < 8L * 1024L * 1024L * 1024L) // 8 GB
                {
                    BatchSize = 1000;
                }
                else
                {
                    BatchSize = 2000;
                }

                // Adjust graph capacity based on available memory
                if (totalAvailableMemoryBytes < 4L * 1024L * 1024L * 1024L) // 4 GB
                {
                    MaxGraphCapacity = 50000;
                }
                else if (totalAvailableMemoryBytes < 8L * 1024L * 1024L * 1024L) // 8 GB
                {
                    MaxGraphCapacity = 100000;
                }
                else
                {
                    MaxGraphCapacity = 200000;
                }
            }
            catch (Exception)
            {
                // Use default values if we can't get memory info
            }

            // Apply the configuration
            ApplyConfiguration();
        }

        /// <summary>
        /// Gets the next power of two
        /// </summary>
        private static int NextPowerOfTwo(int x)
        {
            x--;
            x |= x >> 1;
            x |= x >> 2;
            x |= x >> 4;
            x |= x >> 8;
            x |= x >> 16;
            x++;
            return x;
        }
    }
}
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TSqlColumnLineage.Core.Infrastructure.Concurency;
using TSqlColumnLineage.Core.Infrastructure.Configuration;
using TSqlColumnLineage.Core.Infrastructure.Memory;
using TSqlColumnLineage.Core.Infrastructure.Monitoring;

namespace TSqlColumnLineage.Core.Infrastructure
{
    /// <summary>
    /// Unified service providing access to all infrastructure components
    /// Handles initialization, configuration, and diagnostics
    /// </summary>
    public sealed class InfrastructureService : IDisposable
    {
        // Singleton instance
        private static readonly Lazy<InfrastructureService> _instance =
            new(() => new InfrastructureService());

        // Core infrastructure components
        private readonly MemoryPressureMonitor _memoryMonitor;
        private readonly MemoryManager _memoryManager;
        private readonly BatchOperationManager _batchManager;
        private readonly PerformanceTracker _performanceTracker;
        private readonly PartitionedLockManager _lockManager;
        private readonly InfrastructureConfig _configuration;

        // Service state
        private bool _initialized;
        private readonly object _initLock = new();
        private readonly CancellationTokenSource _shutdownTokenSource = new();
        private readonly List<IDisposable> _managedResources = [];

        /// <summary>
        /// Gets the singleton instance
        /// </summary>
        public static InfrastructureService Instance => _instance.Value;

        /// <summary>
        /// Gets the memory pressure monitor
        /// </summary>
        public MemoryPressureMonitor MemoryMonitor => _memoryMonitor;

        /// <summary>
        /// Gets the memory manager
        /// </summary>
        public MemoryManager MemoryManager => _memoryManager;

        /// <summary>
        /// Gets the batch operation manager
        /// </summary>
        public BatchOperationManager BatchManager => _batchManager;

        /// <summary>
        /// Gets the performance tracker
        /// </summary>
        public PerformanceTracker PerformanceTracker => _performanceTracker;

        /// <summary>
        /// Gets the partitioned lock manager
        /// </summary>
        public PartitionedLockManager LockManager => _lockManager;

        /// <summary>
        /// Gets the configuration manager
        /// </summary>
        public InfrastructureConfig Configuration => _configuration;

        /// <summary>
        /// Gets whether the service is initialized
        /// </summary>
        public bool IsInitialized => _initialized;

        /// <summary>
        /// Gets the cancellation token for shutdown
        /// </summary>
        public CancellationToken ShutdownToken => _shutdownTokenSource.Token;

        /// <summary>
        /// Creates a new infrastructure service
        /// </summary>
        private InfrastructureService()
        {
            // Initialize core components
            _configuration = InfrastructureConfig.Instance;
            _memoryMonitor = MemoryPressureMonitor.Instance;
            _memoryManager = MemoryManager.Instance;
            _batchManager = BatchOperationManager.Instance;
            _performanceTracker = PerformanceTracker.Instance;
            _lockManager = new PartitionedLockManager(_configuration.LockPartitions);

            // Track disposable resources
            _managedResources.Add(_memoryMonitor);
            _managedResources.Add(_shutdownTokenSource);
        }

        /// <summary>
        /// Initializes the infrastructure service
        /// </summary>
        public void Initialize(bool optimize = true)
        {
            if (_initialized)
                return;

            lock (_initLock)
            {
                if (_initialized)
                    return;

                try
                {
                    // Optimize configuration if requested
                    if (optimize)
                    {
                        _configuration.OptimizeForEnvironment();
                    }
                    else
                    {
                        _configuration.ApplyConfiguration();
                    }

                    // Register for memory pressure events
                    _memoryMonitor.MemoryPressureChanged += OnMemoryPressureChanged;

                    // Start monitoring if enabled
                    if (_configuration.EnableMemoryMonitoring && !_memoryMonitor.IsMonitoring)
                    {
                        _memoryMonitor.StartMonitoring();
                    }

                    _initialized = true;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Failed to initialize infrastructure service: {ex.Message}", ex);
                }
            }
        }

        /// <summary>
        /// Handles memory pressure changes
        /// </summary>
        private void OnMemoryPressureChanged(object sender, MemoryPressureEventArgs e)
        {
            // Track the event
            _performanceTracker.IncrementCounter("Memory", "PressureChanges");
            _performanceTracker.SetCounter("Memory", "CurrentPressureLevel", (int)e.NewLevel);

            // If pressure increased to high, try to reduce it
            if (e.NewLevel == MemoryPressureLevel.High && e.OldLevel != MemoryPressureLevel.High)
            {
                if (_configuration.AdaptiveConcurrency)
                {
                    // Reduce concurrency
                    int currentMax = _batchManager.MaxConcurrentBatches;
                    int newMax = Math.Max(1, currentMax / 2);
                    _batchManager.MaxConcurrentBatches = newMax;

                    // Track the adjustment
                    _performanceTracker.SetCounter("Concurrency", "MaxBatches", newMax);
                    _performanceTracker.IncrementCounter("Memory", "ConcurrencyReductions");
                }

                // Force garbage collection
                _memoryMonitor.TryReduceMemoryPressure();
                _performanceTracker.IncrementCounter("Memory", "ForcedGC");
            }
            else if (e.NewLevel == MemoryPressureLevel.Low && e.OldLevel != MemoryPressureLevel.Low)
            {
                if (_configuration.AdaptiveConcurrency)
                {
                    // Increase concurrency back toward configured maximum
                    int currentMax = _batchManager.MaxConcurrentBatches;
                    int configMax = _configuration.MaxConcurrency;

                    if (currentMax < configMax)
                    {
                        int newMax = Math.Min(configMax, currentMax * 2);
                        _batchManager.MaxConcurrentBatches = newMax;

                        // Track the adjustment
                        _performanceTracker.SetCounter("Concurrency", "MaxBatches", newMax);
                        _performanceTracker.IncrementCounter("Memory", "ConcurrencyIncreases");
                    }
                }
            }
        }

        /// <summary>
        /// Processes a batch of items asynchronously with performance tracking
        /// </summary>
        public async Task<BatchResult<TItem, TResult>> ProcessBatchAsync<TItem, TResult>(
            IReadOnlyList<TItem> items,
            Func<TItem, CancellationToken, Task<TResult>> operation,
            string? operationType = null,
            int? customBatchSize = null,
            CancellationToken cancellationToken = default)
        {
            if (!_initialized)
                throw new InvalidOperationException("Infrastructure service not initialized");

            operationType ??= typeof(TItem).Name;

            using var tracker = _performanceTracker.TrackOperation("Batch", operationType);

            // Combine cancellation tokens
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                _shutdownTokenSource.Token, cancellationToken);

            var result = await _batchManager.ProcessBatchAsync(
                items, operation, operationType, customBatchSize, linkedCts.Token);

            // Track statistics
            _performanceTracker.IncrementCounter("Batch", "TotalBatches");
            _performanceTracker.IncrementCounter("Batch", "TotalOperations", result.TotalOperations);
            _performanceTracker.IncrementCounter("Batch", "SuccessOperations", result.SuccessCount);
            _performanceTracker.IncrementCounter("Batch", "ErrorOperations", result.ErrorCount);

            if (result.IsCompletelySuccessful)
            {
                _performanceTracker.IncrementCounter("Batch", "SuccessBatches");
            }
            else if (result.ErrorCount > 0)
            {
                _performanceTracker.IncrementCounter("Batch", "ErrorBatches");
            }

            return result;
        }

        /// <summary>
        /// Processes a batch of items synchronously with performance tracking
        /// </summary>
        public BatchResult<TItem, TResult> ProcessBatch<TItem, TResult>(
            IReadOnlyList<TItem> items,
            Func<TItem, TResult> operation,
            string? operationType = null,
            int? customBatchSize = null,
            CancellationToken cancellationToken = default)
        {
            if (!_initialized)
                throw new InvalidOperationException("Infrastructure service not initialized");

            operationType ??= typeof(TItem).Name;

            using var tracker = _performanceTracker.TrackOperation("Batch", operationType);

            // Combine cancellation tokens
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                _shutdownTokenSource.Token, cancellationToken);

            var result = _batchManager.ProcessBatch(
                items, operation, operationType, customBatchSize, linkedCts.Token);

            // Track statistics
            _performanceTracker.IncrementCounter("Batch", "TotalBatches");
            _performanceTracker.IncrementCounter("Batch", "TotalOperations", result.TotalOperations);
            _performanceTracker.IncrementCounter("Batch", "SuccessOperations", result.SuccessCount);
            _performanceTracker.IncrementCounter("Batch", "ErrorOperations", result.ErrorCount);

            if (result.IsCompletelySuccessful)
            {
                _performanceTracker.IncrementCounter("Batch", "SuccessBatches");
            }
            else if (result.ErrorCount > 0)
            {
                _performanceTracker.IncrementCounter("Batch", "ErrorBatches");
            }

            return result;
        }

        /// <summary>
        /// Acquires a read lock with performance tracking
        /// </summary>
        public IDisposable AcquireReadLock(string key, string category = "Default")
        {
            if (!_initialized)
                throw new InvalidOperationException("Infrastructure service not initialized");

            using (_performanceTracker.TrackOperation("Lock", $"Read-{category}"))
            {
                return _lockManager.AcquireReadLock(key);
            }
        }

        /// <summary>
        /// Acquires a write lock with performance tracking
        /// </summary>
        public IDisposable AcquireWriteLock(string key, string category = "Default")
        {
            if (!_initialized)
                throw new InvalidOperationException("Infrastructure service not initialized");

            using (_performanceTracker.TrackOperation("Lock", $"Write-{category}"))
            {
                return _lockManager.AcquireWriteLock(key);
            }
        }

        /// <summary>
        /// Gets system diagnostics information
        /// </summary>
        public SystemDiagnostics GetDiagnostics()
        {
            return new SystemDiagnostics
            {
                Timestamp = DateTime.UtcNow,
                MemoryStatus = _memoryMonitor.GetMemoryStatus(),
                PerformanceStats = _performanceTracker.GetStatistics(),
                BatchStats = _batchManager.GetStatistics(),
                LockStats = _lockManager.GetStatistics(),
                MemoryStats = _memoryManager.GetStatistics(),
                Configuration = new Dictionary<string, string>
                {
                    ["MaxConcurrency"] = _configuration.MaxConcurrency.ToString(),
                    ["BatchSize"] = _configuration.BatchSize.ToString(),
                    ["LockPartitions"] = _configuration.LockPartitions.ToString(),
                    ["MaxGraphCapacity"] = _configuration.MaxGraphCapacity.ToString(),
                    ["EnableMemoryMonitoring"] = _configuration.EnableMemoryMonitoring.ToString(),
                    ["EnablePerformanceTracking"] = _configuration.EnablePerformanceTracking.ToString(),
                    ["AdaptiveConcurrency"] = _configuration.AdaptiveConcurrency.ToString(),
                    ["MemoryThresholdMB"] = (_configuration.MemoryThresholdBytes / (1024 * 1024)).ToString()
                }
            };
        }

        /// <summary>
        /// Shuts down the infrastructure service
        /// </summary>
        public void Shutdown()
        {
            if (!_initialized)
                return;

            // Cancel all operations
            _shutdownTokenSource.Cancel();

            // Stop monitoring
            if (_memoryMonitor.IsMonitoring)
            {
                _memoryMonitor.StopMonitoring();
            }

            _initialized = false;
        }

        /// <summary>
        /// Disposes resources
        /// </summary>
        public void Dispose()
        {
            Shutdown();

            foreach (var resource in _managedResources)
            {
                resource.Dispose();
            }

            _managedResources.Clear();
        }
    }

    /// <summary>
    /// System diagnostics information
    /// </summary>
    public class SystemDiagnostics
    {
        public DateTime Timestamp { get; set; }
        public required MemoryStatusInfo MemoryStatus { get; set; }
        public required PerformanceStatistics PerformanceStats { get; set; }
        public required BatchOperationStatistics BatchStats { get; set; }
        public required LockStatistics LockStats { get; set; }
        public required MemoryStatistics MemoryStats { get; set; }
        public required Dictionary<string, string> Configuration { get; set; }

        public override string ToString()
        {
            return $"Diagnostics [{Timestamp:HH:mm:ss.fff}]: " +
                   $"Memory: {MemoryStatus?.TotalMemoryMB:F2} MB ({MemoryStatus?.PressureLevel}), " +
                   $"Operations: {PerformanceStats?.TotalOperations}, " +
                   $"Batches: {BatchStats?.TotalBatches}, " +
                   $"Concurrency: {BatchStats?.CurrentConcurrentBatches}/{BatchStats?.MaxConcurrentBatches}";
        }
    }
}
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using TSqlColumnLineage.Core.Infrastructure.Memory;

namespace TSqlColumnLineage.Core.Infrastructure.Concurency
{
    /// <summary>
    /// Manages and executes batch operations with optimized concurrency and memory usage
    /// Designed for data-intensive lineage tracking operations with adaptive throttling
    /// </summary>
    public sealed class BatchOperationManager
    {
        // Singleton instance
        private static readonly Lazy<BatchOperationManager> _instance =
            new(() => new BatchOperationManager());

        // Concurrency settings
        private int _maxConcurrentBatches = Environment.ProcessorCount;
        private int _defaultBatchSize = 1000;

        // Semaphore to limit concurrent batches
        private readonly SemaphoreSlim _batchLimiter;

        // Statistics
        private long _totalBatchesProcessed;
        private long _totalOperationsProcessed;
        private long _totalErrors;
        private readonly ConcurrentDictionary<string, long> _operationTypeStats =
            new();

        // Memory optimization
        private MemoryPressureLevel _lastPressureLevel = MemoryPressureLevel.Low;

        /// <summary>
        /// Gets the singleton instance
        /// </summary>
        public static BatchOperationManager Instance => _instance.Value;

        /// <summary>
        /// Gets or sets the maximum number of concurrent batches
        /// </summary>
        public int MaxConcurrentBatches
        {
            get => _maxConcurrentBatches;
            set => _maxConcurrentBatches = Math.Max(1, value);
        }

        /// <summary>
        /// Gets or sets the default batch size
        /// </summary>
        public int DefaultBatchSize
        {
            get => _defaultBatchSize;
            set => _defaultBatchSize = Math.Max(1, value);
        }

        /// <summary>
        /// Private constructor
        /// </summary>
        private BatchOperationManager()
        {
            _batchLimiter = new SemaphoreSlim(_maxConcurrentBatches, _maxConcurrentBatches);

            // Register for memory pressure events
            MemoryPressureMonitor.Instance.MemoryPressureChanged += OnMemoryPressureChanged;
        }

        /// <summary>
        /// Handles memory pressure changes
        /// </summary>
        private void OnMemoryPressureChanged(object? sender, MemoryPressureEventArgs e)
        {
            _lastPressureLevel = e.NewLevel;

            // Adjust concurrency based on memory pressure
            if (e.NewLevel == MemoryPressureLevel.High && _maxConcurrentBatches > 2)
            {
                int newMax = Math.Max(2, _maxConcurrentBatches / 2);
                Interlocked.Exchange(ref _maxConcurrentBatches, newMax);

                // Update semaphore
                int current = _batchLimiter.CurrentCount;
                if (current > newMax)
                {
                    for (int i = 0; i < current - newMax; i++)
                    {
                        _batchLimiter.Wait(0);
                    }
                }
            }
            else if (e.NewLevel == MemoryPressureLevel.Low && _maxConcurrentBatches < Environment.ProcessorCount)
            {
                int newMax = Math.Min(Environment.ProcessorCount, _maxConcurrentBatches * 2);
                Interlocked.Exchange(ref _maxConcurrentBatches, newMax);

                // Update semaphore
                int current = _batchLimiter.CurrentCount;
                if (current < newMax)
                {
                    for (int i = 0; i < newMax - current; i++)
                    {
                        _batchLimiter.Release();
                    }
                }
            }
        }

        /// <summary>
        /// Processes a batch of items with the specified operation asynchronously
        /// </summary>
        public async Task<BatchResult<TItem, TResult>> ProcessBatchAsync<TItem, TResult>(
            IReadOnlyList<TItem> items,
            Func<TItem, CancellationToken, Task<TResult>> operation,
            string? operationType = null,
            int? customBatchSize = null,
            CancellationToken cancellationToken = default)
        {
            if (items == null || items.Count == 0)
                return BatchResult<TItem, TResult>.Empty;

            ArgumentNullException.ThrowIfNull(operation);

            operationType ??= typeof(TItem).Name;

            // Determine optimal batch size based on memory pressure
            int batchSize = customBatchSize ?? GetOptimalBatchSize(_lastPressureLevel);

            // Create batch processor
            var processor = new BatchProcessor<TItem, TResult>(
                items, operation, batchSize, operationType);

            // Process the batch
            var result = await processor.ProcessAsync(_batchLimiter, cancellationToken)
                .ConfigureAwait(false);

            // Update statistics
            Interlocked.Increment(ref _totalBatchesProcessed);
            Interlocked.Add(ref _totalOperationsProcessed, result.SuccessCount + result.ErrorCount);
            Interlocked.Add(ref _totalErrors, result.ErrorCount);

            _operationTypeStats.AddOrUpdate(
                operationType,
                result.SuccessCount,
                (_, current) => current + result.SuccessCount);

            return result;
        }

        /// <summary>
        /// Processes a batch of items with the specified operation synchronously
        /// </summary>
        public BatchResult<TItem, TResult> ProcessBatch<TItem, TResult>(
            IReadOnlyList<TItem> items,
            Func<TItem, TResult> operation,
            string? operationType = null,
            int? customBatchSize = null,
            CancellationToken cancellationToken = default)
        {
            if (items == null || items.Count == 0)
                return BatchResult<TItem, TResult>.Empty;

            ArgumentNullException.ThrowIfNull(operation);

            operationType ??= typeof(TItem).Name;

            // Determine optimal batch size based on memory pressure
            int batchSize = customBatchSize ?? GetOptimalBatchSize(_lastPressureLevel);

            var allResults = new List<TResult>(items.Count);
            var errors = new ConcurrentDictionary<int, Exception>();

            // Process in batches
            for (int batchStart = 0; batchStart < items.Count; batchStart += batchSize)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int currentBatchSize = Math.Min(batchSize, items.Count - batchStart);

                // Acquire semaphore
                _batchLimiter.Wait(cancellationToken);

                try
                {
                    // Process this batch
                    var results = new TResult[currentBatchSize];

                    Parallel.For(0, currentBatchSize, i =>
                    {
                        try
                        {
                            int itemIndex = batchStart + i;
                            results[i] = operation(items[itemIndex]);
                        }
                        catch (Exception ex)
                        {
                            errors[batchStart + i] = ex;
                        }
                    });

                    // Add results
                    for (int i = 0; i < currentBatchSize; i++)
                    {
                        if (!errors.ContainsKey(batchStart + i))
                        {
                            allResults.Add(results[i]);
                        }
                    }
                }
                finally
                {
                    _batchLimiter.Release();
                }
            }

            var result = new BatchResult<TItem, TResult>
            {
                SuccessCount = allResults.Count,
                ErrorCount = errors.Count,
                Results = allResults,
                Errors = errors
            };

            // Update statistics
            Interlocked.Increment(ref _totalBatchesProcessed);
            Interlocked.Add(ref _totalOperationsProcessed, result.SuccessCount + result.ErrorCount);
            Interlocked.Add(ref _totalErrors, result.ErrorCount);

            _operationTypeStats.AddOrUpdate(
                operationType,
                result.SuccessCount,
                (_, current) => current + result.SuccessCount);

            return result;
        }

        /// <summary>
        /// Gets batch operation statistics
        /// </summary>
        public BatchOperationStatistics GetStatistics()
        {
            var operationStats = new Dictionary<string, long>();
            foreach (var kvp in _operationTypeStats)
            {
                operationStats[kvp.Key] = kvp.Value;
            }

            return new BatchOperationStatistics
            {
                TotalBatches = Interlocked.Read(ref _totalBatchesProcessed),
                TotalOperations = Interlocked.Read(ref _totalOperationsProcessed),
                TotalErrors = Interlocked.Read(ref _totalErrors),
                MaxConcurrentBatches = _maxConcurrentBatches,
                CurrentConcurrentBatches = _batchLimiter.CurrentCount,
                CurrentMemoryPressure = _lastPressureLevel,
                OperationTypeStats = operationStats
            };
        }

        /// <summary>
        /// Gets optimal batch size based on memory pressure
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetOptimalBatchSize(MemoryPressureLevel pressureLevel)
        {
            return pressureLevel switch
            {
                MemoryPressureLevel.High => Math.Max(1, _defaultBatchSize / 4),
                MemoryPressureLevel.Medium => Math.Max(1, _defaultBatchSize / 2),
                _ => _defaultBatchSize
            };
        }
    }

    /// <summary>
    /// Helper class to process batches of items asynchronously
    /// </summary>
    /// <remarks>
    /// Creates a new batch processor
    /// </remarks>
    internal class BatchProcessor<TItem, TResult>(
        IReadOnlyList<TItem> items,
        Func<TItem, CancellationToken, Task<TResult>> operation,
        int batchSize,
        string operationType)
    {

        /// <summary>
        /// Processes all items in batches
        /// </summary>
        public async Task<BatchResult<TItem, TResult>> ProcessAsync(
            SemaphoreSlim semaphore,
            CancellationToken cancellationToken)
        {
            var allResults = new List<TResult>(items.Count);
            var errors = new ConcurrentDictionary<int, Exception>();

            // Process in batches
            for (int batchStart = 0; batchStart < items.Count; batchStart += batchSize)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int currentBatchSize = Math.Min(batchSize, items.Count - batchStart);

                // Acquire semaphore
                await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                try
                {
                    // Process this batch
                    var batchTasks = new Task<(int Index, TResult Result, Exception Error)>[currentBatchSize];

                    for (int i = 0; i < currentBatchSize; i++)
                    {
                        int itemIndex = batchStart + i;
                        batchTasks[i] = ProcessItemAsync(itemIndex, cancellationToken);
                    }

                    // Wait for all tasks to complete
                    await Task.WhenAll(batchTasks).ConfigureAwait(false);

                    // Collect results
                    foreach (var task in batchTasks)
                    {
                        var (index, result, error) = task.Result;

                        if (error != null)
                        {
                            errors[index] = error;
                        }
                        else
                        {
                            allResults.Add(result);
                        }
                    }
                }
                finally
                {
                    semaphore.Release();
                }
            }

            return new BatchResult<TItem, TResult>
            {
                SuccessCount = allResults.Count,
                ErrorCount = errors.Count,
                Results = allResults,
                Errors = errors
            };
        }

        /// <summary>
        /// Processes a single item
        /// </summary>
        private async Task<(int Index, TResult? Result, Exception? Error)> ProcessItemAsync(
            int index, CancellationToken cancellationToken)
        {
            try
            {
                var result = await operation(items[index], cancellationToken)
                    .ConfigureAwait(false);

                return (index, result, null);
            }
            catch (Exception ex)
            {
                return (index, default, ex);
            }
        }
    }

    /// <summary>
    /// Result of a batch operation
    /// </summary>
    public class BatchResult<TItem, TResult>
    {
        /// <summary>
        /// Gets an empty result
        /// </summary>
        public static readonly BatchResult<TItem, TResult> Empty = new()
        {
            SuccessCount = 0,
            ErrorCount = 0,
            Results = [],
            Errors = new ConcurrentDictionary<int, Exception>()
        };

        /// <summary>
        /// Gets the number of successful operations
        /// </summary>
        public int SuccessCount { get; set; }

        /// <summary>
        /// Gets the number of failed operations
        /// </summary>
        public int ErrorCount { get; set; }

        /// <summary>
        /// Gets the results of successful operations
        /// </summary>
        public List<TResult> Results { get; set; } = [];

        /// <summary>
        /// Gets the errors of failed operations
        /// </summary>
        public ConcurrentDictionary<int, Exception>? Errors { get; set; }

        /// <summary>
        /// Gets whether the batch was completely successful
        /// </summary>
        public bool IsCompletelySuccessful => ErrorCount == 0 && SuccessCount > 0;

        /// <summary>
        /// Gets the total number of operations
        /// </summary>
        public int TotalOperations => SuccessCount + ErrorCount;

        public override string ToString()
        {
            return $"Batch result: {SuccessCount} succeeded, {ErrorCount} failed";
        }
    }

    /// <summary>
    /// Batch operation statistics
    /// </summary>
    public class BatchOperationStatistics
    {
        public long TotalBatches { get; set; }
        public long TotalOperations { get; set; }
        public long TotalErrors { get; set; }
        public int MaxConcurrentBatches { get; set; }
        public int CurrentConcurrentBatches { get; set; }
        public MemoryPressureLevel CurrentMemoryPressure { get; set; }
        public Dictionary<string, long> OperationTypeStats { get; set; } = [];

        public override string ToString()
        {
            return $"Batches: {TotalBatches}, Operations: {TotalOperations}, " +
                   $"Errors: {TotalErrors}, Concurrency: {CurrentConcurrentBatches}/{MaxConcurrentBatches}";
        }
    }
}
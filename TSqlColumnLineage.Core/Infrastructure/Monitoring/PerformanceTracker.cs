using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace TSqlColumnLineage.Core.Infrastructure.Monitoring
{
    /// <summary>
    /// Tracks and reports performance metrics for lineage analysis operations
    /// Provides detailed timing information and identifies bottlenecks
    /// </summary>
    public sealed class PerformanceTracker
    {
        // Singleton instance
        private static readonly Lazy<PerformanceTracker> _instance =
            new(() => new PerformanceTracker());

        // Performance counters by category
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, PerformanceCounter>> _counters =
            new();

        // Active operations
        private readonly ConcurrentDictionary<string, List<ActiveOperation>> _activeOperations =
            new();

        // Thread-safe access to active operations
        private readonly ReaderWriterLockSlim _activeOperationsLock = new();

        // Recent operations history
        private readonly ConcurrentQueue<OperationRecord> _recentOperations = new();
        private const int MaxRecentOperations = 1000;

        // Thresholds for warning on slow operations
        private readonly ConcurrentDictionary<string, TimeSpan> _operationWarningThresholds =
            new();

        // Global stats
        private long _totalOperations;
        private long _totalOperationTime;
        private long _warningCount;

        /// <summary>
        /// Gets the singleton instance
        /// </summary>
        public static PerformanceTracker Instance => _instance.Value;

        /// <summary>
        /// Creates a new performance tracker
        /// </summary>
        private PerformanceTracker()
        {
            // Initialize default thresholds
            SetWarningThreshold("Parse", TimeSpan.FromSeconds(2));
            SetWarningThreshold("Analyze", TimeSpan.FromSeconds(5));
            SetWarningThreshold("BuildLineage", TimeSpan.FromSeconds(3));
            SetWarningThreshold("ProcessSql", TimeSpan.FromSeconds(10));
        }

        /// <summary>
        /// Begins tracking an operation
        /// </summary>
        public IDisposable TrackOperation(string category, string operation)
        {
            Interlocked.Increment(ref _totalOperations);

            var tracker = new OperationTracker(this, category, operation);

            _activeOperationsLock.EnterWriteLock();
            try
            {
                if (!_activeOperations.TryGetValue(category, out var operations))
                {
                    operations = [];
                    _activeOperations[category] = operations;
                }

                operations.Add(new ActiveOperation
                {
                    Category = category,
                    Operation = operation,
                    StartTime = DateTime.UtcNow,
                    ThreadId = Environment.CurrentManagedThreadId
                });
            }
            finally
            {
                _activeOperationsLock.ExitWriteLock();
            }

            return tracker;
        }

        /// <summary>
        /// Sets a warning threshold for an operation
        /// </summary>
        public void SetWarningThreshold(string operation, TimeSpan threshold)
        {
            _operationWarningThresholds[operation] = threshold;
        }

        /// <summary>
        /// Gets performance statistics
        /// </summary>
        public PerformanceStatistics GetStatistics()
        {
            var activeOps = GetActiveOperations();
            var counterStats = new Dictionary<string, Dictionary<string, long>>();

            foreach (var category in _counters.Keys)
            {
                var categoryCounters = new Dictionary<string, long>();
                counterStats[category] = categoryCounters;

                if (_counters.TryGetValue(category, out var counters))
                {
                    foreach (var kvp in counters)
                    {
                        categoryCounters[kvp.Key] = kvp.Value.Value;
                    }
                }
            }

            // Get recent operations (with the most recent first)
            var recentOps = _recentOperations.ToArray().Reverse().ToArray();

            return new PerformanceStatistics
            {
                TotalOperations = Interlocked.Read(ref _totalOperations),
                TotalOperationTimeMs = Interlocked.Read(ref _totalOperationTime),
                WarningCount = Interlocked.Read(ref _warningCount),
                ActiveOperations = activeOps,
                CounterValues = counterStats,
                RecentOperations = recentOps
            };
        }

        /// <summary>
        /// Gets active operations by category
        /// </summary>
        public List<ActiveOperation> GetActiveOperations(string category = "")
        {
            var result = new List<ActiveOperation>();

            _activeOperationsLock.EnterReadLock();
            try
            {
                if (string.IsNullOrEmpty(category))
                {
                    foreach (var kvp in _activeOperations)
                    {
                        result.AddRange(kvp.Value);
                    }
                }
                else if (_activeOperations.TryGetValue(category, out var operations))
                {
                    result.AddRange(operations);
                }
            }
            finally
            {
                _activeOperationsLock.ExitReadLock();
            }

            return result;
        }

        /// <summary>
        /// Increments a performance counter
        /// </summary>
        public void IncrementCounter(string category, string counter, long value = 1)
        {
            var categoryCounters = _counters.GetOrAdd(category, _ => new ConcurrentDictionary<string, PerformanceCounter>());

            var perfCounter = categoryCounters.GetOrAdd(counter, _ => new PerformanceCounter());
            perfCounter.Increment(value);
        }

        /// <summary>
        /// Sets a performance counter value
        /// </summary>
        public void SetCounter(string category, string counter, long value)
        {
            var categoryCounters = _counters.GetOrAdd(category, _ => new ConcurrentDictionary<string, PerformanceCounter>());

            var perfCounter = categoryCounters.GetOrAdd(counter, _ => new PerformanceCounter());
            perfCounter.SetValue(value);
        }

        /// <summary>
        /// Updates a counter with a maximum value
        /// </summary>
        public void UpdateMaxCounter(string category, string counter, long value)
        {
            var categoryCounters = _counters.GetOrAdd(category, _ => new ConcurrentDictionary<string, PerformanceCounter>());

            var perfCounter = categoryCounters.GetOrAdd(counter, _ => new PerformanceCounter());
            perfCounter.UpdateMax(value);
        }

        /// <summary>
        /// Records a complete operation
        /// </summary>
        internal void CompleteOperation(OperationTracker tracker)
        {
            var elapsedMs = tracker.ElapsedMilliseconds;
            Interlocked.Add(ref _totalOperationTime, elapsedMs);
            _activeOperationsLock.EnterWriteLock();
            try
            {
                if (_activeOperations.TryGetValue(tracker.Category, out var operations))
                {
                    for (int i = operations.Count - 1; i >= 0; i--)
                    {
                        var op = operations[i];
                        if (op.Operation == tracker.Operation && op.ThreadId == tracker.ThreadId)
                        {
                            operations.RemoveAt(i);
                            break;
                        }
                    }
                }
            }
            finally
            {
                _activeOperationsLock.ExitWriteLock();
            }

            // Check for warning thresholds
            bool isWarning = false;
            if (_operationWarningThresholds.TryGetValue(tracker.Operation, out var threshold))
            {
                isWarning = tracker.Elapsed > threshold;

                if (isWarning)
                {
                    Interlocked.Increment(ref _warningCount);
                }
            }

            // Add to recent operations
            var record = new OperationRecord
            {
                Category = tracker.Category,
                Operation = tracker.Operation,
                StartTime = tracker.StartTime,
                EndTime = DateTime.UtcNow,
                ElapsedMs = elapsedMs,
                ThreadId = tracker.ThreadId,
                IsWarning = isWarning
            };

            _recentOperations.Enqueue(record);

            // Trim excess operations
            while (_recentOperations.Count > MaxRecentOperations)
            {
                _recentOperations.TryDequeue(out _);
            }
        }
    }

    /// <summary>
    /// Tracks a single operation
    /// </summary>
    /// <remarks>
    /// Creates a new operation tracker
    /// </remarks>
    internal class OperationTracker(PerformanceTracker tracker, string category, string operation) : IDisposable
    {
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        private bool _disposed;

        /// <summary>
        /// Gets the operation category
        /// </summary>
        public string Category { get; } = category;

        /// <summary>
        /// Gets the operation name
        /// </summary>
        public string Operation { get; } = operation;

        /// <summary>
        /// Gets the thread ID
        /// </summary>
        public int ThreadId { get; } = Environment.CurrentManagedThreadId;

        /// <summary>
        /// Gets the start time
        /// </summary>
        public DateTime StartTime { get; } = DateTime.UtcNow;

        /// <summary>
        /// Gets the elapsed time
        /// </summary>
        public TimeSpan Elapsed => _stopwatch.Elapsed;

        /// <summary>
        /// Gets the elapsed milliseconds
        /// </summary>
        public long ElapsedMilliseconds => _stopwatch.ElapsedMilliseconds;

        /// <summary>
        /// Disposes resources and records operation completion
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            _stopwatch.Stop();
            tracker.CompleteOperation(this);
            _disposed = true;
        }
    }

    /// <summary>
    /// Thread-safe performance counter
    /// </summary>
    public class PerformanceCounter
    {
        private long _value;

        /// <summary>
        /// Gets the current value
        /// </summary>
        public long Value => Interlocked.Read(ref _value);

        /// <summary>
        /// Increments the counter
        /// </summary>
        public void Increment(long value = 1)
        {
            Interlocked.Add(ref _value, value);
        }

        /// <summary>
        /// Decrements the counter
        /// </summary>
        public void Decrement(long value = 1)
        {
            Interlocked.Add(ref _value, -value);
        }

        /// <summary>
        /// Sets the counter value
        /// </summary>
        public void SetValue(long value)
        {
            Interlocked.Exchange(ref _value, value);
        }

        /// <summary>
        /// Updates the counter if the new value is larger
        /// </summary>
        public void UpdateMax(long value)
        {
            long current;
            do
            {
                current = _value;
                if (value <= current)
                    return;
            } while (Interlocked.CompareExchange(ref _value, value, current) != current);
        }
    }

    /// <summary>
    /// Active operation information
    /// </summary>
    public class ActiveOperation
    {
        public required string Category { get; set; }
        public required string Operation { get; set; }
        public DateTime StartTime { get; set; }
        public int ThreadId { get; set; }
        public TimeSpan Duration => DateTime.UtcNow - StartTime;

        public override string ToString()
        {
            return $"{Category}/{Operation}: {Duration.TotalMilliseconds:F2} ms, Thread: {ThreadId}";
        }
    }

    /// <summary>
    /// Completed operation record
    /// </summary>
    public class OperationRecord
    {
        public required string Category { get; set; }
        public required string Operation { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public long ElapsedMs { get; set; }
        public int ThreadId { get; set; }
        public bool IsWarning { get; set; }

        public override string ToString()
        {
            return $"{Category}/{Operation}: {ElapsedMs} ms" + (IsWarning ? " (Warning)" : "");
        }
    }

    /// <summary>
    /// Performance statistics
    /// </summary>
    public class PerformanceStatistics
    {
        public long TotalOperations { get; set; }
        public long TotalOperationTimeMs { get; set; }
        public long WarningCount { get; set; }
        public required List<ActiveOperation> ActiveOperations { get; set; }
        public required Dictionary<string, Dictionary<string, long>> CounterValues { get; set; }
        public required OperationRecord[] RecentOperations { get; set; }

        public double AverageOperationTimeMs =>
            TotalOperations > 0 ? (double)TotalOperationTimeMs / TotalOperations : 0;

        public override string ToString()
        {
            return $"Operations: {TotalOperations}, Avg time: {AverageOperationTimeMs:F2} ms, " +
                   $"Warnings: {WarningCount}, Active: {ActiveOperations?.Count ?? 0}";
        }
    }
}
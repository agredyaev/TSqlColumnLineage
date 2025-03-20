using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TSqlColumnLineage.Core.Infrastructure;
using TSqlColumnLineage.Core.Infrastructure.Memory;

namespace TSqlColumnLineage.Core.Infrastructure.Monitoring
{
    /// <summary>
    /// Provides diagnostic capabilities for monitoring and troubleshooting
    /// the T-SQL column lineage analysis system
    /// </summary>
    public sealed class DiagnosticsTool
    {
        // Singleton instance
        private static readonly Lazy<DiagnosticsTool> _instance =
            new(() => new DiagnosticsTool());

        // The infrastructure service
        private readonly InfrastructureService _infrastructureService;

        // Diagnostic data collection
        private readonly List<SystemDiagnostics> _diagnosticsHistory = [];
        private readonly object _historyLock = new();
        private readonly int _maxHistoryItems = 100;

        // Continuous monitoring
        private CancellationTokenSource _monitoringCts;
        private Task _monitoringTask;
        private bool _isMonitoring;
        private TimeSpan _sampleInterval = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Gets the singleton instance
        /// </summary>
        public static DiagnosticsTool Instance => _instance.Value;

        /// <summary>
        /// Gets whether continuous monitoring is active
        /// </summary>
        public bool IsMonitoring => _isMonitoring;

        /// <summary>
        /// Gets or sets the diagnostics sampling interval
        /// </summary>
        public TimeSpan SampleInterval
        {
            get => _sampleInterval;
            set => _sampleInterval = value.TotalMilliseconds < 1000 ?
                TimeSpan.FromSeconds(1) : value;
        }

        /// <summary>
        /// Creates a new diagnostics tool
        /// </summary>
        private DiagnosticsTool()
        {
            _infrastructureService = InfrastructureService.Instance;
        }

        /// <summary>
        /// Collects current system diagnostics
        /// </summary>
        public SystemDiagnostics CollectDiagnostics()
        {
            var diagnostics = _infrastructureService.GetDiagnostics();

            lock (_historyLock)
            {
                _diagnosticsHistory.Add(diagnostics);

                // Trim history if needed
                if (_diagnosticsHistory.Count > _maxHistoryItems)
                {
                    _diagnosticsHistory.RemoveAt(0);
                }
            }

            return diagnostics;
        }

        /// <summary>
        /// Gets the diagnostics history
        /// </summary>
        public List<SystemDiagnostics> GetDiagnosticsHistory()
        {
            lock (_historyLock)
            {
                return new List<SystemDiagnostics>(_diagnosticsHistory);
            }
        }

        /// <summary>
        /// Clears the diagnostics history
        /// </summary>
        public void ClearDiagnosticsHistory()
        {
            lock (_historyLock)
            {
                _diagnosticsHistory.Clear();
            }
        }

        /// <summary>
        /// Starts continuous monitoring
        /// </summary>
        public void StartMonitoring()
        {
            if (_isMonitoring)
                return;

            _monitoringCts = new CancellationTokenSource();
            _monitoringTask = Task.Run(MonitoringLoop);
            _isMonitoring = true;
        }

        /// <summary>
        /// Stops continuous monitoring
        /// </summary>
        public void StopMonitoring()
        {
            if (!_isMonitoring)
                return;

            _monitoringCts?.Cancel();
            _monitoringTask?.Wait();
            _isMonitoring = false;
        }

        /// <summary>
        /// Continuous monitoring loop
        /// </summary>
        private async Task MonitoringLoop()
        {
            while (!_monitoringCts.IsCancellationRequested)
            {
                try
                {
                    // Collect diagnostics
                    CollectDiagnostics();

                    // Wait for next sample
                    await Task.Delay(_sampleInterval, _monitoringCts.Token)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception)
                {
                    // Log error
                    await Task.Delay(TimeSpan.FromSeconds(10), _monitoringCts.Token)
                        .ConfigureAwait(false);
                }
            }

            _monitoringCts?.Dispose();
            _monitoringCts = null;
        }

        /// <summary>
        /// Generates a diagnostic report
        /// </summary>
        public string GenerateDiagnosticReport(bool includeHistory = false)
        {
            var currentDiagnostics = CollectDiagnostics();
            var sb = new StringBuilder();

            sb.AppendLine("=== TSqlColumnLineage Diagnostic Report ===");
            sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff} UTC");
            sb.AppendLine();

            // System information
            sb.AppendLine("=== System Information ===");
            sb.AppendLine($"OS: {Environment.OSVersion}");
            sb.AppendLine($"Processors: {Environment.ProcessorCount}");
            sb.AppendLine($"64-bit Process: {Environment.Is64BitProcess}");
            sb.AppendLine($"CLR Version: {Environment.Version}");
            sb.AppendLine();

            // Memory information
            sb.AppendLine("=== Memory Status ===");
            sb.AppendLine($"Current Memory Usage: {currentDiagnostics.MemoryStatus.TotalMemoryMB:F2} MB");
            sb.AppendLine($"Memory Pressure Level: {currentDiagnostics.MemoryStatus.PressureLevel}");
            sb.AppendLine($"Collectable Memory: {currentDiagnostics.MemoryStatus.CollectableMB:F2} MB");
            sb.AppendLine($"GC Recommended: {currentDiagnostics.MemoryStatus.IsGcOptimizationRecommended}");
            sb.AppendLine($"Pooled Strings: {currentDiagnostics.MemoryStats.PooledStringsCount:N0} ({currentDiagnostics.MemoryStats.PooledStringsSizeBytes / 1024.0 / 1024.0:F2} MB)");
            sb.AppendLine($"Pooled Objects: {currentDiagnostics.MemoryStats.PooledObjectsCount:N0}");
            sb.AppendLine($"Pooled Arrays: {currentDiagnostics.MemoryStats.PooledArraysCount:N0}");
            sb.AppendLine();

            // Performance information
            sb.AppendLine("=== Performance Statistics ===");
            sb.AppendLine($"Total Operations: {currentDiagnostics.PerformanceStats.TotalOperations:N0}");
            sb.AppendLine($"Average Operation Time: {currentDiagnostics.PerformanceStats.AverageOperationTimeMs:F2} ms");
            sb.AppendLine($"Warning Count: {currentDiagnostics.PerformanceStats.WarningCount:N0}");
            sb.AppendLine($"Active Operations: {currentDiagnostics.PerformanceStats.ActiveOperations.Count:N0}");

            // Active operations
            if (currentDiagnostics.PerformanceStats.ActiveOperations.Count > 0)
            {
                sb.AppendLine("\nActive Operations:");
                foreach (var op in currentDiagnostics.PerformanceStats.ActiveOperations)
                {
                    sb.AppendLine($"  - {op.Category}/{op.Operation}: {op.Duration.TotalMilliseconds:F2} ms (Thread {op.ThreadId})");
                }
            }
            sb.AppendLine();

            // Batch processing information
            sb.AppendLine("=== Batch Processing Statistics ===");
            sb.AppendLine($"Total Batches: {currentDiagnostics.BatchStats.TotalBatches:N0}");
            sb.AppendLine($"Total Operations: {currentDiagnostics.BatchStats.TotalOperations:N0}");
            sb.AppendLine($"Total Errors: {currentDiagnostics.BatchStats.TotalErrors:N0}");
            sb.AppendLine($"Max Concurrent Batches: {currentDiagnostics.BatchStats.MaxConcurrentBatches}");
            sb.AppendLine($"Current Concurrent Batches: {currentDiagnostics.BatchStats.CurrentConcurrentBatches}");

            // Operation type stats
            if (currentDiagnostics.BatchStats.OperationTypeStats?.Count > 0)
            {
                sb.AppendLine("\nOperation Types:");
                foreach (var kvp in currentDiagnostics.BatchStats.OperationTypeStats)
                {
                    sb.AppendLine($"  - {kvp.Key}: {kvp.Value:N0}");
                }
            }
            sb.AppendLine();

            // Concurrency information
            sb.AppendLine("=== Concurrency Statistics ===");
            sb.AppendLine($"Partition Count: {currentDiagnostics.LockStats.PartitionCount}");
            sb.AppendLine($"Total Read Locks: {currentDiagnostics.LockStats.TotalReadLocks:N0}");
            sb.AppendLine($"Total Write Locks: {currentDiagnostics.LockStats.TotalWriteLocks:N0}");
            sb.AppendLine($"Total Contentions: {currentDiagnostics.LockStats.TotalContentions:N0}");
            sb.AppendLine($"Active Readers: {currentDiagnostics.LockStats.ActiveReaders}");
            sb.AppendLine($"Active Writers: {currentDiagnostics.LockStats.ActiveWriters}");
            sb.AppendLine($"Waiting Readers: {currentDiagnostics.LockStats.WaitingReaders}");
            sb.AppendLine($"Waiting Writers: {currentDiagnostics.LockStats.WaitingWriters}");
            sb.AppendLine();

            // Configuration
            sb.AppendLine("=== Configuration ===");
            foreach (var kvp in currentDiagnostics.Configuration)
            {
                sb.AppendLine($"{kvp.Key}: {kvp.Value}");
            }
            sb.AppendLine();

            // Performance counters
            if (currentDiagnostics.PerformanceStats.CounterValues?.Count > 0)
            {
                sb.AppendLine("=== Performance Counters ===");
                foreach (var category in currentDiagnostics.PerformanceStats.CounterValues)
                {
                    sb.AppendLine($"{category.Key}:");
                    foreach (var counter in category.Value)
                    {
                        sb.AppendLine($"  {counter.Key}: {counter.Value:N0}");
                    }
                }
                sb.AppendLine();
            }

            // Recent operations
            if (currentDiagnostics.PerformanceStats.RecentOperations?.Length > 0)
            {
                sb.AppendLine("=== Recent Operations ===");
                int count = Math.Min(20, currentDiagnostics.PerformanceStats.RecentOperations.Length);
                for (int i = 0; i < count; i++)
                {
                    var op = currentDiagnostics.PerformanceStats.RecentOperations[i];
                    sb.AppendLine($"{op.Category}/{op.Operation}: {op.ElapsedMs} ms" + (op.IsWarning ? " (Warning)" : ""));
                }
                sb.AppendLine();
            }

            // History
            if (includeHistory)
            {
                var history = GetDiagnosticsHistory();
                if (history.Count > 1) // More than just the current sample
                {
                    sb.AppendLine("=== Diagnostics History ===");

                    // Memory trend
                    sb.AppendLine("Memory Usage Trend (MB):");
                    foreach (var sample in history)
                    {
                        sb.AppendLine($"{sample.Timestamp:HH:mm:ss.fff}: {sample.MemoryStatus.TotalMemoryMB:F2} MB ({sample.MemoryStatus.PressureLevel})");
                    }
                    sb.AppendLine();

                    // Operation count trend
                    sb.AppendLine("Operation Count Trend:");
                    foreach (var sample in history)
                    {
                        sb.AppendLine($"{sample.Timestamp:HH:mm:ss.fff}: {sample.PerformanceStats.TotalOperations:N0} operations");
                    }
                    sb.AppendLine();

                    // Batch count trend
                    sb.AppendLine("Batch Count Trend:");
                    foreach (var sample in history)
                    {
                        sb.AppendLine($"{sample.Timestamp:HH:mm:ss.fff}: {sample.BatchStats.TotalBatches:N0} batches");
                    }
                    sb.AppendLine();
                }
            }

            sb.AppendLine("=== End of Diagnostic Report ===");

            return sb.ToString();
        }

        /// <summary>
        /// Runs a quick health check and returns any concerns
        /// </summary>
        public List<string> RunHealthCheck()
        {
            var concerns = new List<string>();
            var diagnostics = CollectDiagnostics();

            // Check memory pressure
            if (diagnostics.MemoryStatus.PressureLevel == MemoryPressureLevel.High)
            {
                concerns.Add("HIGH MEMORY PRESSURE: System is under high memory pressure, performance may degrade");

                if (diagnostics.MemoryStatus.CollectableMB > 500)
                {
                    concerns.Add($"MEMORY OPTIMIZATION: Consider forcing garbage collection to free {diagnostics.MemoryStatus.CollectableMB:F2} MB");
                }
            }

            // Check for potential deadlocks or lock contentions
            if (diagnostics.LockStats.WaitingWriters > 0 && diagnostics.LockStats.ActiveWriters > 0)
            {
                concerns.Add($"LOCK CONTENTION: {diagnostics.LockStats.WaitingWriters} writers waiting, potential contention");
            }

            if (diagnostics.LockStats.TotalContentions > 1000)
            {
                concerns.Add($"HIGH LOCK CONTENTIONS: {diagnostics.LockStats.TotalContentions:N0} total contentions detected");
            }

            // Check for potential memory leaks or other resource issues
            if (diagnostics.MemoryStats.PooledObjectsCount > 1000000)
            {
                concerns.Add($"HIGH OBJECT POOLING: {diagnostics.MemoryStats.PooledObjectsCount:N0} objects in pools, possible resource leak");
            }

            // Check for long-running operations
            if (diagnostics.PerformanceStats.ActiveOperations.Count > 0)
            {
                foreach (var op in diagnostics.PerformanceStats.ActiveOperations)
                {
                    if (op.Duration.TotalSeconds > 60)
                    {
                        concerns.Add($"LONG RUNNING OPERATION: {op.Category}/{op.Operation} running for {op.Duration.TotalSeconds:F1} seconds");
                    }
                }
            }

            // Check for high number of warnings
            if (diagnostics.PerformanceStats.WarningCount > 100)
            {
                concerns.Add($"HIGH WARNING COUNT: {diagnostics.PerformanceStats.WarningCount:N0} performance warnings detected");
            }

            // Check concurrency settings
            int processorCount = Environment.ProcessorCount;
            if (diagnostics.BatchStats.MaxConcurrentBatches > processorCount * 2)
            {
                concerns.Add($"HIGH CONCURRENCY: Max concurrent batches ({diagnostics.BatchStats.MaxConcurrentBatches}) exceeds 2x processor count ({processorCount})");
            }

            return concerns;
        }

        /// <summary>
        /// Disposes resources
        /// </summary>
        public void Dispose()
        {
            StopMonitoring();
            _monitoringCts?.Dispose();
        }
    }
}
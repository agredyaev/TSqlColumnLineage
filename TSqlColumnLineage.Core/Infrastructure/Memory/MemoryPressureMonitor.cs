using System;
using System.Threading;
using System.Threading.Tasks;

namespace TSqlColumnLineage.Core.Infrastructure.Memory
{
    /// <summary>
    /// Monitors and manages memory pressure for the application
    /// Provides callbacks for memory pressure events and adaptive resource management
    /// </summary>
    public sealed class MemoryPressureMonitor : IDisposable
    {
        // Singleton instance
        private static readonly Lazy<MemoryPressureMonitor> _instance =
            new(() => new MemoryPressureMonitor());

        // Monitoring state
        private readonly CancellationTokenSource _cancellationTokenSource = new();
        private Task? _monitoringTask;
        private MemoryPressureLevel _currentLevel = MemoryPressureLevel.Low;
        private bool _isMonitoring;
        private TimeSpan _monitoringInterval = TimeSpan.FromSeconds(5);

        // Memory thresholds (in bytes)
        private readonly long _mediumPressureThreshold = 1024L * 1024L * 1024L; // 1 GB
        private readonly long _highPressureThreshold = 1536L * 1024L * 1024L;   // 1.5 GB

        // Events
        public event EventHandler<MemoryPressureEventArgs>? MemoryPressureChanged;

        /// <summary>
        /// Gets the singleton instance
        /// </summary>
        public static MemoryPressureMonitor Instance => _instance.Value;

        /// <summary>
        /// Gets the current pressure level
        /// </summary>
        public MemoryPressureLevel CurrentLevel => _currentLevel;

        /// <summary>
        /// Gets or sets the monitoring interval
        /// </summary>
        public TimeSpan MonitoringInterval
        {
            get => _monitoringInterval;
            set => _monitoringInterval = value.TotalMilliseconds < 100 ?
                TimeSpan.FromMilliseconds(100) : value;
        }

        /// <summary>
        /// Gets whether monitoring is active
        /// </summary>
        public bool IsMonitoring => _isMonitoring;

        /// <summary>
        /// Starts memory pressure monitoring
        /// </summary>
        public void StartMonitoring()
        {
            if (_isMonitoring)
                return;

            _isMonitoring = true;
            _monitoringTask = Task.Run(MonitoringLoop);
        }

        /// <summary>
        /// Stops memory pressure monitoring
        /// </summary>
        public void StopMonitoring()
        {
            if (!_isMonitoring)
                return;

            _cancellationTokenSource.Cancel();
            _monitoringTask?.Wait();
            _isMonitoring = false;
        }

        /// <summary>
        /// Checks memory pressure once
        /// </summary>
        public MemoryPressureLevel CheckMemoryPressure()
        {
            var gcMemory = GC.GetTotalMemory(false);
            var newLevel = DetermineMemoryPressureLevel(gcMemory);

            if (newLevel != _currentLevel)
            {
                var oldLevel = _currentLevel;
                _currentLevel = newLevel;

                // Raise event
                OnMemoryPressureChanged(new MemoryPressureEventArgs(oldLevel, newLevel));

                // If high pressure, try to reduce it
                if (newLevel == MemoryPressureLevel.High)
                {
                    TryReduceMemoryPressure();
                }
            }

            return _currentLevel;
        }

        /// <summary>
        /// Tries to reduce memory pressure by forcing garbage collection
        /// </summary>
        public bool TryReduceMemoryPressure()
        {
            if (_currentLevel == MemoryPressureLevel.High)
            {
                GC.Collect(2, GCCollectionMode.Forced, true, true);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Gets memory status information
        /// </summary>
        public MemoryStatusInfo GetMemoryStatus()
        {
            long beforeCollection = GC.GetTotalMemory(false);
            long afterCollection = GC.GetTotalMemory(true);
            long collectable = beforeCollection - afterCollection;

            return new MemoryStatusInfo
            {
                TotalMemoryBytes = afterCollection,
                TotalMemoryMB = Math.Round(afterCollection / (1024.0 * 1024.0), 2),
                PressureLevel = _currentLevel,
                CollectableBytes = collectable,
                CollectableMB = Math.Round(collectable / (1024.0 * 1024.0), 2),
                IsGcOptimizationRecommended = _currentLevel == MemoryPressureLevel.High
            };
        }

        /// <summary>
        /// Determines memory pressure level based on current memory usage
        /// </summary>
        private MemoryPressureLevel DetermineMemoryPressureLevel(long memoryBytes)
        {
            if (memoryBytes >= _highPressureThreshold)
                return MemoryPressureLevel.High;

            if (memoryBytes >= _mediumPressureThreshold)
                return MemoryPressureLevel.Medium;

            return MemoryPressureLevel.Low;
        }

        /// <summary>
        /// Continuous monitoring loop
        /// </summary>
        private async Task MonitoringLoop()
        {
            while (!_cancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    CheckMemoryPressure();

                    await Task.Delay(_monitoringInterval, _cancellationTokenSource.Token)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception)
                {
                    // Log error
                    await Task.Delay(TimeSpan.FromSeconds(10), _cancellationTokenSource.Token)
                        .ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Raises the memory pressure changed event
        /// </summary>
        private void OnMemoryPressureChanged(MemoryPressureEventArgs e)
        {
            MemoryPressureChanged?.Invoke(this, e);
        }

        /// <summary>
        /// Disposes resources
        /// </summary>
        public void Dispose()
        {
            StopMonitoring();
            _cancellationTokenSource.Dispose();
        }
    }

    /// <summary>
    /// Memory pressure event arguments
    /// </summary>
    public class MemoryPressureEventArgs(MemoryPressureLevel oldLevel, MemoryPressureLevel newLevel) : EventArgs
    {
        public MemoryPressureLevel OldLevel { get; } = oldLevel;
        public MemoryPressureLevel NewLevel { get; } = newLevel;
        public DateTime Timestamp { get; } = DateTime.UtcNow;
    }

    /// <summary>
    /// Memory pressure levels
    /// </summary>
    public enum MemoryPressureLevel
    {
        Low,
        Medium,
        High
    }

    /// <summary>
    /// Memory status information
    /// </summary>
    public class MemoryStatusInfo
    {
        public long TotalMemoryBytes { get; set; }
        public double TotalMemoryMB { get; set; }
        public MemoryPressureLevel PressureLevel { get; set; }
        public long CollectableBytes { get; set; }
        public double CollectableMB { get; set; }
        public bool IsGcOptimizationRecommended { get; set; }

        public override string ToString()
        {
            return $"Memory: {TotalMemoryMB:F2} MB, Collectable: {CollectableMB:F2} MB, " +
                   $"Pressure: {PressureLevel}" +
                   (IsGcOptimizationRecommended ? " (GC optimization recommended)" : "");
        }
    }
}
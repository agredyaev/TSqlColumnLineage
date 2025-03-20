using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace TSqlColumnLineage.Infrastructure.Concurrency
{
    /// <summary>
    /// Provides partitioned locking for high-concurrency scenarios
    /// Optimized for minimal contention across multiple threads
    /// </summary>
    public sealed class PartitionedLockManager
    {
        // Lock partitions
        private readonly LockPartition[] _partitions;
        private readonly int _partitionCount;
        private readonly int _partitionMask;
        
        // Statistics
        private long _totalReadLocks;
        private long _totalWriteLocks;
        private long _totalContentions;
        
        /// <summary>
        /// Creates a new partitioned lock manager
        /// </summary>
        public PartitionedLockManager(int partitionCount = 32)
        {
            // Ensure partition count is a power of 2
            partitionCount = NextPowerOfTwo(partitionCount);
            _partitionCount = partitionCount;
            _partitionMask = partitionCount - 1;
            
            // Initialize partitions
            _partitions = new LockPartition[partitionCount];
            for (int i = 0; i < partitionCount; i++)
            {
                _partitions[i] = new LockPartition();
            }
        }
        
        /// <summary>
        /// Acquires a read lock for the specified key
        /// </summary>
        public IDisposable AcquireReadLock(string key)
        {
            if (string.IsNullOrEmpty(key))
                return NullLockScope.Instance;
                
            // Get partition
            int partitionIndex = GetPartitionIndex(key);
            var partition = _partitions[partitionIndex];
            
            // Acquire lock
            Interlocked.Increment(ref _totalReadLocks);
            partition.Lock.EnterReadLock();
            
            return new LockScope(partition.Lock, isWriter: false);
        }
        
        /// <summary>
        /// Acquires a read lock for the specified key
        /// </summary>
        public IDisposable AcquireReadLock(int key)
        {
            // Get partition
            int partitionIndex = GetPartitionIndex(key);
            var partition = _partitions[partitionIndex];
            
            // Acquire lock
            Interlocked.Increment(ref _totalReadLocks);
            partition.Lock.EnterReadLock();
            
            return new LockScope(partition.Lock, isWriter: false);
        }
        
        /// <summary>
        /// Acquires a write lock for the specified key
        /// </summary>
        public IDisposable AcquireWriteLock(string key)
        {
            if (string.IsNullOrEmpty(key))
                return NullLockScope.Instance;
                
            // Get partition
            int partitionIndex = GetPartitionIndex(key);
            var partition = _partitions[partitionIndex];
            
            // Check contention
            if (partition.Lock.WaitingWriteCount > 0 || partition.Lock.WaitingReadCount > 0)
            {
                Interlocked.Increment(ref _totalContentions);
            }
            
            // Acquire lock
            Interlocked.Increment(ref _totalWriteLocks);
            partition.Lock.EnterWriteLock();
            
            return new LockScope(partition.Lock, isWriter: true);
        }
        
        /// <summary>
        /// Acquires a write lock for the specified key
        /// </summary>
        public IDisposable AcquireWriteLock(int key)
        {
            // Get partition
            int partitionIndex = GetPartitionIndex(key);
            var partition = _partitions[partitionIndex];
            
            // Check contention
            if (partition.Lock.WaitingWriteCount > 0 || partition.Lock.WaitingReadCount > 0)
            {
                Interlocked.Increment(ref _totalContentions);
            }
            
            // Acquire lock
            Interlocked.Increment(ref _totalWriteLocks);
            partition.Lock.EnterWriteLock();
            
            return new LockScope(partition.Lock, isWriter: true);
        }
        
        /// <summary>
        /// Tries to acquire a read lock with timeout
        /// </summary>
        public IDisposable TryAcquireReadLock(string key, int timeoutMs)
        {
            if (string.IsNullOrEmpty(key))
                return NullLockScope.Instance;
                
            // Get partition
            int partitionIndex = GetPartitionIndex(key);
            var partition = _partitions[partitionIndex];
            
            // Try to acquire lock
            if (partition.Lock.TryEnterReadLock(timeoutMs))
            {
                Interlocked.Increment(ref _totalReadLocks);
                return new LockScope(partition.Lock, isWriter: false);
            }
            
            Interlocked.Increment(ref _totalContentions);
            return NullLockScope.Instance;
        }
        
        /// <summary>
        /// Tries to acquire a write lock with timeout
        /// </summary>
        public IDisposable TryAcquireWriteLock(string key, int timeoutMs)
        {
            if (string.IsNullOrEmpty(key))
                return NullLockScope.Instance;
                
            // Get partition
            int partitionIndex = GetPartitionIndex(key);
            var partition = _partitions[partitionIndex];
            
            // Try to acquire lock
            if (partition.Lock.TryEnterWriteLock(timeoutMs))
            {
                Interlocked.Increment(ref _totalWriteLocks);
                return new LockScope(partition.Lock, isWriter: true);
            }
            
            Interlocked.Increment(ref _totalContentions);
            return NullLockScope.Instance;
        }
        
        /// <summary>
        /// Gets lock statistics
        /// </summary>
        public LockStatistics GetStatistics()
        {
            int activeReaders = 0;
            int activeWriters = 0;
            int waitingReaders = 0;
            int waitingWriters = 0;
            
            for (int i = 0; i < _partitionCount; i++)
            {
                var lock_ = _partitions[i].Lock;
                activeReaders += lock_.CurrentReadCount;
                if (lock_.IsWriteLockHeld) activeWriters++;
                waitingReaders += lock_.WaitingReadCount;
                waitingWriters += lock_.WaitingWriteCount;
            }
            
            return new LockStatistics
            {
                PartitionCount = _partitionCount,
                TotalReadLocks = Interlocked.Read(ref _totalReadLocks),
                TotalWriteLocks = Interlocked.Read(ref _totalWriteLocks),
                TotalContentions = Interlocked.Read(ref _totalContentions),
                ActiveReaders = activeReaders,
                ActiveWriters = activeWriters,
                WaitingReaders = waitingReaders,
                WaitingWriters = waitingWriters
            };
        }
        
        /// <summary>
        /// Gets partition index for a string key
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetPartitionIndex(string key)
        {
            return (key.GetHashCode() & int.MaxValue) & _partitionMask;
        }
        
        /// <summary>
        /// Gets partition index for an integer key
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetPartitionIndex(int key)
        {
            return (key & int.MaxValue) & _partitionMask;
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
        
        /// <summary>
        /// Represents a lock partition
        /// </summary>
        private class LockPartition
        {
            public ReaderWriterLockSlim Lock { get; } = new ReaderWriterLockSlim();
        }
        
        /// <summary>
        /// Scope for automatic lock release
        /// </summary>
        private class LockScope : IDisposable
        {
            private readonly ReaderWriterLockSlim _lock;
            private readonly bool _isWriter;
            private bool _disposed;
            
            public LockScope(ReaderWriterLockSlim lock_, bool isWriter)
            {
                _lock = lock_;
                _isWriter = isWriter;
            }
            
            public void Dispose()
            {
                if (_disposed) return;
                
                if (_isWriter)
                    _lock.ExitWriteLock();
                else
                    _lock.ExitReadLock();
                    
                _disposed = true;
            }
        }
        
        /// <summary>
        /// Null lock scope that does nothing
        /// </summary>
        private class NullLockScope : IDisposable
        {
            public static readonly NullLockScope Instance = new NullLockScope();
            
            private NullLockScope() { }
            
            public void Dispose() { }
        }
    }
    
    /// <summary>
    /// Lock statistics
    /// </summary>
    public class LockStatistics
    {
        public int PartitionCount { get; set; }
        public long TotalReadLocks { get; set; }
        public long TotalWriteLocks { get; set; }
        public long TotalContentions { get; set; }
        public int ActiveReaders { get; set; }
        public int ActiveWriters { get; set; }
        public int WaitingReaders { get; set; }
        public int WaitingWriters { get; set; }
        
        public override string ToString()
        {
            return $"Partitions: {PartitionCount}, " +
                   $"Locks: {TotalReadLocks} read / {TotalWriteLocks} write, " +
                   $"Contentions: {TotalContentions}, " +
                   $"Active: {ActiveReaders} readers / {ActiveWriters} writers, " +
                   $"Waiting: {WaitingReaders} readers / {WaitingWriters} writers";
        }
    }
}
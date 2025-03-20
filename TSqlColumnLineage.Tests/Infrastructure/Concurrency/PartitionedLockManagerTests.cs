using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using TSqlColumnLineage.Core.Infrastructure.Concurency;
using Xunit;

namespace TSqlColumnLineage.Tests.Infrastructure.Concurrency
{
    public class PartitionedLockManagerTests
    {
        [Fact]
        public void Constructor_ShouldCreateManagerWithSpecifiedPartitions()
        {
            // Arrange & Act
            var manager = new PartitionedLockManager(64);
            
            // Assert
            var stats = manager.GetStatistics();
            stats.PartitionCount.Should().Be(64);
        }
        
        [Fact]
        public void Constructor_WithNonPowerOfTwoPartitions_ShouldRoundUpToNextPowerOfTwo()
        {
            // Arrange & Act
            var manager = new PartitionedLockManager(50);
            
            // Assert
            var stats = manager.GetStatistics();
            stats.PartitionCount.Should().Be(64); // Next power of 2 after 50
        }
        
        [Fact]
        public void AcquireReadLock_ShouldAllowMultipleReadersOnSameKey()
        {
            // Arrange
            var manager = new PartitionedLockManager();
            var key = "shared-key";
            var readerCount = 5;
            var countdownEvent = new CountdownEvent(readerCount);
            var exceptions = new List<Exception>();
            
            // Act - Multiple threads acquiring read locks
            for (int i = 0; i < readerCount; i++)
            {
                Task.Run(() =>
                {
                    try
                    {
                        using var readLock = manager.AcquireReadLock(key);
                        // Simulate some work with the lock
                        Thread.Sleep(50);
                    }
                    catch (Exception ex)
                    {
                        lock (exceptions)
                        {
                            exceptions.Add(ex);
                        }
                    }
                    finally
                    {
                        countdownEvent.Signal();
                    }
                });
            }
            
            // Wait for all readers to complete
            countdownEvent.Wait(TimeSpan.FromSeconds(3));
            
            // Assert
            exceptions.Should().BeEmpty("all readers should acquire locks without errors");
            var stats = manager.GetStatistics();
            stats.TotalReadLocks.Should().BeGreaterOrEqualTo(readerCount);
        }
        
        [Fact]
        public void AcquireWriteLock_ShouldBeExclusive()
        {
            // Arrange
            var manager = new PartitionedLockManager();
            var key = "exclusive-key";
            var value = 0;
            var writerCount = 5;
            var incrementsPerWriter = 100;
            var countdownEvent = new CountdownEvent(writerCount);
            
            // Act - Multiple threads acquiring write locks and incrementing a shared value
            for (int i = 0; i < writerCount; i++)
            {
                Task.Run(() =>
                {
                    try
                    {
                        for (int j = 0; j < incrementsPerWriter; j++)
                        {
                            using var writeLock = manager.AcquireWriteLock(key);
                            // Critical section - increment shared value
                            value++;
                        }
                    }
                    finally
                    {
                        countdownEvent.Signal();
                    }
                });
            }
            
            // Wait for all writers to complete
            countdownEvent.Wait(TimeSpan.FromSeconds(5));
            
            // Assert
            value.Should().Be(writerCount * incrementsPerWriter, "all increments should be atomic with exclusive locks");
            var stats = manager.GetStatistics();
            stats.TotalWriteLocks.Should().BeGreaterOrEqualTo(writerCount * incrementsPerWriter);
        }
        
        [Fact]
        public void ReadAndWriteLocks_ShouldEnforceConcurrencyRules()
        {
            // Arrange
            var manager = new PartitionedLockManager();
            var key = "shared-exclusive-key";
            var sharedFlag = false;
            var writersRunning = 0;
            var readersRunning = 0;
            
            // Act & Assert
            // Start a writer that holds the lock for some time
            var writerTask = Task.Run(() =>
            {
                using var writeLock = manager.AcquireWriteLock(key);
                Interlocked.Increment(ref writersRunning);

                // Hold the write lock for a moment
                Thread.Sleep(500);

                // Set the flag while holding the write lock
                sharedFlag = true;

                // Verify no readers are running while we hold write lock
                Interlocked.CompareExchange(ref readersRunning, 0, 0).Should().Be(0);

                Interlocked.Decrement(ref writersRunning);
            });
            
            // Give the writer time to acquire the lock
            Thread.Sleep(100);
            
            // Start multiple readers that try to read the flag
            var readerTasks = new List<Task>();
            for (int i = 0; i < 3; i++)
            {
                readerTasks.Add(Task.Run(() =>
                {
                    using var readLock = manager.AcquireReadLock(key);
                    Interlocked.Increment(ref readersRunning);

                    // At this point, the writer should have completed
                    // and set the flag to true
                    sharedFlag.Should().BeTrue("writer should have set the flag before readers start");

                    // Verify no writers are running while we hold read lock
                    Interlocked.CompareExchange(ref writersRunning, 0, 0).Should().Be(0);

                    // Hold the read lock for a moment
                    Thread.Sleep(100);

                    Interlocked.Decrement(ref readersRunning);
                }));
            }

            // Wait for all tasks to complete
            _ = Task.WhenAll(new[] { writerTask }.Concat(readerTasks)).Wait(TimeSpan.FromSeconds(5));
            
            // Final checks
            sharedFlag.Should().BeTrue();
            writersRunning.Should().Be(0);
            readersRunning.Should().Be(0);
        }
        
        [Fact]
        public void TryAcquireReadLock_WithTimeout_ShouldReturnNullLockScopeOnTimeout()
        {
            // Arrange
            var manager = new PartitionedLockManager();
            var key = "timeout-key";

            // Act - Create a long-running write lock
            using var writeLock = manager.AcquireWriteLock(key);
            // Try to acquire a read lock with a short timeout
            using var readLock = manager.TryAcquireReadLock(key, 10);
            // Assert
            // The readLock should be a non-null placeholder that does nothing when disposed
            readLock.Should().NotBeNull();

            // The writer should still hold the lock exclusively
            var stats = manager.GetStatistics();
            stats.ActiveWriters.Should().Be(1);
            stats.ActiveReaders.Should().Be(0); // Read lock acquisition should have failed
            stats.TotalContentions.Should().BeGreaterThan(0);
        }
        
        [Fact]
        public void TryAcquireWriteLock_WithTimeout_ShouldReturnNullLockScopeOnTimeout()
        {
            // Arrange
            var manager = new PartitionedLockManager();
            var key = "timeout-key";

            // Act - Create a long-running read lock
            using var readLock = manager.AcquireReadLock(key);
            // Try to acquire a write lock with a short timeout
            using var writeLock = manager.TryAcquireWriteLock(key, 10);
            // Assert
            // The writeLock should be a non-null placeholder that does nothing when disposed
            writeLock.Should().NotBeNull();

            // The reader should still hold the lock
            var stats = manager.GetStatistics();
            stats.ActiveReaders.Should().Be(1);
            stats.ActiveWriters.Should().Be(0); // Write lock acquisition should have failed
            stats.TotalContentions.Should().BeGreaterThan(0);
        }
        
        [Fact]
        public void GetStatistics_ShouldReturnAccurateStats()
        {
            // Arrange
            var manager = new PartitionedLockManager(16);
            
            // Act - Acquire some locks
            using (var readLock1 = manager.AcquireReadLock("key1"))
            using (var readLock2 = manager.AcquireReadLock("key2"))
            {
                // Get stats while holding read locks
                var stats = manager.GetStatistics();
                
                // Assert
                stats.PartitionCount.Should().Be(16);
                stats.TotalReadLocks.Should().BeGreaterOrEqualTo(2);
                stats.ActiveReaders.Should().BeGreaterOrEqualTo(2);
                stats.ActiveWriters.Should().Be(0);
            }
            
            // Update stats after releasing read locks
            var finalStats = manager.GetStatistics();
            finalStats.ActiveReaders.Should().Be(0);
        }
    }
}
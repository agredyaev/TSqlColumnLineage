using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using TSqlColumnLineage.Core.Infrastructure.Concurency;
using TSqlColumnLineage.Core.Infrastructure.Memory;
using Xunit;

namespace TSqlColumnLineage.Tests.Infrastructure.Concurrency
{
    public class BatchOperationManagerTests
    {
        [Fact]
        public void Instance_ShouldReturnSingletonInstance()
        {
            // Act
            var instance1 = BatchOperationManager.Instance;
            var instance2 = BatchOperationManager.Instance;
            
            // Assert
            instance1.Should().NotBeNull();
            instance1.Should().BeSameAs(instance2);
        }
        
        [Fact]
        public void MaxConcurrentBatches_ShouldGetAndSetCorrectly()
        {
            // Arrange
            var manager = BatchOperationManager.Instance;
            int originalValue = manager.MaxConcurrentBatches;
            
            try
            {
                // Act
                manager.MaxConcurrentBatches = 8;
                
                // Assert
                manager.MaxConcurrentBatches.Should().Be(8);
                
                // Zero or negative values should be corrected to 1
                manager.MaxConcurrentBatches = 0;
                manager.MaxConcurrentBatches.Should().Be(1);
                
                manager.MaxConcurrentBatches = -5;
                manager.MaxConcurrentBatches.Should().Be(1);
            }
            finally
            {
                // Restore original value
                manager.MaxConcurrentBatches = originalValue;
            }
        }
        
        [Fact]
        public void DefaultBatchSize_ShouldGetAndSetCorrectly()
        {
            // Arrange
            var manager = BatchOperationManager.Instance;
            int originalValue = manager.DefaultBatchSize;
            
            try
            {
                // Act
                manager.DefaultBatchSize = 500;
                
                // Assert
                manager.DefaultBatchSize.Should().Be(500);
                
                // Zero or negative values should be corrected to 1
                manager.DefaultBatchSize = 0;
                manager.DefaultBatchSize.Should().Be(1);
                
                manager.DefaultBatchSize = -5;
                manager.DefaultBatchSize.Should().Be(1);
            }
            finally
            {
                // Restore original value
                manager.DefaultBatchSize = originalValue;
            }
        }
        
        [Fact]
        public async Task ProcessBatchAsync_ShouldProcessItemsInBatches()
        {
            // Arrange
            var manager = BatchOperationManager.Instance;
            var items = Enumerable.Range(1, 100).ToList();
            
            // Set up a processing operation that squares each number
            Func<int, CancellationToken, Task<int>> operation = (item, ct) =>
            {
                return Task.FromResult(item * item);
            };
            
            // Act
            var result = await manager.ProcessBatchAsync(items, operation, "SquareNumbers", 10);
            
            // Assert
            result.Should().NotBeNull();
            result.SuccessCount.Should().Be(100);
            result.ErrorCount.Should().Be(0);
            result.IsCompletelySuccessful.Should().BeTrue();
            result.Results.Should().HaveCount(100);
            
            // Verify results are correct
            for (int i = 0; i < 100; i++)
            {
                result.Results[i].Should().Be((i + 1) * (i + 1));
            }
        }
        
        [Fact]
        public void ProcessBatch_ShouldProcessItemsInBatchesSynchronously()
        {
            // Arrange
            var manager = BatchOperationManager.Instance;
            var items = Enumerable.Range(1, 100).ToList();
            
            // Set up a processing operation that squares each number
            Func<int, int> operation = item => item * item;
            
            // Act
            var result = manager.ProcessBatch(items, operation, "SquareNumbers", 10);
            
            // Assert
            result.Should().NotBeNull();
            result.SuccessCount.Should().Be(100);
            result.ErrorCount.Should().Be(0);
            result.IsCompletelySuccessful.Should().BeTrue();
            result.Results.Should().HaveCount(100);
            
            // Verify results are correct
            for (int i = 0; i < 100; i++)
            {
                result.Results[i].Should().Be((i + 1) * (i + 1));
            }
        }
        
        [Fact]
        public async Task ProcessBatchAsync_ShouldHandleErrors()
        {
            // Arrange
            var manager = BatchOperationManager.Instance;
            var items = Enumerable.Range(1, 100).ToList();
            
            // Set up a processing operation that throws an exception for even numbers
            Func<int, CancellationToken, Task<int>> operation = (item, ct) =>
            {
                if (item % 2 == 0)
                {
                    throw new InvalidOperationException($"Error processing {item}");
                }
                return Task.FromResult(item * item);
            };
            
            // Act
            var result = await manager.ProcessBatchAsync(items, operation, "ErrorTest", 10);
            
            // Assert
            result.Should().NotBeNull();
            result.SuccessCount.Should().Be(50); // Only odd numbers succeed
            result.ErrorCount.Should().Be(50); // Even numbers fail
            result.IsCompletelySuccessful.Should().BeFalse();
            result.Results.Should().HaveCount(50);
            result.Errors.Should().HaveCount(50);
            
            // Verify errors are for even numbers
            foreach (var errorKvp in result.Errors)
            {
                int index = errorKvp.Key;
                // Fix 1: Remove the generic type argument and use explicit predicate
                items[index].Should().Match(x => x % 2 == 0, "only even numbers should have errors");
                // Fix 2: Add null check before attempting to get type
                errorKvp.Value.Should().NotBeNull();
                errorKvp.Value.Should().BeOfType<InvalidOperationException>();
            }
        }
        
        [Fact]
        public void ProcessBatch_EmptyItems_ShouldReturnEmptyResult()
        {
            // Arrange
            var manager = BatchOperationManager.Instance;
            var items = new List<int>();
            
            // Act
            var result = manager.ProcessBatch(items, item => item * item);
            
            // Assert
            result.Should().NotBeNull();
            result.Should().BeSameAs(BatchResult<int, int>.Empty);
            result.SuccessCount.Should().Be(0);
            result.ErrorCount.Should().Be(0);
            result.Results.Should().BeEmpty();
            result.Errors.Should().BeEmpty();
            result.IsCompletelySuccessful.Should().BeFalse(); // Empty is not considered successful
        }
        
        [Fact]
        public void ProcessBatch_WithCancellation_ShouldStopProcessing()
        {
            // Arrange
            var manager = BatchOperationManager.Instance;
            var items = Enumerable.Range(1, 1000).ToList(); // Large list to ensure batching
            var cts = new CancellationTokenSource();
            var processedCount = 0;
            
            // Set up an operation that increments counter and checks cancellation
            Func<int, int> operation = item =>
            {
                Interlocked.Increment(ref processedCount);
                
                // Cancel after processing some items
                if (processedCount > 10)
                {
                    cts.Cancel();
                    // Explicitly throw the exception since we're testing against it
                    cts.Token.ThrowIfCancellationRequested();
                }
                
                // Simulate some work
                Thread.Sleep(5);
                
                return item * item;
            };
            
            // Act & Assert
            Action act = () => manager.ProcessBatch(items, operation, "CancellationTest", null, cts.Token);
            act.Should().Throw<OperationCanceledException>();
            
            // Verify some items were processed before cancellation
            processedCount.Should().BeGreaterThan(0);
            processedCount.Should().BeLessThan(1000);
        }
        
        [Fact]
        public void GetStatistics_ShouldReturnCurrentStats()
        {
            // Arrange
            var manager = BatchOperationManager.Instance;
            
            // Act
            var stats = manager.GetStatistics();
            
            // Assert
            stats.Should().NotBeNull();
            stats.MaxConcurrentBatches.Should().Be(manager.MaxConcurrentBatches);
            stats.CurrentMemoryPressure.Should().BeOneOf(
                MemoryPressureLevel.Low, MemoryPressureLevel.Medium, MemoryPressureLevel.High);
            stats.OperationTypeStats.Should().NotBeNull();
        }
    }
}
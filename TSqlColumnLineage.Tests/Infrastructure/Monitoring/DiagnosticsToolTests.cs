using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using TSqlColumnLineage.Core.Infrastructure;
using TSqlColumnLineage.Core.Infrastructure.Monitoring;
using Xunit;

namespace TSqlColumnLineage.Tests.Infrastructure.Monitoring
{
    public class DiagnosticsToolTests
    {
        [Fact]
        public void Instance_ShouldReturnSingletonInstance()
        {
            // Act
            var instance1 = DiagnosticsTool.Instance;
            var instance2 = DiagnosticsTool.Instance;
            
            // Assert
            instance1.Should().NotBeNull();
            instance1.Should().BeSameAs(instance2);
        }
        
        [Fact]
        public void CollectDiagnostics_ShouldReturnSystemDiagnostics()
        {
            // Arrange
            var tool = DiagnosticsTool.Instance;
            
            // Ensure InfrastructureService is initialized
            var service = InfrastructureService.Instance;
            if (!service.IsInitialized)
            {
                service.Initialize();
            }
            
            // Act
            var diagnostics = tool.CollectDiagnostics();
            
            // Assert
            diagnostics.Should().NotBeNull();
            diagnostics.Timestamp.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
            diagnostics.MemoryStatus.Should().NotBeNull();
            diagnostics.PerformanceStats.Should().NotBeNull();
            diagnostics.BatchStats.Should().NotBeNull();
            diagnostics.LockStats.Should().NotBeNull();
            diagnostics.MemoryStats.Should().NotBeNull();
            diagnostics.Configuration.Should().NotBeEmpty();
        }
        
        [Fact]
        public void GetDiagnosticsHistory_ShouldReturnCollectedItems()
        {
            // Arrange
            var tool = DiagnosticsTool.Instance;
            tool.ClearDiagnosticsHistory(); // Start with empty history
            
            // Act
            // Collect a few diagnostics samples
            var sample1 = tool.CollectDiagnostics();
            Task.Delay(100).Wait(); // Small delay to ensure different timestamps
            var sample2 = tool.CollectDiagnostics();
            
            // Get history
            var history = tool.GetDiagnosticsHistory();
            
            // Assert
            history.Should().NotBeNull();
            history.Should().HaveCount(2);
            history[0].Timestamp.Should().Be(sample1.Timestamp);
            history[1].Timestamp.Should().Be(sample2.Timestamp);
        }
        
        [Fact]
        public void ClearDiagnosticsHistory_ShouldRemoveAllItems()
        {
            // Arrange
            var tool = DiagnosticsTool.Instance;
            
            // Collect some diagnostics
            tool.CollectDiagnostics();
            tool.CollectDiagnostics();
            
            // Act
            tool.ClearDiagnosticsHistory();
            var history = tool.GetDiagnosticsHistory();
            
            // Assert
            history.Should().NotBeNull();
            history.Should().BeEmpty();
        }
        
        [Fact]
        public void StartAndStopMonitoring_ShouldUpdateIsMonitoring()
        {
            // Arrange
            var tool = DiagnosticsTool.Instance;
            
            // Ensure monitoring is stopped
            if (tool.IsMonitoring)
            {
                tool.StopMonitoring();
            }
            
            // Act - start monitoring
            tool.StartMonitoring();
            
            try
            {
                // Assert
                tool.IsMonitoring.Should().BeTrue();
                
                // Short delay to collect at least one sample
                Task.Delay(tool.SampleInterval + TimeSpan.FromMilliseconds(100)).Wait();
                
                var history = tool.GetDiagnosticsHistory();
                history.Should().NotBeEmpty();
            }
            finally
            {
                // Act - stop monitoring
                tool.StopMonitoring();
                
                // Assert
                tool.IsMonitoring.Should().BeFalse();
            }
        }
        
        [Fact]
        public void SampleInterval_ShouldGetAndSetCorrectly()
        {
            // Arrange
            var tool = DiagnosticsTool.Instance;
            var originalInterval = tool.SampleInterval;
            
            try
            {
                // Act
                var testInterval = TimeSpan.FromSeconds(30);
                tool.SampleInterval = testInterval;
                
                // Assert
                tool.SampleInterval.Should().Be(testInterval);
                
                // Test too small interval is adjusted to minimum
                tool.SampleInterval = TimeSpan.FromMilliseconds(100);
                tool.SampleInterval.TotalMilliseconds.Should().BeGreaterOrEqualTo(1000);
            }
            finally
            {
                // Restore original interval
                tool.SampleInterval = originalInterval;
            }
        }
        
        [Fact]
        public void GenerateDiagnosticReport_ShouldReturnNonEmptyReport()
        {
            // Arrange
            var tool = DiagnosticsTool.Instance;
            
            // Act
            var report = tool.GenerateDiagnosticReport(includeHistory: true);
            
            // Assert
            report.Should().NotBeNullOrEmpty();
            report.Should().Contain("=== TSqlColumnLineage Diagnostic Report ===");
            report.Should().Contain("=== System Information ===");
            report.Should().Contain("=== Memory Status ===");
            report.Should().Contain("=== Performance Statistics ===");
        }
        
        [Fact]
        public void RunHealthCheck_ShouldReturnListOfConcerns()
        {
            // Arrange
            var tool = DiagnosticsTool.Instance;
            
            // Act
            var concerns = tool.RunHealthCheck();
            
            // Assert
            concerns.Should().NotBeNull();
            // We can't predict if there will be concerns, but the method should run without error
        }
    }
}
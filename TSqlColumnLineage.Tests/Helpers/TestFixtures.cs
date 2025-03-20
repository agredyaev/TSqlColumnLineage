using System;
using System.Collections.Generic;
using Moq;
using TSqlColumnLineage.Core.Domain.Context;
using TSqlColumnLineage.Core.Domain.Graph;
using TSqlColumnLineage.Core.Infrastructure;
using TSqlColumnLineage.Core.Infrastructure.Concurency;
using TSqlColumnLineage.Core.Infrastructure.Memory;
using TSqlColumnLineage.Core.Infrastructure.Monitoring;

namespace TSqlColumnLineage.Tests.Helpers
{
    /// <summary>
    /// Provides test fixtures and mock objects for unit tests
    /// </summary>
    public static class TestFixtures
    {
        /// <summary>
        /// Creates a sample LineageGraph with test data
        /// </summary>
        public static LineageGraph CreateSampleLineageGraph()
        {
            var graph = new LineageGraph(100, 100);
            
            // Create sample tables
            int customersTableId = graph.AddTableNode("Customers", "Table");
            int ordersTableId = graph.AddTableNode("Orders", "Table");
            int reportTableId = graph.AddTableNode("CustomerReport", "Table");
            
            // Create sample columns
            int customerIdCol = graph.AddColumnNode("CustomerId", "Customers", "int", false, false);
            int customerNameCol = graph.AddColumnNode("CustomerName", "Customers", "nvarchar", false, false);
            
            int orderIdCol = graph.AddColumnNode("OrderId", "Orders", "int", false, false);
            int orderCustomerIdCol = graph.AddColumnNode("CustomerId", "Orders", "int", false, false);
            int orderAmountCol = graph.AddColumnNode("Amount", "Orders", "decimal", false, false);
            
            int reportCustomerIdCol = graph.AddColumnNode("CustomerId", "CustomerReport", "int", false, false);
            int reportNameCol = graph.AddColumnNode("Name", "CustomerReport", "nvarchar", false, false);
            int reportTotalCol = graph.AddColumnNode("TotalAmount", "CustomerReport", "decimal", false, false);
            
            // Create expression node
            int sumExpressionId = graph.AddExpressionNode("SumAmount", "SUM(Amount)", "Aggregation", "decimal");
            
            // Add columns to tables
            graph.AddColumnToTable(customersTableId, customerIdCol);
            graph.AddColumnToTable(customersTableId, customerNameCol);
            
            graph.AddColumnToTable(ordersTableId, orderIdCol);
            graph.AddColumnToTable(ordersTableId, orderCustomerIdCol);
            graph.AddColumnToTable(ordersTableId, orderAmountCol);
            
            graph.AddColumnToTable(reportTableId, reportCustomerIdCol);
            graph.AddColumnToTable(reportTableId, reportNameCol);
            graph.AddColumnToTable(reportTableId, reportTotalCol);
            
            // Create lineage edges
            graph.AddDirectLineage(customerIdCol, reportCustomerIdCol, "INSERT", "INSERT INTO CustomerReport(CustomerId) SELECT CustomerId FROM Customers");
            graph.AddDirectLineage(customerNameCol, reportNameCol, "INSERT", "INSERT INTO CustomerReport(Name) SELECT CustomerName FROM Customers");
            
            graph.AddDirectLineage(orderCustomerIdCol, reportCustomerIdCol, "INSERT", "INSERT INTO CustomerReport(CustomerId) SELECT CustomerId FROM Orders");
            graph.AddIndirectLineage(orderAmountCol, sumExpressionId, "Aggregation", "SUM(Amount)");
            graph.AddDirectLineage(sumExpressionId, reportTotalCol, "INSERT", "INSERT INTO CustomerReport(TotalAmount) SELECT SUM(Amount) FROM Orders GROUP BY CustomerId");
            
            graph.AddJoinRelationship(customerIdCol, orderCustomerIdCol, "INNER JOIN");
            
            return graph;
        }
        
        /// <summary>
        /// Creates a mocked ContextManager with basic setup
        /// </summary>
        public static (ContextManager Manager, Mock<LineageGraph> GraphMock) CreateMockedContextManager()
        {
            var graphMock = new Mock<LineageGraph>();
            var manager = new ContextManager(graphMock.Object);
            
            // Set up some basic state
            manager.DeclareVariable("GlobalVar", "int", 42);
            manager.RegisterTable("dbo.Customers", 1);
            manager.RegisterTable("dbo.Orders", 2);
            
            return (manager, graphMock);
        }
        
        /// <summary>
        /// Creates a test SQL query with various T-SQL constructs
        /// </summary>
        public static string CreateSampleSqlQuery()
        {
            return @"
WITH CustomerOrders AS (
    SELECT 
        c.CustomerId,
        c.CustomerName,
        COUNT(o.OrderId) AS OrderCount,
        SUM(o.Amount) AS TotalAmount
    FROM dbo.Customers c
    INNER JOIN dbo.Orders o ON c.CustomerId = o.CustomerId
    WHERE c.IsActive = 1
    GROUP BY c.CustomerId, c.CustomerName
)
INSERT INTO dbo.CustomerReport (
    CustomerId,
    Name,
    OrderCount,
    TotalAmount
)
SELECT 
    co.CustomerId,
    co.CustomerName,
    co.OrderCount,
    co.TotalAmount
FROM CustomerOrders co
WHERE co.TotalAmount > 1000
ORDER BY co.TotalAmount DESC;";
        }
        
        /// <summary>
        /// Creates a mocked InfrastructureService for testing
        /// </summary>
        public static (InfrastructureService Service, 
                      Mock<MemoryPressureMonitor> MemoryMonitorMock,
                      Mock<MemoryManager> MemoryManagerMock,
                      Mock<BatchOperationManager> BatchManagerMock,
                      Mock<PerformanceTracker> PerformanceTrackerMock,
                      Mock<PartitionedLockManager> LockManagerMock) 
        CreateMockedInfrastructureService()
        {
            // Note: Since InfrastructureService uses singletons, we can't actually mock it directly
            // This is a limitation of the design. In a real test environment, we would need to
            // refactor the code to use dependency injection instead of singletons.
            
            // For this mock, we're providing a way to create mocks of the dependencies,
            // but the actual usage is limited due to the singleton design.
            
            var memoryMonitorMock = new Mock<MemoryPressureMonitor>();
            var memoryManagerMock = new Mock<MemoryManager>();
            var batchManagerMock = new Mock<BatchOperationManager>();
            var perfTrackerMock = new Mock<PerformanceTracker>();
            var lockManagerMock = new Mock<PartitionedLockManager>();
            
            // Get the actual instance - we can't replace it with mocks due to singleton pattern
            var service = InfrastructureService.Instance;
            if (!service.IsInitialized)
            {
                service.Initialize(false);
            }
            
            return (service, memoryMonitorMock, memoryManagerMock, batchManagerMock, 
                   perfTrackerMock, lockManagerMock);
        }
        
        /// <summary>
        /// Creates sample test data for various SQL entities
        /// </summary>
        public static Dictionary<string, List<Dictionary<string, object>>> CreateSampleTableData()
        {
            var data = new Dictionary<string, List<Dictionary<string, object>>>();
            
            // Customers table
            var customers = new List<Dictionary<string, object>>();
            for (int i = 1; i <= 10; i++)
            {
                customers.Add(new Dictionary<string, object>
                {
                    ["CustomerId"] = i,
                    ["CustomerName"] = $"Customer {i}",
                    ["Email"] = $"customer{i}@example.com",
                    ["IsActive"] = i % 3 != 0 // Some inactive customers
                });
            }
            data["Customers"] = customers;
            
            // Orders table
            var orders = new List<Dictionary<string, object>>();
            for (int i = 1; i <= 30; i++)
            {
                int customerId = (i % 10) + 1;
                orders.Add(new Dictionary<string, object>
                {
                    ["OrderId"] = i,
                    ["CustomerId"] = customerId,
                    ["OrderDate"] = DateTime.Now.AddDays(-i % 30),
                    ["Amount"] = 100.0m * (i % 5 + 1)
                });
            }
            data["Orders"] = orders;
            
            // Products table
            var products = new List<Dictionary<string, object>>();
            for (int i = 1; i <= 20; i++)
            {
                products.Add(new Dictionary<string, object>
                {
                    ["ProductId"] = i,
                    ["ProductName"] = $"Product {i}",
                    ["UnitPrice"] = 10.0m * (i % 10 + 1),
                    ["IsDiscontinued"] = i % 7 == 0 // Some discontinued products
                });
            }
            data["Products"] = products;
            
            return data;
        }
    }
}
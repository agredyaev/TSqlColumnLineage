using System;
using System.Linq;
using FluentAssertions;
using TSqlColumnLineage.Core.Domain.Context;
using TSqlColumnLineage.Core.Domain.Graph;
using TSqlColumnLineage.Core.Infrastructure;
using TSqlColumnLineage.Tests.Helpers;
using Xunit;

namespace TSqlColumnLineage.Tests.Integration
{
    /// <summary>
    /// Integration tests that exercise multiple components together
    /// </summary>
    public class LineageTrackingIntegrationTests
    {
        [Fact]
        public void EndToEndLineageTracking_ShouldConstructCorrectGraph()
        {
            // Initialize Infrastructure
            var infrastructure = InfrastructureService.Instance;
            if (!infrastructure.IsInitialized)
            {
                infrastructure.Initialize();
            }
            
            // Create domain objects
            var graph = new LineageGraph();
            var contextManager = new ContextManager(graph);
            var metadataStore = new MetadataStore();

            // Set up metadata
            SetupTestSchema(metadataStore);
            
            // Set up a sample query context
            var sqlQuery = TestFixtures.CreateSampleSqlQuery();
            var queryContext = new QueryContext(contextManager, sqlQuery);

            // Track tables and columns
            RegisterTablesAndColumns(graph, contextManager, metadataStore);

            // Process a sample SQL operation
            ProcessSampleOperation(graph, queryContext);
            
            // Verify the results
            var stats = graph.GetStatistics();
            stats.TotalNodes.Should().BeGreaterThan(5);
            stats.TotalEdges.Should().BeGreaterThan(3);
            
            // Verify specific lineage
            int customersNameColumnId = graph.GetColumnNode("dbo.Customers", "CustomerName");
            customersNameColumnId.Should().BeGreaterThanOrEqualTo(0);
            
            int reportNameColumnId = graph.GetColumnNode("dbo.CustomerReport", "Name");
            reportNameColumnId.Should().BeGreaterThanOrEqualTo(0);
            
            var paths = graph.GetLineagePaths(customersNameColumnId, reportNameColumnId);
            paths.Should().NotBeEmpty();
            
            // First path should be our direct lineage
            var firstPath = paths.First();
            firstPath.SourceId.Should().Be(customersNameColumnId);
            firstPath.TargetId.Should().Be(reportNameColumnId);
            firstPath.Edges.Count.Should().BeGreaterThan(0);
        }
        
        private static void SetupTestSchema(MetadataStore metadataStore)
        {
            // Customers table
            metadataStore.AddTable("dbo.Customers");
            metadataStore.AddColumn("dbo.Customers", "CustomerId", "int", false, true);
            metadataStore.AddColumn("dbo.Customers", "CustomerName", "nvarchar", false);
            metadataStore.AddColumn("dbo.Customers", "Email", "nvarchar", true);
            metadataStore.AddColumn("dbo.Customers", "IsActive", "bit", false);
            
            // Orders table
            metadataStore.AddTable("dbo.Orders");
            metadataStore.AddColumn("dbo.Orders", "OrderId", "int", false, true);
            metadataStore.AddColumn("dbo.Orders", "CustomerId", "int", false);
            metadataStore.AddColumn("dbo.Orders", "OrderDate", "datetime", false);
            metadataStore.AddColumn("dbo.Orders", "Amount", "decimal", false);
            
            // CustomerReport table
            metadataStore.AddTable("dbo.CustomerReport");
            metadataStore.AddColumn("dbo.CustomerReport", "CustomerId", "int", false, true);
            metadataStore.AddColumn("dbo.CustomerReport", "Name", "nvarchar", false);
            metadataStore.AddColumn("dbo.CustomerReport", "OrderCount", "int", false);
            metadataStore.AddColumn("dbo.CustomerReport", "TotalAmount", "decimal", false);
        }
        
        private static void RegisterTablesAndColumns(LineageGraph graph, ContextManager contextManager, MetadataStore metadataStore)
        {
            // Register tables
            foreach (var table in metadataStore.GetAllTables())
            {
                var tableId = graph.AddTableNode(table.Name, "Table");
                contextManager.RegisterTable(table.Name, tableId);
                
                // Register columns
                foreach (var column in metadataStore.GetTableColumns(table.Name))
                {
                    var columnId = graph.AddColumnNode(
                        column.Name, 
                        table.Name, 
                        column.DataType, 
                        column.IsNullable);
                    
                    graph.AddColumnToTable(tableId, columnId);
                }
            }
            
            // Register the CustomerOrders CTE
            var cteId = graph.AddTableNode("CustomerOrders", "CTE");
            contextManager.RegisterCte("CustomerOrders", cteId);
            
            // Register CTE columns
            var cteColumns = new[] 
            {
                ("CustomerId", "int", false),
                ("CustomerName", "nvarchar", false),
                ("OrderCount", "int", false),
                ("TotalAmount", "decimal", false)
            };
            
            foreach (var (name, type, isNullable) in cteColumns)
            {
                var columnId = graph.AddColumnNode(name, "CustomerOrders", type, isNullable);
                graph.AddColumnToTable(cteId, columnId);
            }
        }
        
        private static void ProcessSampleOperation(LineageGraph graph, QueryContext queryContext)
        {
            // Create source columns (from Customers table)
            int customersIdColId = graph.GetColumnNode("dbo.Customers", "CustomerId");
            int customersNameColId = graph.GetColumnNode("dbo.Customers", "CustomerName");
            
            // Create source columns (from Orders table)
            int ordersIdColId = graph.GetColumnNode("dbo.Orders", "OrderId");
            int ordersCustomerIdColId = graph.GetColumnNode("dbo.Orders", "CustomerId");
            int ordersAmountColId = graph.GetColumnNode("dbo.Orders", "Amount");
            
            // Create target columns (in CustomerOrders CTE)
            int cteCustomerIdColId = graph.GetColumnNode("CustomerOrders", "CustomerId");
            int cteCustomerNameColId = graph.GetColumnNode("CustomerOrders", "CustomerName");
            int cteOrderCountColId = graph.GetColumnNode("CustomerOrders", "OrderCount");
            int cteTotalAmountColId = graph.GetColumnNode("CustomerOrders", "TotalAmount");
            
            // Create target columns (in CustomerReport table)
            int reportCustomerIdColId = graph.GetColumnNode("dbo.CustomerReport", "CustomerId");
            int reportNameColId = graph.GetColumnNode("dbo.CustomerReport", "Name");
            int reportOrderCountColId = graph.GetColumnNode("dbo.CustomerReport", "OrderCount");
            int reportTotalAmountColId = graph.GetColumnNode("dbo.CustomerReport", "TotalAmount");
            
            // Create aggregation expressions
            int countExpressionId = graph.AddExpressionNode(
                "CountOrders", 
                "COUNT(OrderId)", 
                "Aggregation", 
                "int",
                "CustomerOrders");
                
            int sumExpressionId = graph.AddExpressionNode(
                "SumAmount",
                "SUM(Amount)",
                "Aggregation",
                "decimal",
                "CustomerOrders");
            
            // Create lineage for CTE part
            graph.AddDirectLineage(customersIdColId, cteCustomerIdColId, "SELECT", "CTE");
            graph.AddDirectLineage(customersNameColId, cteCustomerNameColId, "SELECT", "CTE");
            graph.AddDirectLineage(ordersIdColId, countExpressionId, "Aggregation", "COUNT(OrderId)");
            graph.AddDirectLineage(countExpressionId, cteOrderCountColId, "SELECT", "CTE");
            graph.AddDirectLineage(ordersAmountColId, sumExpressionId, "Aggregation", "SUM(Amount)");
            graph.AddDirectLineage(sumExpressionId, cteTotalAmountColId, "SELECT", "CTE");
            
            // Create join relationship
            graph.AddJoinRelationship(customersIdColId, ordersCustomerIdColId, "INNER JOIN");
            
            // Create lineage for INSERT part
            graph.AddDirectLineage(cteCustomerIdColId, reportCustomerIdColId, "INSERT", "INSERT");
            graph.AddDirectLineage(cteCustomerNameColId, reportNameColId, "INSERT", "INSERT");
            graph.AddDirectLineage(cteOrderCountColId, reportOrderCountColId, "INSERT", "INSERT");
            graph.AddDirectLineage(cteTotalAmountColId, reportTotalAmountColId, "INSERT", "INSERT");
            
            // Register outputs in query context
            queryContext.AddOutputColumn("dbo.CustomerReport", reportCustomerIdColId);
            queryContext.AddOutputColumn("dbo.CustomerReport", reportNameColId);
            queryContext.AddOutputColumn("dbo.CustomerReport", reportOrderCountColId);
            queryContext.AddOutputColumn("dbo.CustomerReport", reportTotalAmountColId);
            
            // Register inputs in query context
            queryContext.AddInputTable("dbo.Customers", graph.GetTableColumns(graph.GetColumnNode("dbo.Customers", "CustomerId"))[0]);
            queryContext.AddInputTable("dbo.Orders", graph.GetTableColumns(graph.GetColumnNode("dbo.Orders", "OrderId"))[0]);
        }
    }
}
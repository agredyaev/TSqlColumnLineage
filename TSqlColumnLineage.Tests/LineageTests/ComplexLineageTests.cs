using Microsoft.VisualStudio.TestTools.UnitTesting;
using TSqlColumnLineage.Core;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Parsing;
using TSqlColumnLineage.Core.Services;
using TSqlColumnLineage.Core.Visitors;
using System.Linq;

namespace TSqlColumnLineage.Tests.LineageTests
{
    [TestClass]
    public class ComplexLineageTests
    {
        private LineageService _lineageService;
        private SqlParser? _sqlParser;
        private LineageNodeFactory _nodeFactory;
        private TSqlColumnLineage.Core.LineageEdgeFactory _edgeFactory;
        private IMetadataService? _metadataService;
        private IGraphService? _graphService;
        private ILogger _logger;

        [TestInitialize]
        public void Initialize()
        {
            _nodeFactory = new LineageNodeFactory();
            _edgeFactory = new Core.LineageEdgeFactory();
            _sqlParser = new SqlParser();
            _logger = new ConsoleLogger();
            _metadataService = new MetadataService();
            _graphService = new GraphService(_nodeFactory, _edgeFactory);
            _lineageService = new LineageService(_sqlParser, _metadataService, _graphService, _logger, _nodeFactory, _edgeFactory);
        }

        [TestMethod]
        public void SelectWithCTE_ShouldCreateCorrectLineage()
        {
            // Arrange
            string sql = @"
                WITH CustomerOrders AS (
                    SELECT 
                        c.CustomerID,
                        c.FirstName,
                        c.LastName,
                        o.OrderDate,
                        o.OrderID
                    FROM 
                        Customers c
                    JOIN 
                        Orders o ON c.CustomerID = o.CustomerID
                )
                SELECT 
                    CustomerID,
                    FirstName,
                    LastName,
                    COUNT(OrderID) AS TotalOrders
                FROM 
                    CustomerOrders
                GROUP BY 
                    CustomerID, FirstName, LastName";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check if CTE is recognized as a table
            var cteTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("CustomerOrders"));
            
            Assert.IsNotNull(cteTable, "CTE should be recognized as a table");
            Assert.AreEqual("CTE", cteTable.TableType, "Table type should be CTE");
            
            // Check if source tables exist
            var customersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Customers"));
            var ordersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Orders"));
            
            Assert.IsNotNull(customersTable, "Customers table should exist");
            Assert.IsNotNull(ordersTable, "Orders table should exist");
            
            // Check lineage from source table columns to CTE columns
            var customerIdColumns = lineageGraph.Nodes.OfType<ColumnNode>()
                .Where(n => n.Name.Equals("CustomerID"))
                .ToList();
            
            // Should have at least two CustomerID columns (one from Customers, one from CTE)
            Assert.IsTrue(customerIdColumns.Count >= 2, "Should have CustomerID columns from multiple tables");
            
            // Check for the existence of the calculated TotalOrders column
            var totalOrdersColumn = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.ObjectName.Contains("COUNT") && n.ObjectName.Contains("OrderID"));
            
            Assert.IsNotNull(totalOrdersColumn, "TotalOrders expression node should exist");
        }

        [TestMethod]
        public void SelectWithSubquery_ShouldCreateCorrectLineage()
        {
            // Arrange
            string sql = @"
                SELECT 
                    p.ProductID,
                    p.ProductName,
                    p.UnitPrice,
                    (SELECT AVG(UnitPrice) FROM Products) AS AveragePrice,
                    p.UnitPrice - (SELECT AVG(UnitPrice) FROM Products) AS PriceDifference
                FROM 
                    Products p";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for the existence of products table
            var productsTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Products"));
            
            Assert.IsNotNull(productsTable, "Products table should exist");
            
            // Check for the existence of the expression nodes for subqueries
            var avgPriceNode = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.ObjectName.Contains("AVG") && n.ObjectName.Contains("UnitPrice"));
            
            Assert.IsNotNull(avgPriceNode, "Average price expression node should exist");
            
            // Check for the existence of the expression node for price difference
            var priceDiffNode = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.ObjectName.Contains("-") && n.ObjectName.Contains("UnitPrice"));
            
            Assert.IsNotNull(priceDiffNode, "Price difference expression node should exist");
            
            // Verify that the priceDiffNode has an edge from the avgPriceNode
            var edgeFromAvgToDiff = lineageGraph.Edges
                .FirstOrDefault(e => e.SourceId == avgPriceNode.Id && e.TargetId == priceDiffNode.Id);
            
            Assert.IsNotNull(edgeFromAvgToDiff, "Should have an edge from average price to price difference");
        }

        [TestMethod]
        public void SelectWithPivot_ShouldCreateCorrectLineage()
        {
            // Arrange
            string sql = @"
                SELECT 
                    ProductID,
                    [2022] AS Sales2022,
                    [2023] AS Sales2023,
                    [2024] AS Sales2024
                FROM (
                    SELECT 
                        ProductID, 
                        Year, 
                        SalesAmount
                    FROM 
                        ProductSales
                ) AS SourceTable
                PIVOT (
                    SUM(SalesAmount)
                    FOR Year IN ([2022], [2023], [2024])
                ) AS PivotTable";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for the existence of source table
            var productSalesTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("ProductSales"));
            
            Assert.IsNotNull(productSalesTable, "ProductSales table should exist");
            
            // Check for derived pivot columns
            var pivotColumns = lineageGraph.Nodes.OfType<ColumnNode>()
                .Where(n => n.Name.Equals("Sales2022") || n.Name.Equals("Sales2023") || n.Name.Equals("Sales2024"))
                .ToList();
            
            Assert.AreEqual(3, pivotColumns.Count, "Should have 3 pivot result columns");
            
            // Check for source columns
            var yearColumn = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("Year") && n.TableOwner.Equals("ProductSales"));
            var salesAmountColumn = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("SalesAmount") && n.TableOwner.Equals("ProductSales"));
            
            Assert.IsNotNull(yearColumn, "Year column should exist in source table");
            Assert.IsNotNull(salesAmountColumn, "SalesAmount column should exist in source table");
            
            // Verify edges from source to pivot columns
            foreach (var pivotColumn in pivotColumns)
            {
                // Each pivot column should have edges from both Year and SalesAmount
                var edges = lineageGraph.Edges
                    .Where(e => e.TargetId == pivotColumn.Id &&
                               (e.SourceId == yearColumn.Id || e.SourceId == salesAmountColumn.Id))
                    .ToList();
                
                Assert.IsTrue(edges.Count >= 1, $"{pivotColumn.Name} should have at least one edge from source columns");
            }
        }

        [TestMethod]
        public void StoredProcedureWithOutputParameter_ShouldCreateCorrectLineage()
        {
            // Arrange
            string sql = @"
                CREATE PROCEDURE CalculateCustomerStats
                    @CustomerID INT,
                    @TotalOrders INT OUTPUT,
                    @TotalAmount DECIMAL(18,2) OUTPUT
                AS
                BEGIN
                    SELECT 
                        @TotalOrders = COUNT(OrderID),
                        @TotalAmount = SUM(TotalAmount)
                    FROM 
                        Orders
                    WHERE 
                        CustomerID = @CustomerID
                END";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for the stored procedure as an object
            var procNode = lineageGraph.Nodes
                .FirstOrDefault(n => n.ObjectName.Equals("CalculateCustomerStats"));
            
            Assert.IsNotNull(procNode, "Stored procedure node should exist");
            
            // Check for parameter nodes
            var inputParam = lineageGraph.Nodes
                .FirstOrDefault(n => n.Name.Contains("CustomerID") && n.ObjectName.Contains("@CustomerID"));
            
            var outputParams = lineageGraph.Nodes
                .Where(n => n.Name.Contains("TotalOrders") || n.Name.Contains("TotalAmount"))
                .ToList();
            
            Assert.IsNotNull(inputParam, "Input parameter node should exist");
            Assert.AreEqual(2, outputParams.Count, "Should have 2 output parameter nodes");
            
            // Check for source table
            var ordersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Orders"));
            
            Assert.IsNotNull(ordersTable, "Orders table node should exist");
            
            // Check for lineage edges between table columns and output parameters
            var tableColumns = lineageGraph.Nodes.OfType<ColumnNode>()
                .Where(n => n.TableOwner.Equals("Orders"))
                .ToList();
            
            Assert.IsTrue(tableColumns.Count >= 2, "Should have at least OrderID and TotalAmount columns");
            
            // Verify there's an edge to at least one of the output parameters
            var anyEdgeToOutput = lineageGraph.Edges
                .Any(e => outputParams.Any(op => op.Id == e.TargetId) && 
                         tableColumns.Any(tc => tc.Id == e.SourceId));
            
            Assert.IsTrue(anyEdgeToOutput, "Should have at least one edge from table column to output parameter");
        }
    }
}

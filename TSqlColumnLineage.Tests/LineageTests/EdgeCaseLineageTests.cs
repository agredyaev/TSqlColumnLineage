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
    public class EdgeCaseLineageTests
    {
        private LineageService _lineageService;
        private SqlParser _sqlParser;
        private LineageNodeFactory _nodeFactory;
        private TSqlColumnLineage.Core.LineageEdgeFactory _edgeFactory;
        private IMetadataService _metadataService;
        private IGraphService _graphService;
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
        public void CrossApply_ShouldCreateCorrectLineage()
        {
            // Arrange
            string sql = @"
                SELECT 
                    c.CustomerID,
                    c.CustomerName,
                    o.OrderID,
                    o.OrderDate
                FROM 
                    Customers c
                CROSS APPLY (
                    SELECT TOP 3 
                        OrderID, 
                        OrderDate 
                    FROM 
                        Orders 
                    WHERE 
                        CustomerID = c.CustomerID 
                    ORDER BY 
                        OrderDate DESC
                ) o";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check if tables exist
            var customersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Customers"));
            var ordersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Orders"));
            
            Assert.IsNotNull(customersTable, "Customers table should exist");
            Assert.IsNotNull(ordersTable, "Orders table should exist");
            
            // Check for cross apply correlation
            var custIdFromCustomers = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("CustomerID") && n.TableOwner.Equals("Customers"));
            var custIdFromOrders = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("CustomerID") && n.TableOwner.Equals("Orders"));
            
            Assert.IsNotNull(custIdFromCustomers, "CustomerID column from Customers should exist");
            Assert.IsNotNull(custIdFromOrders, "CustomerID column from Orders should exist");
            
            // Check for existence of correlation edge
            var correlationEdge = lineageGraph.Edges
                .FirstOrDefault(e => e.SourceId == custIdFromCustomers.Id && e.TargetId == custIdFromOrders.Id);
            
            Assert.IsNotNull(correlationEdge, "Should have an edge between correlated CustomerID columns");
        }

        [TestMethod]
        public void OuterApply_ShouldCreateCorrectLineage()
        {
            // Arrange
            string sql = @"
                SELECT 
                    c.CustomerID,
                    c.CustomerName,
                    AveragePurchase = ISNULL(s.AvgAmount, 0)
                FROM 
                    Customers c
                OUTER APPLY (
                    SELECT 
                        AVG(Amount) AS AvgAmount
                    FROM 
                        Sales
                    WHERE 
                        CustomerID = c.CustomerID
                ) s";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check tables
            var customersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Customers"));
            var salesTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Sales"));
            
            Assert.IsNotNull(customersTable, "Customers table should exist");
            Assert.IsNotNull(salesTable, "Sales table should exist");
            
            // Check for the derived column
            var avgAmountExpr = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.ObjectName.Contains("AVG") && n.ObjectName.Contains("Amount"));
            
            Assert.IsNotNull(avgAmountExpr, "Average amount expression should exist");
            
            // Check for the ISNULL expression
            var isnullExpr = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.ObjectName.Contains("ISNULL") && n.ObjectName.Contains("AvgAmount"));
            
            Assert.IsNotNull(isnullExpr, "ISNULL expression should exist");
            
            // Check lineage from avgAmount to isnull
            var exprEdge = lineageGraph.Edges
                .FirstOrDefault(e => e.SourceId == avgAmountExpr.Id && e.TargetId == isnullExpr.Id);
            
            Assert.IsNotNull(exprEdge, "Should have an edge from AVG expression to ISNULL expression");
        }

        [TestMethod]
        public void ComplexCaseExpression_ShouldCreateCorrectLineage()
        {
            // Arrange
            string sql = @"
                SELECT 
                    ProductID,
                    ProductName,
                    UnitPrice,
                    CASE 
                        WHEN UnitPrice > 100 THEN 'Premium'
                        WHEN UnitPrice > 50 THEN 'Standard'
                        WHEN UnitPrice > 20 THEN 'Economy'
                        ELSE 'Budget'
                    END AS PriceCategory,
                    CASE ProductCategory
                        WHEN 1 THEN 'Electronics'
                        WHEN 2 THEN 'Clothing'
                        WHEN 3 THEN 'Food'
                        ELSE 'Other'
                    END AS CategoryName
                FROM 
                    Products";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for Products table
            var productsTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Products"));
            
            Assert.IsNotNull(productsTable, "Products table should exist");
            
            // Check for UnitPrice column
            var unitPriceColumn = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("UnitPrice") && n.TableOwner.Equals("Products"));
            
            Assert.IsNotNull(unitPriceColumn, "UnitPrice column should exist");
            
            // Check for CASE expressions
            var caseExpressions = lineageGraph.Nodes.OfType<ExpressionNode>()
                .Where(n => n.ObjectName.Contains("CASE"))
                .ToList();
            
            Assert.AreEqual(2, caseExpressions.Count, "Should have 2 CASE expressions");
            
            // Check for edge from UnitPrice to first CASE
            var unitPriceCaseExpr = caseExpressions
                .FirstOrDefault(n => n.ObjectName.Contains("UnitPrice"));
            
            Assert.IsNotNull(unitPriceCaseExpr, "CASE expression with UnitPrice should exist");
            
            var unitPriceToCaseEdge = lineageGraph.Edges
                .FirstOrDefault(e => e.SourceId == unitPriceColumn.Id && e.TargetId == unitPriceCaseExpr.Id);
            
            Assert.IsNotNull(unitPriceToCaseEdge, "Should have an edge from UnitPrice to CASE expression");
        }

        [TestMethod]
        public void TempTable_ShouldCreateCorrectLineage()
        {
            // Arrange
            string sql = @"
                -- Create temp table
                CREATE TABLE #TempCustomers (
                    CustomerID INT,
                    CustomerName VARCHAR(100),
                    TotalOrders INT
                );
                
                -- Insert data
                INSERT INTO #TempCustomers (CustomerID, CustomerName, TotalOrders)
                SELECT 
                    c.CustomerID,
                    c.CustomerName,
                    COUNT(o.OrderID) AS TotalOrders
                FROM 
                    Customers c
                LEFT JOIN 
                    Orders o ON c.CustomerID = o.CustomerID
                GROUP BY 
                    c.CustomerID, c.CustomerName;
                
                -- Query temp table
                SELECT 
                    t.CustomerID,
                    t.CustomerName,
                    t.TotalOrders,
                    CASE 
                        WHEN t.TotalOrders > 10 THEN 'High Value'
                        WHEN t.TotalOrders > 5 THEN 'Medium Value'
                        ELSE 'Low Value'
                    END AS CustomerValue
                FROM 
                    #TempCustomers t
                WHERE 
                    t.TotalOrders > 0";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for temp table
            var tempTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("#TempCustomers"));
            
            Assert.IsNotNull(tempTable, "Temp table should exist");
            Assert.AreEqual("TempTable", tempTable.TableType, "Table type should be TempTable");
            
            // Check for source tables
            var customersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Customers"));
            var ordersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Orders"));
            
            Assert.IsNotNull(customersTable, "Customers table should exist");
            Assert.IsNotNull(ordersTable, "Orders table should exist");
            
            // Check for temp table columns
            var tempTableCols = lineageGraph.Nodes.OfType<ColumnNode>()
                .Where(n => n.TableOwner.Equals("#TempCustomers"))
                .ToList();
            
            Assert.AreEqual(3, tempTableCols.Count, "Temp table should have 3 columns");
            
            // Check for lineage from source to temp
            var custIdInCustomers = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("CustomerID") && n.TableOwner.Equals("Customers"));
            var custIdInTemp = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("CustomerID") && n.TableOwner.Equals("#TempCustomers"));
            
            Assert.IsNotNull(custIdInCustomers, "CustomerID in Customers should exist");
            Assert.IsNotNull(custIdInTemp, "CustomerID in temp table should exist");
            
            // Check for edge between them
            var custIdEdge = lineageGraph.Edges
                .FirstOrDefault(e => e.SourceId == custIdInCustomers.Id && e.TargetId == custIdInTemp.Id);
            
            Assert.IsNotNull(custIdEdge, "Should have an edge from Customers.CustomerID to #TempCustomers.CustomerID");
            
            // Check for CASE expression
            var caseExpr = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.ObjectName.Contains("CASE") && n.ObjectName.Contains("TotalOrders"));
            
            Assert.IsNotNull(caseExpr, "CASE expression should exist");
            
            // Check for edge from temp.TotalOrders to CASE
            var totalOrdersInTemp = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("TotalOrders") && n.TableOwner.Equals("#TempCustomers"));
            
            Assert.IsNotNull(totalOrdersInTemp, "TotalOrders in temp table should exist");
            
            var totalOrdersToCaseEdge = lineageGraph.Edges
                .FirstOrDefault(e => e.SourceId == totalOrdersInTemp.Id && e.TargetId == caseExpr.Id);
            
            Assert.IsNotNull(totalOrdersToCaseEdge, "Should have an edge from TotalOrders to CASE expression");
        }
    }
}

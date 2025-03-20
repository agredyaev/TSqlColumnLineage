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
    public class BasicLineageTests
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
            _metadataService = new MetadataService(); // Assuming you have a concrete implementation
            _graphService = new GraphService(_nodeFactory, _edgeFactory);
            _lineageService = new LineageService(_sqlParser, _metadataService, _graphService, _logger, _nodeFactory, _edgeFactory);
        }

        [TestMethod]
        public void SimpleSelect_ShouldCreateDirectEdges()
        {
            // Arrange
            string sql = @"
                SELECT 
                    CustomerID, 
                    FirstName, 
                    LastName 
                FROM 
                    Customers";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Find table node
            var tableNode = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Customers"));
            
            Assert.IsNotNull(tableNode, "Customers table node should exist");
            
            // Find column nodes and verify they were created
            var columnNodes = lineageGraph.Nodes.OfType<ColumnNode>()
                .Where(n => n.TableOwner.Equals("Customers"))
                .ToList();
            
            Assert.AreEqual(4, columnNodes.Count, "Should have 4 column nodes");
            
            // Verify column names
            Assert.IsTrue(columnNodes.Any(n => n.Name.Equals("CustomerID")), "CustomerID column should exist");
            Assert.IsTrue(columnNodes.Any(n => n.Name.Equals("FirstName")), "FirstName column should exist");
            Assert.IsTrue(columnNodes.Any(n => n.Name.Equals("LastName")), "LastName column should exist");
            Assert.IsTrue(columnNodes.Any(n => n.Name.Equals("CustomerName")), "CustomerName column should exist");
            
            // Verify edges
            Assert.IsTrue(lineageGraph.Edges.Any(), "Lineage graph should have edges");
        }

        [TestMethod]
        public void SelectWithAlias_ShouldCreateCorrectTableAndColumnNodes()
        {
            // Arrange
            string sql = @"
                SELECT 
                    c.CustomerID, 
                    c.FirstName, 
                    c.LastName 
                FROM 
                    Customers AS c";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Find table node
            var tableNode = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Customers"));
            
            Assert.IsNotNull(tableNode, "Customers table node should exist");
            Assert.AreEqual("c", tableNode.Alias, "Table alias should be 'c'");
            
            // Find column nodes
            var columnNodes = lineageGraph.Nodes.OfType<ColumnNode>()
                .Where(n => n.TableOwner.Equals("Customers"))
                .ToList();
            
            Assert.AreEqual(3, columnNodes.Count, "Should have 3 column nodes");
            
            // Verify column names
            Assert.IsTrue(columnNodes.Any(n => n.Name.Equals("CustomerID")), "CustomerID column should exist");
            Assert.IsTrue(columnNodes.Any(n => n.Name.Equals("FirstName")), "FirstName column should exist");
            Assert.IsTrue(columnNodes.Any(n => n.Name.Equals("LastName")), "LastName column should exist");
        }

        [TestMethod]
        public void SelectWithJoin_ShouldCreateCorrectLineage()
        {
            // Arrange
            string sql = @"
                SELECT 
                    c.CustomerID, 
                    c.FirstName, 
                    c.LastName,
                    o.OrderID,
                    o.OrderDate
                FROM 
                    Customers c
                JOIN 
                    Orders o ON c.CustomerID = o.CustomerID";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Find table nodes
            var customerTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Customers"));
            var orderTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Orders"));
            
            Assert.IsNotNull(customerTable, "Customers table node should exist");
            Assert.IsNotNull(orderTable, "Orders table node should exist");
            
            // Find all column nodes
            var columnNodes = lineageGraph.Nodes.OfType<ColumnNode>().ToList();
            
            // Verify customer columns
            var customerColumns = columnNodes.Where(n => n.TableOwner.Equals("Customers")).ToList();
            Assert.AreEqual(3, customerColumns.Count, "Should have 3 customer columns");
            
            // Verify order columns
            var orderColumns = columnNodes.Where(n => n.TableOwner.Equals("Orders")).ToList();
            Assert.AreEqual(2, orderColumns.Count, "Should have 2 order columns");
            
            // Check join condition
            var customerIdColumn = columnNodes.FirstOrDefault(n => 
                n.TableOwner.Equals("Customers") && n.Name.Equals("CustomerID"));
            var orderCustomerIdColumn = columnNodes.FirstOrDefault(n => 
                n.TableOwner.Equals("Orders") && n.Name.Equals("CustomerID"));
            
            Assert.IsNotNull(customerIdColumn, "CustomerID column should exist in Customers table");
            Assert.IsNotNull(orderCustomerIdColumn, "CustomerID column should exist in Orders table");
            
            // Edge count check
            Assert.IsTrue(lineageGraph.Edges.Count >= 5, "Should have at least 5 edges for direct column references");
        }

        [TestMethod]
        public void SelectWithColumnCalculation_ShouldCreateExpressionNode()
        {
            // Arrange
            string sql = @"
                SELECT 
                    ProductID, 
                    ProductName, 
                    UnitPrice * Quantity AS TotalPrice
                FROM 
                    Products";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Find expression node
            var expressionNode = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.ObjectName.Contains("UnitPrice") && n.ObjectName.Contains("Quantity"));
            
            Assert.IsNotNull(expressionNode, "Expression node for calculation should exist");
            
            // Verify source columns for the expression
            var unitPriceColumn = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("UnitPrice"));
            var quantityColumn = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("Quantity"));
            
            Assert.IsNotNull(unitPriceColumn, "UnitPrice column should exist");
            Assert.IsNotNull(quantityColumn, "Quantity column should exist");
            
            // Verify edges from source columns to expression
            var edgesToExpression = lineageGraph.Edges
                .Where(e => e.TargetId == expressionNode.Id)
                .ToList();
            
            Assert.AreEqual(2, edgesToExpression.Count, "Should have 2 edges to the expression node");
            
            // Verify edge type is "indirect" since it's a calculation
            Assert.IsTrue(edgesToExpression.All(e => e.Type == "indirect"), "Edges to expression should be indirect type");
        }
    }
}

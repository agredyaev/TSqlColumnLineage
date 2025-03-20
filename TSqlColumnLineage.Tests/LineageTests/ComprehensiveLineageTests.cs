using Microsoft.VisualStudio.TestTools.UnitTesting;
using TSqlColumnLineage.Core;
using TSqlColumnLineage.Core.Models;
using TSqlColumnLineage.Core.Parsing;
using TSqlColumnLineage.Core.Services;
using TSqlColumnLineage.Core.Visitors;
using System;
using System.Linq;
using System.Collections.Generic;

namespace TSqlColumnLineage.Tests.LineageTests
{
    [TestClass]
    public class ComprehensiveLineageTests
    {
        private LineageService _lineageService;
        private SqlParser _sqlParser;
        private LineageNodeFactory _nodeFactory;
        private TSqlColumnLineage.Core.LineageEdgeFactory _edgeFactory;
        private MetadataService _metadataService;
        private IGraphService _graphService;
        private ConsoleLogger _logger;

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
        public void NestedSubqueries_ShouldTraceCorrectLineage()
        {
            // Arrange
            string sql = @"
                SELECT 
                    CustomerID,
                    (SELECT COUNT(*) 
                     FROM Orders o 
                     WHERE o.CustomerID = c.CustomerID) AS OrderCount,
                    (SELECT MAX(o.OrderDate) 
                     FROM Orders o 
                     WHERE o.CustomerID = c.CustomerID) AS LastOrderDate,
                    (SELECT SUM(
                        (SELECT SUM(od.Quantity * od.UnitPrice) 
                         FROM OrderDetails od 
                         WHERE od.OrderID = o.OrderID)
                     ) 
                     FROM Orders o 
                     WHERE o.CustomerID = c.CustomerID) AS TotalSpent
                FROM 
                    Customers c
                WHERE 
                    c.CustomerID IN (SELECT TOP 10 CustomerID FROM Orders GROUP BY CustomerID ORDER BY COUNT(*) DESC)";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for key tables
            var customersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Customers"));
            var ordersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Orders"));
            
            Assert.IsNotNull(customersTable, "Customers table should exist");
            Assert.IsNotNull(ordersTable, "Orders table should exist");
            
            // Count expression nodes (for COUNT, MAX, SUM operations)
            var expressionNodes = lineageGraph.Nodes.OfType<ExpressionNode>().ToList();
            Assert.IsTrue(expressionNodes.Count >= 4, "Should have at least 4 expression nodes for the aggregate operations");
            
            // Check for edges connecting the nested queries
            var edges = lineageGraph.Edges.ToList();
            Assert.IsTrue(edges.Count >= 4, "Should have sufficient edges for the nested relationships");
        }

        [TestMethod]
        public void UnionAndIntersect_ShouldTraceCorrectLineage()
        {
            // Arrange
            string sql = @"
                SELECT CustomerID, FirstName, LastName, 'Current' AS Status
                FROM CurrentCustomers
                UNION ALL
                SELECT CustomerID, FirstName, LastName, 'Former' AS Status
                FROM FormerCustomers
                INTERSECT
                SELECT CustomerID, FirstName, LastName, Status
                FROM AllCustomers
                WHERE Region = 'North'";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for set operation tables
            var currentCustomersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("CurrentCustomers"));
            var formerCustomersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("FormerCustomers"));
            var allCustomersTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("AllCustomers"));
            
            // Check for columns from each table
            var currentCustomerCols = lineageGraph.Nodes.OfType<ColumnNode>()
                .Where(n => n.TableOwner.Equals("CurrentCustomers")).ToList();
            var formerCustomerCols = lineageGraph.Nodes.OfType<ColumnNode>()
                .Where(n => n.TableOwner.Equals("FormerCustomers")).ToList();
            var allCustomerCols = lineageGraph.Nodes.OfType<ColumnNode>()
                .Where(n => n.TableOwner.Equals("AllCustomers")).ToList();
            
            // Verify the presence of literals as expression nodes
            var literalExpressions = lineageGraph.Nodes.OfType<ExpressionNode>()
                .Where(n => 
                    (n.Expression != null) && 
                    (n.Expression.Contains("'Current'") || n.Expression.Contains("'Former'"))
                ).ToList();
            
            Assert.IsTrue(literalExpressions.Any(), "Should have at least one literal expression node");
        }
        
        [TestMethod]
        public void WindowFunctions_ShouldTraceCorrectLineage()
        {
            // Arrange
            string sql = @"
                SELECT 
                    e.EmployeeID,
                    e.FirstName,
                    e.LastName,
                    e.Salary,
                    AVG(e.Salary) OVER (PARTITION BY e.DepartmentID) AS AvgDeptSalary,
                    SUM(e.Salary) OVER (PARTITION BY e.DepartmentID) AS TotalDeptSalary,
                    RANK() OVER (PARTITION BY e.DepartmentID ORDER BY e.Salary DESC) AS SalaryRank,
                    ROW_NUMBER() OVER (PARTITION BY e.DepartmentID ORDER BY e.HireDate) AS SeniorityRank,
                    LAG(e.Salary, 1, 0) OVER (PARTITION BY e.DepartmentID ORDER BY e.Salary) AS PrevSalary,
                    LEAD(e.Salary, 1, 0) OVER (PARTITION BY e.DepartmentID ORDER BY e.Salary) AS NextSalary
                FROM 
                    Employees e
                WHERE 
                    e.Status = 'Active'";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for key table
            var employeesTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Employees"));
            
            Assert.IsNotNull(employeesTable, "Employees table should exist");
            
            // Check for salary column
            var salaryColumn = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("Salary") && n.TableOwner.Equals("Employees"));
            
            Assert.IsNotNull(salaryColumn, "Salary column should exist");
            
            // Check for window function expression nodes
            var windowFunctions = lineageGraph.Nodes.OfType<ExpressionNode>()
                .Where(n => n.Expression?.Contains("OVER") == true).ToList();
            
            Assert.IsTrue(windowFunctions.Count >= 5, "Should have at least 5 window function expressions");
            
            // Check for edges from salary to window functions
            var edgesFromSalary = lineageGraph.Edges
                .Where(e => e.SourceId == salaryColumn.Id).ToList();
            
            Assert.IsTrue(edgesFromSalary.Count >= 3, "Should have sufficient edges from Salary column to window functions");
        }
        
        [TestMethod]
        public void DynamicSQL_ShouldTraceCorrectLineage()
        {
            // Arrange
            string sql = @"
                DECLARE @SQLQuery NVARCHAR(1000)
                DECLARE @ColumnList NVARCHAR(500)
                DECLARE @TableName NVARCHAR(100) = 'Customers'
                
                SET @ColumnList = 'CustomerID, CustomerName, City, Country'
                SET @SQLQuery = 'SELECT ' + @ColumnList + ' FROM ' + @TableName + ' WHERE Status = ''Active'''
                
                EXEC sp_executesql @SQLQuery";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for variable nodes
            var variables = lineageGraph.Nodes
                .Where(n => n.Name.Contains("@")).ToList();
            
            Assert.IsTrue(variables.Count >= 3, "Should have at least 3 variable nodes");
            
            // Check for edges between variables
            var variableEdges = lineageGraph.Edges
                .Where(e => lineageGraph.GetNodeById(e.SourceId)?.Name.Contains("@") == true || 
                            lineageGraph.GetNodeById(e.TargetId)?.Name.Contains("@") == true).ToList();
            
            Assert.IsTrue(variableEdges.Count >= 2, "Should have edges connecting variable assignments");
        }
        
        [TestMethod]
        public void RecursiveCTE_ShouldTraceCorrectLineage()
        {
            // Arrange
            string sql = @"
                WITH EmployeeHierarchy AS (
                    -- Base case: Manager is NULL (top level)
                    SELECT 
                        EmployeeID, 
                        FirstName, 
                        LastName, 
                        ManagerID, 
                        0 AS Level
                    FROM 
                        Employees
                    WHERE 
                        ManagerID IS NULL
                        
                    UNION ALL
                    
                    -- Recursive case: Join to find subordinates
                    SELECT 
                        e.EmployeeID, 
                        e.FirstName, 
                        e.LastName, 
                        e.ManagerID, 
                        eh.Level + 1
                    FROM 
                        Employees e
                    INNER JOIN 
                        EmployeeHierarchy eh ON e.ManagerID = eh.EmployeeID
                )
                SELECT 
                    EmployeeID,
                    FirstName,
                    LastName,
                    Level,
                    REPLICATE('--', Level) + FirstName + ' ' + LastName AS EmployeePath
                FROM 
                    EmployeeHierarchy
                ORDER BY 
                    Level, FirstName";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for CTE and source table
            var hierachyCTE = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("EmployeeHierarchy"));
            var employeesTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Employees"));
            
            Assert.IsNotNull(hierachyCTE, "EmployeeHierarchy CTE should exist");
            Assert.IsNotNull(employeesTable, "Employees table should exist");
            
            // Check for the calculated Level column
            var levelColumns = lineageGraph.Nodes.OfType<ColumnNode>()
                .Where(n => n.Name.Equals("Level")).ToList();
            
            Assert.IsTrue(levelColumns.Count >= 1, "Level column should exist");
            
            // Check for the REPLICATE expression
            var replicateExpr = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.Expression?.Contains("REPLICATE") == true);
            
            Assert.IsNotNull(replicateExpr, "REPLICATE expression should exist");
            
            // Check for self-referencing edges (recursion)
            var selfRefEdges = lineageGraph.Edges
                .Where(e => {
                    var sourceNode = lineageGraph.GetNodeById(e.SourceId) as ColumnNode;
                    var targetNode = lineageGraph.GetNodeById(e.TargetId) as ColumnNode;
                    
                    return sourceNode?.TableOwner == "EmployeeHierarchy" && 
                           targetNode?.TableOwner == "EmployeeHierarchy";
                }).ToList();
            
            Assert.IsTrue(selfRefEdges.Count >= 1, "Should have self-referencing edges for recursion");
        }
        
        [TestMethod]
        public void FullDMLStatement_ShouldTraceCorrectLineage()
        {
            // Arrange - Testing all DML operations in sequence
            string sql = @"
                -- Create a temporary table
                CREATE TABLE #SalesReport (
                    ProductID INT,
                    ProductName VARCHAR(100),
                    CategoryName VARCHAR(50),
                    TotalQuantity INT,
                    TotalRevenue DECIMAL(18,2),
                    UpdateDate DATETIME DEFAULT GETDATE()
                );
                
                -- INSERT from multiple sources
                INSERT INTO #SalesReport (ProductID, ProductName, CategoryName, TotalQuantity, TotalRevenue)
                SELECT 
                    p.ProductID,
                    p.ProductName,
                    c.CategoryName,
                    SUM(od.Quantity) AS TotalQuantity,
                    SUM(od.Quantity * od.UnitPrice) AS TotalRevenue
                FROM 
                    Products p
                INNER JOIN 
                    Categories c ON p.CategoryID = c.CategoryID
                INNER JOIN 
                    [Order Details] od ON p.ProductID = od.ProductID
                INNER JOIN 
                    Orders o ON od.OrderID = o.OrderID
                WHERE 
                    o.OrderDate >= DATEADD(YEAR, -1, GETDATE())
                GROUP BY 
                    p.ProductID, p.ProductName, c.CategoryName;
                
                -- UPDATE with calculated values
                UPDATE #SalesReport
                SET 
                    TotalRevenue = TotalRevenue * 1.1,
                    UpdateDate = GETDATE()
                WHERE 
                    CategoryName = 'Beverages';
                
                -- DELETE based on condition
                DELETE FROM #SalesReport
                WHERE TotalQuantity < 10;
                
                -- SELECT final results
                SELECT * FROM #SalesReport
                ORDER BY TotalRevenue DESC;";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Check for temp table
            var tempTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("#SalesReport"));
            
            Assert.IsNotNull(tempTable, "#SalesReport temp table should exist");
            Assert.AreEqual("TempTable", tempTable.TableType, "Table type should be TempTable");
            
            // Check for source tables
            var productsTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Products"));
            var categoriesTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("Categories"));
            
            Assert.IsNotNull(productsTable, "Products table should exist");
            Assert.IsNotNull(categoriesTable, "Categories table should exist");
            
            // Check for calculated columns
            var totalQuantityCol = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("TotalQuantity") && n.TableOwner.Equals("#SalesReport"));
            var totalRevenueCol = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("TotalRevenue") && n.TableOwner.Equals("#SalesReport"));
            
            Assert.IsNotNull(totalQuantityCol, "TotalQuantity column should exist");
            Assert.IsNotNull(totalRevenueCol, "TotalRevenue column should exist");
            
            // Check for GETDATE expression
            var getdateExpr = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.Expression?.Contains("GETDATE") == true);
            
            Assert.IsNotNull(getdateExpr, "GETDATE expression should exist");
            
            // Check for aggregate expressions (SUM)
            var sumExpressions = lineageGraph.Nodes.OfType<ExpressionNode>()
                .Where(n => n.Expression?.Contains("SUM") == true).ToList();
            
            Assert.IsTrue(sumExpressions.Count >= 2, "Should have SUM expressions for aggregates");
        }
    }
}

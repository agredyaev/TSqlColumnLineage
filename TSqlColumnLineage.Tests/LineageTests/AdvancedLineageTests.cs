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
    public class AdvancedLineageTests
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
        public void ComplexStoredProcedure_WithMultipleSteps_ShouldTraceCorrectLineage()
        {
            // Arrange - Complex stored procedure with multiple transformation steps
            string sql = @"
                CREATE PROCEDURE dbo.GenerateSalesAnalyticsReport
                    @StartDate DATE,
                    @EndDate DATE,
                    @RegionID INT = NULL,
                    @MinimumSalesAmount DECIMAL(18,2) = 1000.00
                AS
                BEGIN
                    SET NOCOUNT ON;
                    
                    -- Create a temp table to hold initial sales data
                    CREATE TABLE #InitialSalesData (
                        SalesID INT,
                        OrderDate DATE,
                        ProductID INT, 
                        CustomerID INT,
                        StoreID INT,
                        SalesPersonID INT,
                        RegionID INT,
                        Quantity INT,
                        UnitPrice DECIMAL(18,2),
                        Discount DECIMAL(5,2),
                        TotalAmount DECIMAL(18,2)
                    );
                    
                    -- Insert data from multiple tables with joins
                    INSERT INTO #InitialSalesData (
                        SalesID, OrderDate, ProductID, CustomerID, StoreID,
                        SalesPersonID, RegionID, Quantity, UnitPrice, Discount, TotalAmount
                    )
                    SELECT 
                        s.SalesID,
                        s.OrderDate,
                        s.ProductID,
                        s.CustomerID,
                        st.StoreID,
                        sp.SalesPersonID,
                        r.RegionID,
                        sd.Quantity,
                        p.UnitPrice,
                        ISNULL(sd.Discount, 0) AS Discount,
                        sd.Quantity * p.UnitPrice * (1 - ISNULL(sd.Discount, 0)) AS TotalAmount
                    FROM 
                        Sales s
                    INNER JOIN 
                        SalesDetails sd ON s.SalesID = sd.SalesID
                    INNER JOIN 
                        Products p ON sd.ProductID = p.ProductID
                    INNER JOIN 
                        Stores st ON s.StoreID = st.StoreID
                    INNER JOIN 
                        SalesPeople sp ON s.SalesPersonID = sp.SalesPersonID
                    INNER JOIN 
                        Regions r ON st.RegionID = r.RegionID
                    WHERE 
                        s.OrderDate BETWEEN @StartDate AND @EndDate
                        AND (@RegionID IS NULL OR r.RegionID = @RegionID);
                        
                    -- Create intermediate table for product category aggregation
                    CREATE TABLE #ProductCategorySales (
                        CategoryID INT,
                        CategoryName NVARCHAR(100),
                        SubcategoryID INT,
                        SubcategoryName NVARCHAR(100),
                        RegionID INT,
                        RegionName NVARCHAR(100),
                        TotalQuantity INT,
                        GrossSales DECIMAL(18,2),
                        TotalDiscount DECIMAL(18,2),
                        NetSales DECIMAL(18,2),
                        AvgUnitPrice DECIMAL(18,2),
                        SalesRank INT
                    );
                    
                    -- Populate with aggregations, window functions, and complex calculations
                    INSERT INTO #ProductCategorySales
                    SELECT
                        pc.CategoryID,
                        pc.CategoryName,
                        ps.SubcategoryID,
                        ps.SubcategoryName,
                        r.RegionID,
                        r.RegionName,
                        SUM(s.Quantity) AS TotalQuantity,
                        SUM(s.Quantity * s.UnitPrice) AS GrossSales,
                        SUM(s.Quantity * s.UnitPrice * s.Discount) AS TotalDiscount,
                        SUM(s.TotalAmount) AS NetSales,
                        AVG(s.UnitPrice) AS AvgUnitPrice,
                        RANK() OVER (
                            PARTITION BY pc.CategoryID 
                            ORDER BY SUM(s.TotalAmount) DESC
                        ) AS SalesRank
                    FROM 
                        #InitialSalesData s
                    INNER JOIN 
                        Products p ON s.ProductID = p.ProductID
                    INNER JOIN 
                        ProductSubcategories ps ON p.SubcategoryID = ps.SubcategoryID
                    INNER JOIN 
                        ProductCategories pc ON ps.CategoryID = pc.CategoryID
                    INNER JOIN 
                        Regions r ON s.RegionID = r.RegionID
                    GROUP BY 
                        pc.CategoryID, pc.CategoryName, ps.SubcategoryID, ps.SubcategoryName,
                        r.RegionID, r.RegionName;
                        
                    -- Create final results table with complex derived metrics
                    CREATE TABLE #FinalAnalytics (
                        CategoryName NVARCHAR(100),
                        SubcategoryName NVARCHAR(100),
                        RegionName NVARCHAR(100),
                        TotalQuantity INT,
                        GrossSales DECIMAL(18,2),
                        NetSales DECIMAL(18,2),
                        DiscountRate DECIMAL(5,2),
                        ProfitMargin DECIMAL(5,2),
                        RegionContribution DECIMAL(5,2),
                        YoYGrowth DECIMAL(5,2),
                        PerformanceCategory NVARCHAR(20),
                        RankInCategory INT,
                        IsTopPerformer BIT
                    );
                    
                    -- Complex window functions, case expressions, and multi-column calculations
                    INSERT INTO #FinalAnalytics
                    SELECT
                        s.CategoryName,
                        s.SubcategoryName,
                        s.RegionName,
                        s.TotalQuantity,
                        s.GrossSales,
                        s.NetSales,
                        -- Calculate discount rate
                        CASE 
                            WHEN s.GrossSales = 0 THEN 0
                            ELSE (s.GrossSales - s.NetSales) / s.GrossSales
                        END AS DiscountRate,
                        -- Calculate profit margin (mocked calculation)
                        CASE
                            WHEN s.NetSales = 0 THEN 0
                            ELSE (s.NetSales - (s.NetSales * 0.7)) / s.NetSales
                        END AS ProfitMargin,
                        -- Calculate regional contribution using window function
                        CASE
                            WHEN SUM(s.NetSales) OVER (PARTITION BY s.CategoryID) = 0 THEN 0
                            ELSE s.NetSales / SUM(s.NetSales) OVER (PARTITION BY s.CategoryID)
                        END AS RegionContribution,
                        -- Mock YoY growth with a derived calculation
                        (
                            s.NetSales - 
                            LAG(s.NetSales, 1, s.NetSales) OVER (
                                PARTITION BY s.CategoryID, s.SubcategoryID 
                                ORDER BY s.RegionID
                            )
                        ) / NULLIF(
                            LAG(s.NetSales, 1, s.NetSales) OVER (
                                PARTITION BY s.CategoryID, s.SubcategoryID 
                                ORDER BY s.RegionID
                            ), 0
                        ) AS YoYGrowth,
                        -- Performance categorization based on multiple metrics
                        CASE
                            WHEN s.NetSales > 50000 AND s.SalesRank <= 3 THEN 'High Performer'
                            WHEN s.NetSales BETWEEN 10000 AND 50000 AND s.SalesRank <= 10 THEN 'Mid Performer'
                            WHEN s.NetSales < 10000 OR s.SalesRank > 20 THEN 'Low Performer'
                            ELSE 'Average Performer'
                        END AS PerformanceCategory,
                        -- Ranking within category
                        DENSE_RANK() OVER (
                            PARTITION BY s.CategoryID
                            ORDER BY s.NetSales DESC
                        ) AS RankInCategory,
                        -- Flag for top performers
                        CASE 
                            WHEN s.NetSales > AVG(s.NetSales) OVER (PARTITION BY s.CategoryID) * 1.5 
                            THEN 1 ELSE 0 
                        END AS IsTopPerformer
                    FROM 
                        #ProductCategorySales s
                    WHERE 
                        s.NetSales >= @MinimumSalesAmount;
                        
                    -- Final output with additional calculations
                    SELECT
                        f.CategoryName,
                        f.SubcategoryName,
                        f.RegionName,
                        f.TotalQuantity,
                        f.GrossSales,
                        f.NetSales,
                        f.DiscountRate,
                        f.ProfitMargin,
                        f.RegionContribution,
                        f.YoYGrowth,
                        -- Calculate weighted performance score using 4 different metrics
                        (f.ProfitMargin * 0.4) + 
                        (f.RegionContribution * 0.3) + 
                        ((1 - f.DiscountRate) * 0.2) + 
                        (CASE WHEN f.YoYGrowth > 0 THEN f.YoYGrowth * 0.1 ELSE 0 END) AS PerformanceScore,
                        f.PerformanceCategory,
                        f.RankInCategory,
                        f.IsTopPerformer,
                        -- Recommendation based on multiple factors
                        CASE
                            WHEN f.IsTopPerformer = 1 AND f.ProfitMargin > 0.3 THEN 'Expand Inventory'
                            WHEN f.IsTopPerformer = 1 AND f.ProfitMargin <= 0.3 THEN 'Price Adjustment Needed'
                            WHEN f.IsTopPerformer = 0 AND f.DiscountRate > 0.15 THEN 'Reduce Discounts'
                            WHEN f.IsTopPerformer = 0 AND f.RankInCategory > 10 THEN 'Consider Discontinuation'
                            ELSE 'Maintain Current Strategy'
                        END AS Recommendation
                    FROM 
                        #FinalAnalytics f
                    ORDER BY 
                        f.CategoryName, f.RankInCategory, f.RegionName;
                    
                    -- Clean up temp tables
                    DROP TABLE #InitialSalesData;
                    DROP TABLE #ProductCategorySales;
                    DROP TABLE #FinalAnalytics;
                END";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Verify the stored procedure is parsed
            var procNodes = lineageGraph.Nodes
                .Where(n => n.Name.Contains("GenerateSalesAnalyticsReport"))
                .ToList();
            
            Assert.IsTrue(procNodes.Any(), "Stored procedure node should exist");
            
            // Verify parameter nodes
            var paramNodes = lineageGraph.Nodes
                .Where(n => n.Name.Contains("@") && 
                        (n.Name.Contains("StartDate") || 
                         n.Name.Contains("EndDate") || 
                         n.Name.Contains("RegionID") ||
                         n.Name.Contains("MinimumSalesAmount")))
                .ToList();
            
            Assert.IsTrue(paramNodes.Count >= 4, "Parameter nodes should exist");
            
            // Verify temp tables
            var tempTables = lineageGraph.Nodes.OfType<TableNode>()
                .Where(n => n.Name.StartsWith("#"))
                .ToList();
            
            Assert.AreEqual(3, tempTables.Count, "Should have 3 temp tables");
            
            // Verify source tables
            var sourceTables = lineageGraph.Nodes.OfType<TableNode>()
                .Where(n => !n.Name.StartsWith("#") && 
                           n.TableType != "Derived" && 
                           n.TableType != "CTE")
                .ToList();
            
            // Should have Sales, SalesDetails, Products, Stores, SalesPeople, Regions, 
            // ProductSubcategories, ProductCategories
            Assert.IsTrue(sourceTables.Count >= 6, "Should have at least 6 source tables");
            
            // Verify complex calculations with window functions
            var windowFunctions = lineageGraph.Nodes.OfType<ExpressionNode>()
                .Where(n => n.Expression?.Contains("OVER") == true)
                .ToList();
            
            Assert.IsTrue(windowFunctions.Count >= 5, "Should have at least 5 window functions");
            
            // Verify CASE expressions
            var caseExpressions = lineageGraph.Nodes.OfType<ExpressionNode>()
                .Where(n => n.Expression?.Contains("CASE") == true)
                .ToList();
            
            Assert.IsTrue(caseExpressions.Count >= 4, "Should have at least 4 CASE expressions");
            
            // Verify edge count for complex lineage paths
            Assert.IsTrue(lineageGraph.Edges.Count >= 50, "Should have at least 50 edges for this complex procedure");
            
            // Verify a few specific lineage paths
            
            // Find the final output TotalQuantity column
            var finalQuantityColumn = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("TotalQuantity") && 
                                   n.TableOwner.StartsWith("#Final"));
            
            Assert.IsNotNull(finalQuantityColumn, "Final TotalQuantity column should exist");
            
            // Trace back to original Quantity column
            var pathToQuantity = TraceColumnLineagePath(lineageGraph, finalQuantityColumn.Id);
            Assert.IsTrue(pathToQuantity.Count >= 3, "Should trace TotalQuantity back through at least 3 steps");
            
            // Find PerformanceScore which is calculated from 4 other columns
            var perfScoreNode = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.Name.Contains("PerformanceScore"));
            
            if (perfScoreNode != null)
            {
                var perfScoreInputs = lineageGraph.Edges
                    .Where(e => e.TargetId == perfScoreNode.Id)
                    .ToList();
                
                Assert.IsTrue(perfScoreInputs.Count >= 4, "PerformanceScore should have at least 4 input columns");
            }
        }
        
        [TestMethod]
        public void ComplexTransformationsWithMultipleTables_ShouldTraceCorrectLineage()
        {
            // Arrange
            string sql = @"
                -- Create temp table for sales data
                CREATE TABLE #Sales (
                    OrderID INT,
                    CustomerID INT,
                    ProductID INT,
                    OrderDate DATE,
                    Quantity INT,
                    UnitPrice DECIMAL(10,2),
                    Discount DECIMAL(5,2),
                    TotalAmount DECIMAL(12,2)
                );
                
                -- Step 1: Extract and transform sales data from multiple tables
                INSERT INTO #Sales
                SELECT
                    o.OrderID,
                    o.CustomerID,
                    od.ProductID,
                    o.OrderDate,
                    od.Quantity,
                    od.UnitPrice,
                    od.Discount,
                    od.Quantity * od.UnitPrice * (1 - od.Discount) AS TotalAmount
                FROM
                    Orders o
                JOIN
                    OrderDetails od ON o.OrderID = od.OrderID
                WHERE
                    o.OrderDate >= DATEADD(YEAR, -1, GETDATE());
                
                -- Step 2: Create customer segment table with demographic data
                CREATE TABLE #CustomerSegments (
                    CustomerID INT,
                    CustomerName NVARCHAR(100),
                    Country NVARCHAR(50),
                    Region NVARCHAR(50),
                    CustomerSegment NVARCHAR(20),
                    LifetimeValue DECIMAL(12,2),
                    PurchaseFrequency INT,
                    LastPurchaseDate DATE,
                    DaysSinceLastPurchase INT
                );
                
                -- Step 3: Transform customer data with complex calculations
                INSERT INTO #CustomerSegments
                SELECT
                    c.CustomerID,
                    c.CompanyName AS CustomerName,
                    c.Country,
                    c.Region,
                    -- Complex segmentation logic with CASE
                    CASE
                        WHEN SUM(s.TotalAmount) > 50000 THEN 'Enterprise'
                        WHEN SUM(s.TotalAmount) BETWEEN 10000 AND 50000 THEN 'Mid-Market'
                        WHEN SUM(s.TotalAmount) BETWEEN 1000 AND 9999 THEN 'Small Business'
                        ELSE 'Individual'
                    END AS CustomerSegment,
                    -- Lifetime value calculation
                    SUM(s.TotalAmount) AS LifetimeValue,
                    -- Purchase frequency
                    COUNT(DISTINCT s.OrderID) AS PurchaseFrequency,
                    -- Last purchase date
                    MAX(s.OrderDate) AS LastPurchaseDate,
                    -- Days since last purchase
                    DATEDIFF(DAY, MAX(s.OrderDate), GETDATE()) AS DaysSinceLastPurchase
                FROM
                    Customers c
                LEFT JOIN
                    #Sales s ON c.CustomerID = s.CustomerID
                GROUP BY
                    c.CustomerID, c.CompanyName, c.Country, c.Region;
                
                -- Step 4: Create product analytics with supply chain data
                CREATE TABLE #ProductPerformance (
                    ProductID INT,
                    ProductName NVARCHAR(100),
                    CategoryName NVARCHAR(50),
                    SupplierName NVARCHAR(100),
                    SupplierCountry NVARCHAR(50),
                    UnitsSold INT,
                    Revenue DECIMAL(12,2),
                    COGS DECIMAL(12,2),
                    GrossProfit DECIMAL(12,2),
                    GrossProfitMargin DECIMAL(5,2),
                    AverageUnitPrice DECIMAL(10,2),
                    InventoryTurnover DECIMAL(5,2),
                    IsDiscontinued BIT
                );
                
                -- Step 5: Complex product analysis with joins to 4 different tables
                INSERT INTO #ProductPerformance
                SELECT
                    p.ProductID,
                    p.ProductName,
                    c.CategoryName,
                    s.CompanyName AS SupplierName,
                    s.Country AS SupplierCountry,
                    -- Units sold calculation
                    COALESCE(SUM(sales.Quantity), 0) AS UnitsSold,
                    -- Revenue calculation
                    COALESCE(SUM(sales.TotalAmount), 0) AS Revenue,
                    -- Cost of goods sold - complex calculation
                    COALESCE(SUM(sales.Quantity * p.UnitCost), 0) AS COGS,
                    -- Gross profit calculation
                    COALESCE(SUM(sales.TotalAmount - (sales.Quantity * p.UnitCost)), 0) AS GrossProfit,
                    -- Gross profit margin with safeguard against division by zero
                    CASE
                        WHEN COALESCE(SUM(sales.TotalAmount), 0) = 0 THEN 0
                        ELSE COALESCE(SUM(sales.TotalAmount - (sales.Quantity * p.UnitCost)), 0) / 
                             COALESCE(SUM(sales.TotalAmount), 0)
                    END AS GrossProfitMargin,
                    -- Average unit price calculation
                    COALESCE(AVG(sales.UnitPrice), p.UnitPrice) AS AverageUnitPrice,
                    -- Inventory turnover calculation
                    CASE
                        WHEN p.UnitsInStock = 0 THEN 0
                        ELSE COALESCE(SUM(sales.Quantity), 0) / NULLIF(AVG(p.UnitsInStock), 0)
                    END AS InventoryTurnover,
                    -- Discontinued flag
                    p.Discontinued AS IsDiscontinued
                FROM
                    Products p
                JOIN
                    Categories c ON p.CategoryID = c.CategoryID
                JOIN
                    Suppliers s ON p.SupplierID = s.SupplierID
                LEFT JOIN
                    #Sales sales ON p.ProductID = sales.ProductID
                LEFT JOIN
                    Inventory i ON p.ProductID = i.ProductID
                GROUP BY
                    p.ProductID, p.ProductName, c.CategoryName, s.CompanyName, s.Country,
                    p.UnitPrice, p.UnitsInStock, p.Discontinued, p.UnitCost;
                
                -- Step 6: Final analysis with window functions and complex calculations
                WITH SalesAnalysis AS (
                    SELECT
                        seg.CustomerSegment,
                        perf.CategoryName AS ProductCategory,
                        perf.SupplierCountry,
                        -- Date dimension derived from OrderDate
                        DATEPART(QUARTER, s.OrderDate) AS SalesQuarter,
                        DATEPART(YEAR, s.OrderDate) AS SalesYear,
                        -- Aggregated metrics
                        COUNT(DISTINCT s.CustomerID) AS UniqueCustomers,
                        COUNT(DISTINCT s.OrderID) AS OrderCount,
                        SUM(s.Quantity) AS TotalQuantity,
                        SUM(s.TotalAmount) AS TotalRevenue,
                        SUM(perf.COGS) AS TotalCOGS,
                        SUM(perf.GrossProfit) AS TotalProfit,
                        -- Multi-column calculations
                        SUM(s.TotalAmount) / COUNT(DISTINCT s.OrderID) AS AvgOrderValue,
                        SUM(s.Quantity) / COUNT(DISTINCT s.OrderID) AS AvgOrderSize,
                        -- Window functions for ranking and comparison
                        RANK() OVER(PARTITION BY seg.CustomerSegment ORDER BY SUM(s.TotalAmount) DESC) AS CategoryRankBySegment,
                        SUM(s.TotalAmount) / SUM(SUM(s.TotalAmount)) OVER(PARTITION BY DATEPART(YEAR, s.OrderDate)) AS YearlyRevenueContribution,
                        -- YoY Growth calculation using LAG
                        (SUM(s.TotalAmount) - LAG(SUM(s.TotalAmount), 1, NULL) OVER(
                            PARTITION BY seg.CustomerSegment, perf.CategoryName
                            ORDER BY DATEPART(YEAR, s.OrderDate)
                        )) / NULLIF(LAG(SUM(s.TotalAmount), 1, NULL) OVER(
                            PARTITION BY seg.CustomerSegment, perf.CategoryName
                            ORDER BY DATEPART(YEAR, s.OrderDate)
                        ), 0) AS YoYGrowth,
                        -- Moving average of last 3 periods
                        AVG(SUM(s.TotalAmount)) OVER(
                            PARTITION BY seg.CustomerSegment, perf.CategoryName
                            ORDER BY DATEPART(YEAR, s.OrderDate)
                            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                        ) AS MovingAvgRevenue
                    FROM
                        #Sales s
                    JOIN
                        #CustomerSegments seg ON s.CustomerID = seg.CustomerID
                    JOIN
                        #ProductPerformance perf ON s.ProductID = perf.ProductID
                    GROUP BY
                        seg.CustomerSegment, perf.CategoryName, perf.SupplierCountry,
                        DATEPART(QUARTER, s.OrderDate), DATEPART(YEAR, s.OrderDate)
                )
                -- Final query with even more transformations
                SELECT
                    sa.CustomerSegment,
                    sa.ProductCategory,
                    sa.SupplierCountry,
                    sa.SalesYear,
                    sa.SalesQuarter,
                    sa.UniqueCustomers,
                    sa.OrderCount,
                    sa.TotalQuantity,
                    sa.TotalRevenue,
                    sa.TotalProfit,
                    sa.AvgOrderValue,
                    sa.YoYGrowth,
                    sa.MovingAvgRevenue,
                    -- Complex performance scoring with 4 metrics
                    (sa.YoYGrowth * 0.3) +
                    (sa.TotalProfit / NULLIF(sa.TotalRevenue, 0) * 0.3) +
                    (sa.AvgOrderValue / 1000 * 0.2) +
                    (sa.YearlyRevenueContribution * 0.2) AS PerformanceScore,
                    -- Performance category based on score with CASE
                    CASE
                        WHEN ((sa.YoYGrowth * 0.3) +
                              (sa.TotalProfit / NULLIF(sa.TotalRevenue, 0) * 0.3) +
                              (sa.AvgOrderValue / 1000 * 0.2) +
                              (sa.YearlyRevenueContribution * 0.2)) > 0.8 THEN 'Excellent'
                        WHEN ((sa.YoYGrowth * 0.3) +
                              (sa.TotalProfit / NULLIF(sa.TotalRevenue, 0) * 0.3) +
                              (sa.AvgOrderValue / 1000 * 0.2) +
                              (sa.YearlyRevenueContribution * 0.2)) > 0.6 THEN 'Good'
                        WHEN ((sa.YoYGrowth * 0.3) +
                              (sa.TotalProfit / NULLIF(sa.TotalRevenue, 0) * 0.3) +
                              (sa.AvgOrderValue / 1000 * 0.2) +
                              (sa.YearlyRevenueContribution * 0.2)) > 0.4 THEN 'Average'
                        ELSE 'Below Average'
                    END AS PerformanceCategory,
                    -- Opportunity sizing based on multiple factors
                    CASE
                        WHEN sa.YoYGrowth > 0.1 AND sa.TotalProfit / NULLIF(sa.TotalRevenue, 0) > 0.3 THEN 'High Growth & Profitable'
                        WHEN sa.YoYGrowth > 0.1 THEN 'High Growth, Optimize Profit'
                        WHEN sa.TotalProfit / NULLIF(sa.TotalRevenue, 0) > 0.3 THEN 'Profitable, Accelerate Growth'
                        ELSE 'Evaluate Strategy'
                    END AS BusinessOpportunity,
                    -- Overall rank among all segments
                    DENSE_RANK() OVER(ORDER BY 
                        (sa.YoYGrowth * 0.3) +
                        (sa.TotalProfit / NULLIF(sa.TotalRevenue, 0) * 0.3) +
                        (sa.AvgOrderValue / 1000 * 0.2) +
                        (sa.YearlyRevenueContribution * 0.2) DESC
                    ) AS OverallRank
                FROM
                    SalesAnalysis sa
                ORDER BY
                    PerformanceScore DESC,
                    sa.CustomerSegment,
                    sa.ProductCategory;";

            // Act
            var lineageGraph = _lineageService.BuildLineage(sql);

            // Assert
            Assert.IsNotNull(lineageGraph);
            
            // Verify temp tables
            var tempTables = lineageGraph.Nodes.OfType<TableNode>()
                .Where(n => n.Name.StartsWith("#"))
                .ToList();
            
            Assert.AreEqual(3, tempTables.Count, "Should have 3 temp tables");
            
            // Verify source tables
            var sourceTables = lineageGraph.Nodes.OfType<TableNode>()
                .Where(n => !n.Name.StartsWith("#") && 
                           !n.Name.Equals("SalesAnalysis") && 
                           n.TableType != "Derived" && 
                           n.TableType != "CTE")
                .ToList();
            
            // Should have Orders, OrderDetails, Customers, Products, Categories, Suppliers, Inventory
            Assert.IsTrue(sourceTables.Count >= 6, "Should have at least 6 source tables");
            
            // Verify CTE
            var cteTable = lineageGraph.Nodes.OfType<TableNode>()
                .FirstOrDefault(n => n.Name.Equals("SalesAnalysis"));
            
            Assert.IsNotNull(cteTable, "SalesAnalysis CTE should exist");
            
            // Verify window functions
            var windowFunctions = lineageGraph.Nodes.OfType<ExpressionNode>()
                .Where(n => n.Expression?.Contains("OVER") == true)
                .ToList();
            
            Assert.IsTrue(windowFunctions.Count >= 5, "Should have at least 5 window functions");
            
            // Verify CASE expressions
            var caseExpressions = lineageGraph.Nodes.OfType<ExpressionNode>()
                .Where(n => n.Expression?.Contains("CASE") == true)
                .ToList();
            
            Assert.IsTrue(caseExpressions.Count >= 5, "Should have at least 5 CASE expressions");
            
            // Verify edge count for complex lineage paths
            Assert.IsTrue(lineageGraph.Edges.Count >= 60, "Should have at least 60 edges for this complex transformation");
            
            // Verify specific lineage paths
            
            // Find the PerformanceScore column which combines multiple metrics
            var performanceScoreNode = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.Expression?.Contains("PerformanceScore") == true ||
                                    n.Name?.Contains("PerformanceScore") == true);
            
            Assert.IsNotNull(performanceScoreNode, "PerformanceScore calculation should exist");
            
            // It should have at least 4 inputs
            if (performanceScoreNode != null)
            {
                var performanceScoreInputs = lineageGraph.Edges
                    .Where(e => e.TargetId == performanceScoreNode.Id)
                    .ToList();
                
                Assert.IsTrue(performanceScoreInputs.Count >= 4, "PerformanceScore should have at least 4 input columns");
            }
            
            // Verify YoYGrowth which uses window functions
            var yoyGrowthNode = lineageGraph.Nodes.OfType<ExpressionNode>()
                .FirstOrDefault(n => n.Name?.Contains("YoYGrowth") == true || 
                                    n.Expression?.Contains("YoYGrowth") == true);
            
            Assert.IsNotNull(yoyGrowthNode, "YoYGrowth calculation should exist");
            
            // Trace lineage path to validate complex transformation
            if (yoyGrowthNode != null)
            {
                var yoyGrowthPath = TraceColumnLineagePath(lineageGraph, yoyGrowthNode.Id);
                Assert.IsTrue(yoyGrowthPath.Count >= 3, "YoYGrowth should trace back through at least 3 steps");
            }
            
            // Verify TotalRevenue column in the final output
            var totalRevenueColumn = lineageGraph.Nodes.OfType<ColumnNode>()
                .FirstOrDefault(n => n.Name.Equals("TotalRevenue") && 
                                   n.TableOwner?.Contains("SalesAnalysis") == true);
            
            Assert.IsNotNull(totalRevenueColumn, "TotalRevenue column should exist in final output");
            
            // Trace back to original TotalAmount column
            if (totalRevenueColumn != null)
            {
                var pathToTotalAmount = TraceColumnLineagePath(lineageGraph, totalRevenueColumn.Id);
                Assert.IsTrue(pathToTotalAmount.Count >= 3, "Should trace TotalRevenue back through at least 3 steps");
            }
        }
        
        // Helper method to trace column lineage paths
        private List<string> TraceColumnLineagePath(LineageGraph graph, string startNodeId)
        {
            var path = new List<string>();
            var visited = new HashSet<string>();
            
            // Add the starting node to the path
            path.Add(startNodeId);
            visited.Add(startNodeId);
            
            // Trace backward through lineage
            TraceLineagePathRecursive(graph, startNodeId, path, visited);
            
            return path;
        }
        
        private void TraceLineagePathRecursive(LineageGraph graph, string currentNodeId, 
                                              List<string> path, HashSet<string> visited)
        {
            // Find all edges where current node is the target
            var incomingEdges = graph.Edges
                .Where(e => e.TargetId == currentNodeId)
                .ToList();
            
            foreach (var edge in incomingEdges)
            {
                if (!visited.Contains(edge.SourceId))
                {
                    // Add this node to the path
                    path.Add(edge.SourceId);
                    visited.Add(edge.SourceId);
                    
                    // Continue tracing recursively
                    TraceLineagePathRecursive(graph, edge.SourceId, path, visited);
                }
            }
        }
    }
}
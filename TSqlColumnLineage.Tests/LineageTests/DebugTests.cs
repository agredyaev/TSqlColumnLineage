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
    public class DebugTests
    {
        private LineageService _lineageService;
        private SqlParser _sqlParser;
        private LineageNodeFactory _nodeFactory;
        private TSqlColumnLineage.Core.LineageEdgeFactory _edgeFactory;
        private MockMetadataService _metadataService;
        private IGraphService _graphService;
        private DebugLogger _logger;

        // Debug logger to capture logs during tests
        public class DebugLogger : ILogger
        {
            public void LogDebug(string message)
            {
                Console.WriteLine($"DEBUG: {message}");
            }

            public void LogError(Exception ex, string message)
            {
                Console.WriteLine($"ERROR: {message}");
                if (ex != null)
                {
                    Console.WriteLine($"EXCEPTION: {ex.Message}");
                    Console.WriteLine($"STACKTRACE: {ex.StackTrace}");
                }
            }

            public void LogInformation(string message)
            {
                Console.WriteLine($"INFO: {message}");
            }

            public void LogWarning(string message)
            {
                Console.WriteLine($"WARN: {message}");
            }
        }

        // Mock metadata service that pre-populates with sample table schema info
        public class MockMetadataService : IMetadataService
        {
            // Dictionary to hold table metadata
            private readonly Dictionary<string, TableNode> _tables = new Dictionary<string, TableNode>();
            
            // Dictionary to hold column metadata
            private readonly Dictionary<string, ColumnNode> _columns = new Dictionary<string, ColumnNode>();
            
            public void PopulateContext(LineageContext context)
            {
                // Simulate schema information for common test tables
                
                // 1. Create Customers table columns
                var customersTable = new TableNode
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = "Customers",
                    ObjectName = "Customers",
                    SchemaName = "dbo",
                    Type = "Table",
                    TableType = "Table"
                };
                context.AddTable(customersTable);
                
                // Add columns for Customers
                var customerColumns = new[] {
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "CustomerID",
                        ObjectName = "CustomerID",
                        TableOwner = "Customers",
                        Type = "Column",
                        DataType = "INT"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "CustomerName",
                        ObjectName = "CustomerName",
                        TableOwner = "Customers",
                        Type = "Column",
                        DataType = "VARCHAR(100)"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "FirstName",
                        ObjectName = "FirstName",
                        TableOwner = "Customers",
                        Type = "Column",
                        DataType = "VARCHAR(50)"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "LastName",
                        ObjectName = "LastName",
                        TableOwner = "Customers",
                        Type = "Column",
                        DataType = "VARCHAR(50)"
                    }
                };
                
                // 2. Create Orders table columns
                var ordersTable = new TableNode
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = "Orders",
                    ObjectName = "Orders",
                    SchemaName = "dbo",
                    Type = "Table",
                    TableType = "Table"
                };
                context.AddTable(ordersTable);
                
                // Add columns for Orders
                var orderColumns = new[] {
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "OrderID",
                        ObjectName = "OrderID",
                        TableOwner = "Orders",
                        Type = "Column",
                        DataType = "INT"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "CustomerID",
                        ObjectName = "CustomerID",
                        TableOwner = "Orders",
                        Type = "Column",
                        DataType = "INT"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "OrderDate",
                        ObjectName = "OrderDate",
                        TableOwner = "Orders",
                        Type = "Column",
                        DataType = "DATETIME"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "TotalAmount",
                        ObjectName = "TotalAmount",
                        TableOwner = "Orders",
                        Type = "Column",
                        DataType = "DECIMAL(18,2)"
                    }
                };
                
                // 3. Create Products table columns
                var productsTable = new TableNode
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = "Products",
                    ObjectName = "Products",
                    SchemaName = "dbo",
                    Type = "Table",
                    TableType = "Table"
                };
                context.AddTable(productsTable);
                
                // Add columns for Products
                var productColumns = new[] {
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "ProductID",
                        ObjectName = "ProductID",
                        TableOwner = "Products",
                        Type = "Column",
                        DataType = "INT"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "ProductName",
                        ObjectName = "ProductName",
                        TableOwner = "Products",
                        Type = "Column",
                        DataType = "VARCHAR(100)"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "UnitPrice",
                        ObjectName = "UnitPrice",
                        TableOwner = "Products",
                        Type = "Column",
                        DataType = "DECIMAL(18,2)"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "Quantity",
                        ObjectName = "Quantity",
                        TableOwner = "Products",
                        Type = "Column",
                        DataType = "INT"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "ProductCategory",
                        ObjectName = "ProductCategory",
                        TableOwner = "Products",
                        Type = "Column",
                        DataType = "INT"
                    }
                };
                
                // 4. Create ProductSales table columns
                var productSalesTable = new TableNode
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = "ProductSales",
                    ObjectName = "ProductSales",
                    SchemaName = "dbo",
                    Type = "Table",
                    TableType = "Table"
                };
                context.AddTable(productSalesTable);
                
                // Add columns for ProductSales
                var productSalesColumns = new[] {
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "ProductID",
                        ObjectName = "ProductID",
                        TableOwner = "ProductSales",
                        Type = "Column",
                        DataType = "INT"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "Year",
                        ObjectName = "Year",
                        TableOwner = "ProductSales",
                        Type = "Column",
                        DataType = "INT"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "SalesAmount",
                        ObjectName = "SalesAmount",
                        TableOwner = "ProductSales",
                        Type = "Column",
                        DataType = "DECIMAL(18,2)"
                    }
                };
                
                // 5. Create Sales table columns
                var salesTable = new TableNode
                {
                    Id = Guid.NewGuid().ToString(),
                    Name = "Sales",
                    ObjectName = "Sales",
                    SchemaName = "dbo",
                    Type = "Table",
                    TableType = "Table"
                };
                context.AddTable(salesTable);
                
                // Add columns for Sales
                var salesColumns = new[] {
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "SaleID",
                        ObjectName = "SaleID",
                        TableOwner = "Sales",
                        Type = "Column",
                        DataType = "INT"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "CustomerID",
                        ObjectName = "CustomerID",
                        TableOwner = "Sales",
                        Type = "Column",
                        DataType = "INT"
                    },
                    new ColumnNode {
                        Id = Guid.NewGuid().ToString(),
                        Name = "Amount",
                        ObjectName = "Amount",
                        TableOwner = "Sales",
                        Type = "Column",
                        DataType = "DECIMAL(18,2)"
                    }
                };
                
                // Add all tables and columns to the lineage graph
                context.Graph.AddNode(customersTable);
                foreach (var col in customerColumns)
                    context.Graph.AddNode(col);
                    
                context.Graph.AddNode(ordersTable);
                foreach (var col in orderColumns)
                    context.Graph.AddNode(col);
                    
                context.Graph.AddNode(productsTable);
                foreach (var col in productColumns)
                    context.Graph.AddNode(col);
                    
                context.Graph.AddNode(productSalesTable);
                foreach (var col in productSalesColumns)
                    context.Graph.AddNode(col);
                    
                context.Graph.AddNode(salesTable);
                foreach (var col in salesColumns)
                    context.Graph.AddNode(col);
                
                // Store tables and columns for other methods
                _tables["customers"] = customersTable;
                foreach (var col in customerColumns)
                    _columns[$"customers.{col.Name.ToLowerInvariant()}"] = col;
                
                _tables["orders"] = ordersTable;
                foreach (var col in orderColumns)
                    _columns[$"orders.{col.Name.ToLowerInvariant()}"] = col;
                
                _tables["products"] = productsTable;
                foreach (var col in productColumns)
                    _columns[$"products.{col.Name.ToLowerInvariant()}"] = col;
                
                _tables["productsales"] = productSalesTable;
                foreach (var col in productSalesColumns)
                    _columns[$"productsales.{col.Name.ToLowerInvariant()}"] = col;
                
                _tables["sales"] = salesTable;
                foreach (var col in salesColumns)
                    _columns[$"sales.{col.Name.ToLowerInvariant()}"] = col;
            }
            
            public IEnumerable<ColumnNode> GetTableColumnsMetadata(string tableName)
            {
                if (string.IsNullOrEmpty(tableName))
                    return Enumerable.Empty<ColumnNode>();
                    
                string normalizedName = tableName.ToLowerInvariant();
                
                return _columns.Values
                    .Where(c => c.TableOwner.ToLowerInvariant() == normalizedName)
                    .ToList();
            }
            
            public bool TableExists(string tableName)
            {
                if (string.IsNullOrEmpty(tableName))
                    return false;
                    
                return _tables.ContainsKey(tableName.ToLowerInvariant());
            }
            
            public ColumnNode GetColumnMetadata(string tableName, string columnName)
            {
                if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(columnName))
                    return null;
                    
                string key = $"{tableName.ToLowerInvariant()}.{columnName.ToLowerInvariant()}";
                
                return _columns.TryGetValue(key, out var column) ? column : null;
            }
        }

        [TestInitialize]
        public void Initialize()
        {
            _nodeFactory = new LineageNodeFactory();
            _edgeFactory = new Core.LineageEdgeFactory();
            _sqlParser = new SqlParser();
            _logger = new DebugLogger();
            _metadataService = new MockMetadataService();
            _graphService = new GraphService(_nodeFactory, _edgeFactory);
            _lineageService = new LineageService(_sqlParser, _metadataService, _graphService, _logger, _nodeFactory, _edgeFactory);
        }
        
        [TestMethod]
        public void DebugSimpleSelect()
        {
            try
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
                
                // Print graph details for debugging
                Console.WriteLine($"Graph created with {lineageGraph.Nodes.Count} nodes and {lineageGraph.Edges.Count} edges");
                
                Console.WriteLine("\nNODES:");
                foreach (var node in lineageGraph.Nodes)
                {
                    Console.WriteLine($"  ID: {node.Id}, Type: {node.Type}, Name: {node.Name}");
                }
                
                Console.WriteLine("\nEDGES:");
                foreach (var edge in lineageGraph.Edges)
                {
                    var sourceNode = lineageGraph.GetNodeById(edge.SourceId);
                    var targetNode = lineageGraph.GetNodeById(edge.TargetId);
                    
                    Console.WriteLine($"  {sourceNode?.Name ?? "Unknown"} -> {targetNode?.Name ?? "Unknown"} [{edge.Type}] ({edge.Operation})");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"EXCEPTION: {ex.Message}");
                Console.WriteLine($"STACKTRACE: {ex.StackTrace}");
                throw;
            }
        }
        
        [TestMethod]
        public void DebugLineageEdgeCreation()
        {
            try
            {
                // Create some nodes for test edges
                var sourceNode = new ColumnNode
                {
                    Id = "source-node",
                    Name = "SourceColumn",
                    TableOwner = "SourceTable",
                    Type = "Column"
                };
                
                var targetNode = new ColumnNode
                {
                    Id = "target-node",
                    Name = "TargetColumn",
                    TableOwner = "TargetTable",
                    Type = "Column"
                };
                
                // Test creating edges between nodes
                Console.WriteLine("Testing LineageEdge creation...");
                var edge = new LineageEdge
                {
                    Id = Guid.NewGuid().ToString(),
                    SourceId = sourceNode.Id,
                    TargetId = targetNode.Id,
                    Type = "direct",
                    Operation = "select"
                };
                
                Console.WriteLine($"Created edge: {edge.Id}, Source: {edge.SourceId}, Target: {edge.TargetId}, Type: {edge.Type}");
                
                Assert.AreEqual("direct", edge.Type);
                Assert.AreEqual("source-node", edge.SourceId);
                Assert.AreEqual("target-node", edge.TargetId);
                Assert.AreEqual("select", edge.Operation);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"EXCEPTION: {ex.Message}");
                Console.WriteLine($"STACKTRACE: {ex.StackTrace}");
                throw;
            }
        }
        
        [TestMethod]
        public void DebugParserWithBasicSQL()
        {
            try
            {
                // Test basic SQL parsing
                string sql = "SELECT CustomerID, CustomerName FROM Customers WHERE CustomerID > 100";
                
                Console.WriteLine("Testing SQL Parser...");
                var parser = new SqlParser();
                var fragment = parser.Parse(sql);
                
                Console.WriteLine($"SQL parsed successfully, fragment type: {fragment.GetType().Name}");
                
                // Print LineageContext details
                var context = parser.LineageContext;
                Console.WriteLine($"LineageContext created: {context != null}");
                if (context != null)
                {
                    Console.WriteLine($"Context has {context.Tables.Count} tables");
                    foreach (var table in context.Tables)
                    {
                        Console.WriteLine($"  Table: {table.Key} -> {table.Value.Name}");
                    }
                }
                
                Assert.IsNotNull(fragment);
                Assert.IsNotNull(context);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"EXCEPTION: {ex.Message}");
                Console.WriteLine($"STACKTRACE: {ex.StackTrace}");
                throw;
            }
        }
    }
}

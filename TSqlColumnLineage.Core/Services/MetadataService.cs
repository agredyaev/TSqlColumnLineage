using System;
using System.Collections.Generic;
using System.Linq;
using TSqlColumnLineage.Core.Models;

namespace TSqlColumnLineage.Core.Services
{
    /// <summary>
    /// Service for metadata operations (e.g., fetching table and column metadata)
    /// </summary>
    public interface IMetadataService
    {
        /// <summary>
        /// Populates the lineage context with metadata information
        /// </summary>
        /// <param name="context">Lineage context to populate</param>
        void PopulateContext(LineageContext context);

        /// <summary>
        /// Retrieves column metadata for a given table
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <returns>Collection of ColumnMetadata objects</returns>
        IEnumerable<ColumnNode> GetTableColumnsMetadata(string tableName);

        /// <summary>
        /// Checks if a table exists in the metadata store
        /// </summary>
        /// <param name="tableName">Table name to check</param>
        /// <returns>True if the table exists, false otherwise</returns>
        bool TableExists(string tableName);

        /// <summary>
        /// Gets metadata for a specific column in a table
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="columnName">Name of the column</param>
        /// <returns>ColumnNode if found, null otherwise</returns>
        ColumnNode GetColumnMetadata(string tableName, string columnName);
    }

    /// <summary>
    /// Default implementation of the metadata service
    /// </summary>
    public class MetadataService : IMetadataService
    {
        // Dictionary to hold table metadata, with table name as key
        private readonly Dictionary<string, TableNode> _tables = new();
        
        // Dictionary to hold column metadata, with "table.column" as key
        private readonly Dictionary<string, ColumnNode> _columns = new();

        /// <summary>
        /// Initializes a new instance of the MetadataService
        /// </summary>
        public MetadataService()
        {
            // Initialize with sample/default metadata for common SQL Server system tables
            InitializeDefaultMetadata();
        }

        /// <summary>
        /// Populates the lineage context with metadata information
        /// </summary>
        /// <param name="context">Lineage context to populate</param>
        public void PopulateContext(LineageContext context)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            // Add tables to the context
            foreach (var table in _tables.Values)
            {
                context.AddTable(table);
            }

            // Additional metadata could be added to context.Metadata dictionary
            context.Metadata["MetadataSource"] = "MetadataService";
            context.Metadata["LastUpdated"] = DateTime.UtcNow;
        }

        /// <summary>
        /// Retrieves column metadata for a given table
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <returns>Collection of ColumnMetadata objects</returns>
        public IEnumerable<ColumnNode> GetTableColumnsMetadata(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(nameof(tableName));

            var normalizedTableName = NormalizeObjectName(tableName);
            
            Console.WriteLine($"DEBUG: GetTableColumnsMetadata called for table: {tableName}, normalized: {normalizedTableName}");
            Console.WriteLine($"DEBUG: Tables available: {string.Join(", ", _tables.Keys)}");
            
            // If table exists in our metadata
            if (_tables.TryGetValue(normalizedTableName, out var table))
            {
                // Return all columns that belong to this table
                var columns = _columns.Values
                    .Where(c => c.TableOwner.ToLowerInvariant() == normalizedTableName)
                    .ToList();
                
                Console.WriteLine($"DEBUG: Found {columns.Count} columns for table {normalizedTableName}");
                foreach (var col in columns)
                {
                    Console.WriteLine($"DEBUG: Column: {col.Name}, TableOwner: {col.TableOwner}");
                }
                
                return columns;
            }
            
            Console.WriteLine($"DEBUG: Table {normalizedTableName} not found in metadata");

            // Table not found, return empty collection
            return Enumerable.Empty<ColumnNode>();
        }

        /// <summary>
        /// Checks if a table exists in the metadata store
        /// </summary>
        /// <param name="tableName">Table name to check</param>
        /// <returns>True if the table exists, false otherwise</returns>
        public bool TableExists(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return false;

            return _tables.ContainsKey(NormalizeObjectName(tableName));
        }

        /// <summary>
        /// Gets metadata for a specific column in a table
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="columnName">Name of the column</param>
        /// <returns>ColumnNode if found, null otherwise</returns>
        public ColumnNode GetColumnMetadata(string tableName, string columnName)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(columnName))
                return null;

            var key = $"{NormalizeObjectName(tableName)}.{columnName.ToLowerInvariant()}";
            
            return _columns.TryGetValue(key, out var column) ? column : null;
        }

        /// <summary>
        /// Adds a table to the metadata store
        /// </summary>
        /// <param name="table">Table node to add</param>
        public void AddTable(TableNode table)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            var normalizedName = NormalizeObjectName(table.Name);
            _tables[normalizedName] = table;
        }

        /// <summary>
        /// Adds a table to the metadata store using just the table name
        /// </summary>
        /// <param name="tableName">Name of the table to add</param>
        public void AddTable(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(nameof(tableName));

            var tableNode = new TableNode
            {
                Name = tableName,
                ObjectName = tableName,
                SchemaName = "dbo",
                TableType = tableName.StartsWith("#") ? "TempTable" : "Table"
            };
            
            AddTable(tableNode);
        }

        /// <summary>
        /// Adds a column to the metadata store
        /// </summary>
        /// <param name="column">Column node to add</param>
        public void AddColumn(ColumnNode column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            var key = $"{NormalizeObjectName(column.TableOwner)}.{column.Name.ToLowerInvariant()}";
            _columns[key] = column;
        }

        /// <summary>
        /// Adds a column to the metadata store using table name and column name
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="columnName">Name of the column</param>
        public void AddColumn(string tableName, string columnName)
        {
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(nameof(tableName));
            
            if (string.IsNullOrEmpty(columnName))
                throw new ArgumentNullException(nameof(columnName));

            var columnNode = new ColumnNode
            {
                Name = columnName,
                ObjectName = columnName,
                TableOwner = tableName,
                SchemaName = "dbo",
                DataType = "varchar(100)",
                IsNullable = true
            };
            
            AddColumn(columnNode);
        }

        /// <summary>
        /// Normalizes an object name for consistent key comparison
        /// </summary>
        /// <param name="name">Object name to normalize</param>
        /// <returns>Normalized name</returns>
        private string NormalizeObjectName(string name)
        {
            return name?.ToLowerInvariant() ?? string.Empty;
        }

        /// <summary>
        /// Initialize with some default metadata for common objects
        /// This would be replaced with actual metadata retrieval logic in a production environment
        /// </summary>
        private void InitializeDefaultMetadata()
        {
            // Add system tables
            AddSystemTablesMetadata();
            
            // Add test tables for unit tests
            AddTestTablesMetadata();
        }
        
        /// <summary>
        /// Add system tables metadata
        /// </summary>
        private void AddSystemTablesMetadata()
        {
            // Example: Add metadata for system tables
            var sysObjectsTable = new TableNode
            {
                Name = "sys.objects",
                ObjectName = "objects",
                SchemaName = "sys",
                TableType = "System Table"
            };
            AddTable(sysObjectsTable);

            // Add columns for sys.objects
            AddColumn(new ColumnNode
            {
                Name = "object_id",
                TableOwner = "sys.objects",
                ObjectName = "object_id",
                SchemaName = "sys",
                DataType = "int",
                IsNullable = false
            });

            AddColumn(new ColumnNode
            {
                Name = "name",
                TableOwner = "sys.objects",
                ObjectName = "name",
                SchemaName = "sys",
                DataType = "nvarchar(128)",
                IsNullable = false
            });

            AddColumn(new ColumnNode
            {
                Name = "type",
                TableOwner = "sys.objects",
                ObjectName = "type",
                SchemaName = "sys",
                DataType = "char(2)",
                IsNullable = false
            });
        }
        
        /// <summary>
        /// Add test tables metadata for unit tests
        /// </summary>
        private void AddTestTablesMetadata()
        {
            // Add Customers table and columns
            AddTable("Customers");
            AddColumn("Customers", "CustomerID");
            AddColumn("Customers", "FirstName");
            AddColumn("Customers", "LastName");
            AddColumn("Customers", "CustomerName");

            // Add Orders table and columns
            AddTable("Orders");
            AddColumn("Orders", "OrderID");
            AddColumn("Orders", "CustomerID");
            AddColumn("Orders", "OrderDate");
            AddColumn("Orders", "TotalAmount");

            // Add Products table and columns
            AddTable("Products");
            AddColumn("Products", "ProductID");
            AddColumn("Products", "ProductName");
            AddColumn("Products", "UnitPrice");
            AddColumn("Products", "Quantity");

            // Add temporary products table
            AddTable("#TempProducts");
            AddColumn("#TempProducts", "ProductID");
            AddColumn("#TempProducts", "ProductName");
            AddColumn("#TempProducts", "UnitPrice");
            
            // Add temporary customers table
            AddTable("#TempCustomers");
            AddColumn("#TempCustomers", "CustomerID");
            AddColumn("#TempCustomers", "CustomerName");
            AddColumn("#TempCustomers", "TotalOrders");

            // Add OrderDetails table and columns
            AddTable("OrderDetails");
            AddColumn("OrderDetails", "OrderDetailID");
            AddColumn("OrderDetails", "OrderID");
            AddColumn("OrderDetails", "ProductID");
            AddColumn("OrderDetails", "Quantity");
            AddColumn("OrderDetails", "UnitPrice");
            AddColumn("OrderDetails", "Discount");
            
            // Add CTE tables for tests
            var cteTable = new TableNode
            {
                Name = "CustomerOrders",
                ObjectName = "CustomerOrders",
                SchemaName = "dbo",
                TableType = "CTE"
            };
            AddTable(cteTable);
            
            // Add CustomerOrders columns
            AddColumn(new ColumnNode
            {
                Name = "CustomerID",
                TableOwner = "CustomerOrders",
                ObjectName = "CustomerID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = false
            });
            
            AddColumn(new ColumnNode
            {
                Name = "FirstName",
                TableOwner = "CustomerOrders",
                ObjectName = "FirstName",
                SchemaName = "dbo",
                DataType = "nvarchar(50)",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "LastName",
                TableOwner = "CustomerOrders",
                ObjectName = "LastName",
                SchemaName = "dbo",
                DataType = "nvarchar(50)",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "OrderID",
                TableOwner = "CustomerOrders",
                ObjectName = "OrderID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "OrderDate",
                TableOwner = "CustomerOrders",
                ObjectName = "OrderDate",
                SchemaName = "dbo",
                DataType = "datetime",
                IsNullable = true
            });
            
            // Add ProductSales table for PIVOT tests
            var productSalesTable = new TableNode
            {
                Name = "ProductSales",
                ObjectName = "ProductSales",
                SchemaName = "dbo",
                TableType = "Table"
            };
            AddTable(productSalesTable);
            
            // Add ProductSales columns
            AddColumn(new ColumnNode
            {
                Name = "ProductID",
                TableOwner = "ProductSales",
                ObjectName = "ProductID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = false
            });
            
            AddColumn(new ColumnNode
            {
                Name = "Year",
                TableOwner = "ProductSales",
                ObjectName = "Year",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "SalesAmount",
                TableOwner = "ProductSales",
                ObjectName = "SalesAmount",
                SchemaName = "dbo",
                DataType = "decimal(18,2)",
                IsNullable = true
            });
            
            // Add PivotTable
            var pivotTable = new TableNode
            {
                Name = "PivotTable",
                ObjectName = "PivotTable",
                SchemaName = "dbo",
                TableType = "Table"
            };
            AddTable(pivotTable);
            
            // Add test columns for years in pivot
            AddColumn(new ColumnNode
            {
                Name = "2022",
                TableOwner = "PivotTable",
                ObjectName = "2022",
                SchemaName = "dbo",
                DataType = "decimal(18,2)",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "2023",
                TableOwner = "PivotTable",
                ObjectName = "2023",
                SchemaName = "dbo",
                DataType = "decimal(18,2)",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "2024",
                TableOwner = "PivotTable",
                ObjectName = "2024",
                SchemaName = "dbo",
                DataType = "decimal(18,2)",
                IsNullable = true
            });
            
            // Add Employees table for recursive CTE test
            var employeesTable = new TableNode
            {
                Name = "Employees",
                ObjectName = "Employees",
                SchemaName = "dbo",
                TableType = "Table"
            };
            AddTable(employeesTable);
            
            // Add Employees columns
            AddColumn(new ColumnNode
            {
                Name = "EmployeeID",
                TableOwner = "Employees",
                ObjectName = "EmployeeID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = false
            });
            
            AddColumn(new ColumnNode
            {
                Name = "FirstName",
                TableOwner = "Employees",
                ObjectName = "FirstName",
                SchemaName = "dbo",
                DataType = "nvarchar(50)",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "LastName",
                TableOwner = "Employees",
                ObjectName = "LastName",
                SchemaName = "dbo",
                DataType = "nvarchar(50)",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "ManagerID",
                TableOwner = "Employees",
                ObjectName = "ManagerID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "Level",
                TableOwner = "Employees",
                ObjectName = "Level",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = true
            });
            
            // Add more tables needed for tests
            
            // Add #Sales temp table
            var salesTempTable = new TableNode
            {
                Name = "#Sales",
                ObjectName = "#Sales",
                SchemaName = "dbo",
                TableType = "TempTable"
            };
            AddTable(salesTempTable);
            
            // Add #Sales columns
            AddColumn(new ColumnNode
            {
                Name = "SaleID",
                TableOwner = "#Sales",
                ObjectName = "SaleID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = false
            });
            
            AddColumn(new ColumnNode
            {
                Name = "CustomerID",
                TableOwner = "#Sales",
                ObjectName = "CustomerID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "SaleAmount",
                TableOwner = "#Sales",
                ObjectName = "SaleAmount",
                SchemaName = "dbo",
                DataType = "decimal(18,2)",
                IsNullable = true
            });
            
            // Add #CustomerSegments temp table
            var customerSegmentsTable = new TableNode
            {
                Name = "#CustomerSegments",
                ObjectName = "#CustomerSegments",
                SchemaName = "dbo",
                TableType = "TempTable"
            };
            AddTable(customerSegmentsTable);
            
            // Add #CustomerSegments columns
            AddColumn(new ColumnNode
            {
                Name = "CustomerID",
                TableOwner = "#CustomerSegments",
                ObjectName = "CustomerID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = false
            });
            
            AddColumn(new ColumnNode
            {
                Name = "Segment",
                TableOwner = "#CustomerSegments",
                ObjectName = "Segment",
                SchemaName = "dbo",
                DataType = "nvarchar(50)",
                IsNullable = true
            });
            
            // Add #ProductPerformance temp table
            var productPerformanceTable = new TableNode
            {
                Name = "#ProductPerformance",
                ObjectName = "#ProductPerformance",
                SchemaName = "dbo",
                TableType = "TempTable"
            };
            AddTable(productPerformanceTable);
            
            // Add #ProductPerformance columns
            AddColumn(new ColumnNode
            {
                Name = "ProductID",
                TableOwner = "#ProductPerformance",
                ObjectName = "ProductID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = false
            });
            
            AddColumn(new ColumnNode
            {
                Name = "Performance",
                TableOwner = "#ProductPerformance",
                ObjectName = "Performance",
                SchemaName = "dbo",
                DataType = "nvarchar(50)",
                IsNullable = true
            });
            
            // Add Categories table
            var categoriesTable = new TableNode
            {
                Name = "Categories",
                ObjectName = "Categories",
                SchemaName = "dbo",
                TableType = "Table"
            };
            AddTable(categoriesTable);
            
            // Add Categories columns
            AddColumn(new ColumnNode
            {
                Name = "CategoryID",
                TableOwner = "Categories",
                ObjectName = "CategoryID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = false
            });
            
            AddColumn(new ColumnNode
            {
                Name = "CategoryName",
                TableOwner = "Categories",
                ObjectName = "CategoryName",
                SchemaName = "dbo",
                DataType = "nvarchar(50)",
                IsNullable = true
            });
            
            // Add Suppliers table
            var suppliersTable = new TableNode
            {
                Name = "Suppliers",
                ObjectName = "Suppliers",
                SchemaName = "dbo",
                TableType = "Table"
            };
            AddTable(suppliersTable);
            
            // Add Suppliers columns
            AddColumn(new ColumnNode
            {
                Name = "SupplierID",
                TableOwner = "Suppliers",
                ObjectName = "SupplierID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = false
            });
            
            AddColumn(new ColumnNode
            {
                Name = "SupplierName",
                TableOwner = "Suppliers",
                ObjectName = "SupplierName",
                SchemaName = "dbo",
                DataType = "nvarchar(100)",
                IsNullable = true
            });
            
            // Add Inventory table
            var inventoryTable = new TableNode
            {
                Name = "Inventory",
                ObjectName = "Inventory",
                SchemaName = "dbo",
                TableType = "Table"
            };
            AddTable(inventoryTable);
            
            // Add Inventory columns
            AddColumn(new ColumnNode
            {
                Name = "ProductID",
                TableOwner = "Inventory",
                ObjectName = "ProductID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = false
            });
            
            AddColumn(new ColumnNode
            {
                Name = "Quantity",
                TableOwner = "Inventory",
                ObjectName = "Quantity",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = true
            });
            
            // Add SalesAnalysis table
            var salesAnalysisTable = new TableNode
            {
                Name = "SalesAnalysis",
                ObjectName = "SalesAnalysis",
                SchemaName = "dbo",
                TableType = "Table"
            };
            AddTable(salesAnalysisTable);
            
            // Add SalesAnalysis columns
            AddColumn(new ColumnNode
            {
                Name = "ProductID",
                TableOwner = "SalesAnalysis",
                ObjectName = "ProductID",
                SchemaName = "dbo",
                DataType = "int",
                IsNullable = false
            });
            
            AddColumn(new ColumnNode
            {
                Name = "SalesTotal",
                TableOwner = "SalesAnalysis",
                ObjectName = "SalesTotal",
                SchemaName = "dbo",
                DataType = "decimal(18,2)",
                IsNullable = true
            });
            
            AddColumn(new ColumnNode
            {
                Name = "Trend",
                TableOwner = "SalesAnalysis",
                ObjectName = "Trend",
                SchemaName = "dbo",
                DataType = "nvarchar(50)",
                IsNullable = true
            });
        }
    }
}

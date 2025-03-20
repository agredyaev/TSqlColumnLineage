using System;
using System.Linq;
using FluentAssertions;
using TSqlColumnLineage.Core.Domain.Graph;
using Xunit;

namespace TSqlColumnLineage.Tests.Domain.Graph
{
    public class LineageGraphTests
    {
        [Fact]
        public void LineageGraph_ShouldInitializeCorrectly()
        {
            // Arrange & Act
            var graph = new LineageGraph();
            
            // Assert
            graph.Should().NotBeNull();
            var stats = graph.GetStatistics();
            stats.TotalNodes.Should().Be(0);
            stats.TotalEdges.Should().Be(0);
            stats.ColumnNodes.Should().Be(0);
            stats.TableNodes.Should().Be(0);
            stats.ExpressionNodes.Should().Be(0);
        }
        
        [Fact]
        public void AddColumnNode_ShouldCreateAndReturnValidId()
        {
            // Arrange
            var graph = new LineageGraph();
            
            // Act
            int columnId = graph.AddColumnNode("CustomerId", "Customer", "int", false, false);
            
            // Assert
            columnId.Should().BeGreaterOrEqualTo(0);
            var stats = graph.GetStatistics();
            stats.TotalNodes.Should().Be(1);
            stats.ColumnNodes.Should().Be(1);
        }
        
        [Fact]
        public void AddTableNode_ShouldCreateAndReturnValidId()
        {
            // Arrange
            var graph = new LineageGraph();
            
            // Act
            int tableId = graph.AddTableNode("Customer", "Table");
            
            // Assert
            tableId.Should().BeGreaterOrEqualTo(0);
            var stats = graph.GetStatistics();
            stats.TotalNodes.Should().Be(1);
            stats.TableNodes.Should().Be(1);
        }
        
        [Fact]
        public void AddExpressionNode_ShouldCreateAndReturnValidId()
        {
            // Arrange
            var graph = new LineageGraph();
            
            // Act
            int expressionId = graph.AddExpressionNode("TotalAmount", "SUM(OrderAmount)", "Aggregation", "decimal");
            
            // Assert
            expressionId.Should().BeGreaterOrEqualTo(0);
            var stats = graph.GetStatistics();
            stats.TotalNodes.Should().Be(1);
            stats.ExpressionNodes.Should().Be(1);
        }
        
        [Fact]
        public void AddDirectLineage_ShouldCreateEdgeBetweenColumns()
        {
            // Arrange
            var graph = new LineageGraph();
            int sourceId = graph.AddColumnNode("CustomerName", "CustomerSource", "nvarchar");
            int targetId = graph.AddColumnNode("Name", "CustomerTarget", "nvarchar");
            
            // Act
            int edgeId = graph.AddDirectLineage(sourceId, targetId, "SELECT", "SELECT Name FROM CustomerSource");
            
            // Assert
            edgeId.Should().BeGreaterOrEqualTo(0);
            var stats = graph.GetStatistics();
            stats.TotalEdges.Should().Be(1);
            stats.DirectEdges.Should().Be(1);
            
            // Check lineage paths
            var paths = graph.GetLineagePaths(sourceId, targetId);
            paths.Should().NotBeEmpty();
            paths.Should().HaveCount(1);
            paths[0].SourceId.Should().Be(sourceId);
            paths[0].TargetId.Should().Be(targetId);
            paths[0].Edges.Should().HaveCount(1);
        }
        
        [Fact]
        public void AddMultipleNodesAndEdges_ShouldCreateComplexLineage()
        {
            // Arrange
            var graph = new LineageGraph();
            
            // Create table nodes
            int sourceTableId = graph.AddTableNode("Orders", "Table");
            int targetTableId = graph.AddTableNode("OrderSummary", "Table");
            int intermediateTableId = graph.AddTableNode("OrderDetails", "Table");
            
            // Create column nodes
            int sourceColumnId = graph.AddColumnNode("OrderAmount", "Orders", "decimal");
            int intermediateColumnId = graph.AddColumnNode("Amount", "OrderDetails", "decimal");
            int targetColumnId = graph.AddColumnNode("TotalAmount", "OrderSummary", "decimal");
            
            // Create expression node
            int expressionId = graph.AddExpressionNode("SumAmount", "SUM(Amount)", "Aggregation", "decimal");
            
            // Add columns to tables
            graph.AddColumnToTable(sourceTableId, sourceColumnId);
            graph.AddColumnToTable(intermediateTableId, intermediateColumnId);
            graph.AddColumnToTable(targetTableId, targetColumnId);
            
            // Create lineage
            graph.AddDirectLineage(sourceColumnId, intermediateColumnId, "INSERT", "INSERT INTO OrderDetails SELECT OrderAmount FROM Orders");
            graph.AddIndirectLineage(intermediateColumnId, expressionId, "Aggregation", "SUM(Amount)");
            graph.AddDirectLineage(expressionId, targetColumnId, "INSERT", "INSERT INTO OrderSummary SELECT SUM(Amount) FROM OrderDetails");
            
            // Act
            var paths = graph.GetLineagePaths(sourceColumnId, targetColumnId);
            var stats = graph.GetStatistics();
            
            // Assert
            paths.Should().NotBeEmpty();
            paths.First().Edges.Count.Should().BeGreaterThan(1);
            stats.TotalNodes.Should().Be(6);
            stats.TotalEdges.Should().BeGreaterThan(2);
            
            // Check source columns
            var sourceColumns = graph.GetSourceColumns(targetColumnId);
            sourceColumns.Should().NotBeEmpty();
            
            // Check target columns
            var targetColumns = graph.GetTargetColumns(sourceColumnId);
            targetColumns.Should().NotBeEmpty();
        }
        
        [Fact]
        public void GetColumnNode_ShouldReturnCorrectNodeId()
        {
            // Arrange
            var graph = new LineageGraph();
            string tableName = "Customer";
            string columnName = "CustomerId";
            int originalId = graph.AddColumnNode(columnName, tableName, "int");
            
            // Act
            int retrievedId = graph.GetColumnNode(tableName, columnName);
            
            // Assert
            retrievedId.Should().Be(originalId);
        }
        
        [Fact]
        public void GetNodeData_ShouldReturnCorrectNodeInformation()
        {
            // Arrange
            var graph = new LineageGraph();
            string tableName = "Customer";
            string columnName = "CustomerId";
            string dataType = "int";
            int nodeId = graph.AddColumnNode(columnName, tableName, dataType);
            
            // Act
            var nodeData = graph.GetNodeData(nodeId);
            
            // Assert
            nodeData.Should().NotBeNull();
            nodeData.Name.Should().Be(columnName);
            nodeData.Type.Should().Be(NodeType.Column);
            nodeData.ColumnData.Should().NotBeNull();
            nodeData.ColumnData!.DataType.Should().Be(dataType);
            nodeData.ColumnData!.TableOwner.Should().Be(tableName);
        }
        
        [Fact]
        public void SetMetadata_ShouldStoreAndRetrieveCorrectly()
        {
            // Arrange
            var graph = new LineageGraph();
            string key = "TestKey";
            string value = "TestValue";
            
            // Act
            graph.SetMetadata(key, value);
            var retrievedValue = graph.GetMetadata(key);
            
            // Assert
            retrievedValue.Should().Be(value);
        }
    }
}
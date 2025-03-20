using System;
using FluentAssertions;
using Moq;
using TSqlColumnLineage.Core.Domain.Context;
using TSqlColumnLineage.Core.Domain.Graph;
using Xunit;

namespace TSqlColumnLineage.Tests.Domain.Context
{
    public class ContextManagerTests
    {
        private LineageGraph CreateMockGraph()
        {
            return new LineageGraph(100, 100);
        }
        
        [Fact]
        public void Constructor_ShouldInitializeCorrectly()
        {
            // Arrange
            var graph = CreateMockGraph();
            
            // Act
            var manager = new ContextManager(graph);
            
            // Assert
            manager.Should().NotBeNull();
            manager.Graph.Should().BeSameAs(graph);
            manager.ShouldStop.Should().BeFalse();
        }
        
        [Fact]
        public void Constructor_WithNullGraph_ShouldThrowArgumentNullException()
        {
            // Arrange & Act
            Action act = () => new ContextManager(null!);
            
            // Assert
            act.Should().Throw<ArgumentNullException>()
                .Which.ParamName.Should().Be("graph");
        }
        
        [Fact]
        public void PushScope_ShouldCreateNewScope()
        {
            // Arrange
            var manager = new ContextManager(CreateMockGraph());
            var scopeType = ScopeType.Procedure;
            var name = "TestProcedure";
            
            // Act
            manager.PushScope(scopeType, name);
            
            // Act & Assert - verify by using CreateScope and checking that we can pop twice
            using (var scope = manager.CreateScope(ScopeType.Block))
            {
                // Inner scope created
            }
            // Inner scope popped
            
            manager.PopScope(); // Should pop the TestProcedure scope
            
            // Verify we're back at global scope
            manager.GetVariable("GlobalVar").Should().BeNull();
            
            // Action to pop again (which should be a no-op since we're at global scope)
            Action act = () => manager.PopScope();
            act.Should().NotThrow(); // Global scope can't be popped, but it shouldn't throw
        }
        
        [Fact]
        public void CreateScope_ShouldCreateDisposableScopeThatAutomaticallyPops()
        {
            // Arrange
            var manager = new ContextManager(CreateMockGraph());
            var globalVar = "GlobalVar";
            var localVar = "LocalVar";
            
            // Act - declare a global variable
            manager.DeclareVariable(globalVar, "int", 42);
            
            // Create a scope and declare a local variable
            using (var scope = manager.CreateScope(ScopeType.Block, "TestBlock"))
            {
                manager.DeclareVariable(localVar, "int", 100);
                
                // Both variables should be accessible
                manager.GetVariable(globalVar).Should().NotBeNull();
                manager.GetVariable(localVar).Should().NotBeNull();
            }
            
            // Assert - after scope is disposed, local variable should not be accessible
            manager.GetVariable(globalVar).Should().NotBeNull();
            manager.GetVariable(localVar).Should().BeNull();
        }
        
        [Fact]
        public void Variables_ShouldRespectScopeChain()
        {
            // Arrange
            var manager = new ContextManager(CreateMockGraph());
            
            // Act - set up variables in different scopes
            manager.DeclareVariable("GlobalVar", "int", 42);
            
            using (var scope1 = manager.CreateScope(ScopeType.Procedure, "Proc1"))
            {
                manager.DeclareVariable("Proc1Var", "int", 100);
                
                using (var scope2 = manager.CreateScope(ScopeType.Block, "Block1"))
                {
                    manager.DeclareVariable("BlockVar", "int", 200);
                    
                    // All variables should be accessible in innermost scope
                    manager.GetVariable("GlobalVar").Should().NotBeNull();
                    manager.GetVariable("Proc1Var").Should().NotBeNull();
                    manager.GetVariable("BlockVar").Should().NotBeNull();
                    
                    // Modify a variable from outer scope
                    manager.SetVariable("Proc1Var", 150);
                }
                
                // Assert after inner block scope ends
                manager.GetVariable("GlobalVar").Should().NotBeNull();
                manager.GetVariable("Proc1Var").Should().NotBeNull();
                manager.GetVariable("Proc1Var")?.Value.Should().Be(150); // Value should be updated
                manager.GetVariable("BlockVar").Should().BeNull(); // Block var should be gone
            }
            
            // Assert after procedure scope ends
            manager.GetVariable("GlobalVar").Should().NotBeNull();
            manager.GetVariable("Proc1Var").Should().BeNull();
            manager.GetVariable("BlockVar").Should().BeNull();
        }
        
        [Fact]
        public void AddAndResolveTableAlias_ShouldWorkCorrectly()
        {
            // Arrange
            var manager = new ContextManager(CreateMockGraph());
            var tableName = "dbo.Customers";
            var alias = "c";
            
            // Act
            manager.AddTableAlias(alias, tableName);
            var resolved = manager.ResolveTableAlias(alias);
            
            // Assert
            resolved.Should().Be(tableName);
            
            // Original name should resolve to itself
            manager.ResolveTableAlias(tableName).Should().Be(tableName);
        }
        
        [Fact]
        public void RegisterAndGetTableId_ShouldWorkCorrectly()
        {
            // Arrange
            var manager = new ContextManager(CreateMockGraph());
            var tableName = "dbo.Customers";
            var tableId = 42;
            
            // Act
            manager.RegisterTable(tableName, tableId);
            var retrievedId = manager.GetTableId(tableName);
            
            // Assert
            retrievedId.Should().Be(tableId);
            
            // Non-existent table should return -1
            manager.GetTableId("NonExistentTable").Should().Be(-1);
        }
        
        [Fact]
        public void RegisterCte_ShouldRegisterCommonTableExpression()
        {
            // Arrange
            var manager = new ContextManager(CreateMockGraph());
            var cteName = "CustomerCTE";
            var cteId = 100;
            
            // Act
            manager.RegisterCte(cteName, cteId);
            var retrievedId = manager.GetTableId(cteName);
            
            // Assert
            retrievedId.Should().Be(cteId);
        }
        
        [Fact]
        public void SetAndGetState_ShouldStoreAndRetrieveValues()
        {
            // Arrange
            var manager = new ContextManager(CreateMockGraph());
            var key = "StateKey";
            var value = "StateValue";
            
            // Act
            manager.SetState(key, value);
            var retrieved = manager.GetState(key);
            
            // Assert
            retrieved.Should().Be(value);
            
            // Boolean state
            manager.SetState("BoolKey", true);
            manager.GetBoolState("BoolKey").Should().BeTrue();
            manager.GetBoolState("NonExistentKey").Should().BeFalse(); // Default is false
            manager.GetBoolState("NonExistentKey", true).Should().BeTrue(); // Explicit default
        }
        
        [Fact]
        public void StopProcessing_ShouldSetShouldStopToTrue()
        {
            // Arrange
            var manager = new ContextManager(CreateMockGraph());
            
            // Act
            manager.StopProcessing();
            
            // Assert
            manager.ShouldStop.Should().BeTrue();
            manager.CancellationToken.IsCancellationRequested.Should().BeTrue();
        }
        
        [Fact]
        public void GetAllTables_ShouldReturnAllRegisteredTables()
        {
            // Arrange
            var manager = new ContextManager(CreateMockGraph());
            manager.RegisterTable("dbo.Customers", 1);
            manager.RegisterTable("dbo.Orders", 2);
            manager.RegisterTable("#TempTable", 3);
            manager.RegisterTable("@TableVar", 4);
            manager.RegisterCte("OrderCTE", 5);
            
            // Act
            var allTables = manager.GetAllTables();
            
            // Assert
            allTables.Should().HaveCount(5);
            allTables.Should().ContainKeys("dbo.Customers", "dbo.Orders", "#TempTable", "@TableVar", "OrderCTE");
            allTables["dbo.Customers"].Should().Be(1);
            allTables["dbo.Orders"].Should().Be(2);
            allTables["#TempTable"].Should().Be(3);
            allTables["@TableVar"].Should().Be(4);
            allTables["OrderCTE"].Should().Be(5);
        }
    }
}
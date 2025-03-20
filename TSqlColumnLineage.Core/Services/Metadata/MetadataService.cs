using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Services.Metadata
{
    public class MetadataService : IMetadataService
    {
        public IEnumerable<ColumnMetadata> GetTableColumnsMetadata(string tableName)
        {
            // Dummy metadata for now
            if (tableName == "Customers")
            {
                return new List<ColumnMetadata>
                {
                    new ColumnMetadata { Name = "CustomerID", DataType = "INT" },
                    new ColumnMetadata { Name = "FirstName", DataType = "VARCHAR" },
                    new ColumnMetadata { Name = "LastName", DataType = "VARCHAR" },
                    new ColumnMetadata { Name = "OrderCount", DataType = "INT" }
                };
            }
            return new List<ColumnMetadata>();
        }
    }
}

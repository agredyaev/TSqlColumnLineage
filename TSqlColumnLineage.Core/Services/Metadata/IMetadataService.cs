using System.Collections.Generic;

namespace TSqlColumnLineage.Core.Services.Metadata
{
    public interface IMetadataService
    {
        IEnumerable<ColumnMetadata> GetTableColumnsMetadata(string tableName);
    }

    public class ColumnMetadata
    {
        public string Name { get; set; }
        public string DataType { get; set; }
    }
}

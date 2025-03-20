using TSqlColumnLineage.Core.Parsing;

namespace TSqlColumnLineage.Core.Services.Lineage
{
    public class LineageServiceOptions
    {
        public SqlServerVersion SqlServerVersion { get; set; } = SqlServerVersion.Latest;
    }
}

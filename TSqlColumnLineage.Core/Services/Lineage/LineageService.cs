using TSqlColumnLineage.Core.Models.Graph;
using TSqlColumnLineage.Core.Parsing;

namespace TSqlColumnLineage.Core.Services.Lineage
{
    public class LineageService
    {
        private readonly LineageServiceOptions _options;

        public LineageService(LineageServiceOptions options = null)
        {
            _options = options ?? new LineageServiceOptions();
        }

        public LineageGraph GetLineage(string sqlQuery)
        {
            var parser = SqlParserFactory.CreateParser(_options.SqlServerVersion);
            parser.Parse(sqlQuery);
            return parser.LineageContext.Graph;
        }
    }
}

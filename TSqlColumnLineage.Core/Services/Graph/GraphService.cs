using Newtonsoft.Json;
using TSqlColumnLineage.Core.Models.Graph;

namespace TSqlColumnLineage.Core.Services.Graph
{
    public class GraphService
    {
        private readonly SerializationOptions _options;

        public GraphService(SerializationOptions options = null)
        {
            _options = options ?? new SerializationOptions();
        }

        public string SerializeGraphToJson(LineageGraph graph)
        {
            return JsonConvert.SerializeObject(graph, Formatting.Indented, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                DefaultValueHandling = DefaultValueHandling.Ignore
            });
        }

        public LineageGraph DeserializeGraphFromJson(string json)
        {
            return JsonConvert.DeserializeObject<LineageGraph>(json);
        }
    }
}

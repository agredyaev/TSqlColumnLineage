using System.IO;

namespace TSqlColumnLineage.Core.Common.Utils
{
    public class SqlScriptUtils
    {
        public static string ReadSqlScriptFromFile(string filePath)
        {
            return File.ReadAllText(filePath);
        }
    }
}

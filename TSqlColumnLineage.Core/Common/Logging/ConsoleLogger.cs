using System;

namespace TSqlColumnLineage.Core.Common.Logging
{
    public class ConsoleLogger : ILogger
    {
        public void LogDebug(string message)
        {
            Console.WriteLine($"Debug: {message}");
        }

        public void LogError(Exception exception, string message)
        {
            Console.WriteLine($"Error: {message} Exception: {exception}");
        }

        public void LogInformation(string message)
        {
            Console.WriteLine($"Info: {message}");
        }

        public void LogWarning(string message)
        {
            Console.WriteLine($"Warning: {message}");
        }
    }
}

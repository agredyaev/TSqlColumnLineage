using System;

namespace TSqlColumnLineage.Core.Visitors
{
    /// <summary>
    /// Simple console logger implementation
    /// </summary>
    public class ConsoleLogger : ILogger
    {
        private readonly bool _includeDebug;

        public ConsoleLogger(bool includeDebug = false)
        {
            _includeDebug = includeDebug;
        }

        public void LogError(Exception ex, string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"ERROR: {message}");
            if (ex != null)
            {
                Console.WriteLine($"Exception: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
            Console.ResetColor();
        }

        public void LogInformation(string message)
        {
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine($"INFO: {message}");
            Console.ResetColor();
        }

        public void LogDebug(string message)
        {
            if (_includeDebug)
            {
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.WriteLine($"DEBUG: {message}");
                Console.ResetColor();
            }
        }

        public void LogWarning(string message)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"WARNING: {message}");
            Console.ResetColor();
        }
    }
}

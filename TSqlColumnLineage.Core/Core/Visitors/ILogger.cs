using System;

namespace TSqlColumnLineage.Core.Visitors
{
    /// <summary>
    /// Interface for logging
    /// </summary>
    public interface ILogger
    {
        void LogError(Exception ex, string message);
        void LogInformation(string message);
        void LogDebug(string message);
        void LogWarning(string message);
    }
}

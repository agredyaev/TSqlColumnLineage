using System;

namespace TSqlColumnLineage.Core.Common.Logging
{
    public interface ILogger
    {
        void LogError(Exception exception, string message);
        void LogWarning(string message);
        void LogInformation(string message);
        void LogDebug(string message);
    }
}

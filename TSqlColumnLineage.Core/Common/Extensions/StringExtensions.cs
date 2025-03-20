namespace TSqlColumnLineage.Core.Common.Extensions
{
    public static class StringExtensions
    {
        /// <summary>
        /// Checks if a string is null or empty after trimming whitespace.
        /// </summary>
        /// <param name="str">The string to check.</param>
        /// <returns>True if the string is null or whitespace, false otherwise.</returns>
        public static bool IsNullOrWhitespace(this string str)
        {
            return string.IsNullOrWhiteSpace(str);
        }

        /// <summary>
        /// Checks if a string is not null and not empty after trimming whitespace.
        /// </summary>
        /// <param name="str">The string to check.</param>
        /// <returns>True if the string is not null or whitespace, false otherwise.</returns>
        public static bool IsNotNullOrWhitespace(this string str)
        {
            return !string.IsNullOrWhiteSpace(str);
        }
    }
}

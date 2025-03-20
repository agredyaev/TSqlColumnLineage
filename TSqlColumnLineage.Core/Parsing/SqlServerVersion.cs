namespace TSqlColumnLineage.Core.Parsing
{
    /// <summary>
    /// Supported SQL Server versions for parsing
    /// </summary>
    public enum SqlServerVersion
    {
        /// <summary>
        /// SQL Server 2016
        /// </summary>
        SqlServer2016 = 130,
        
        /// <summary>
        /// SQL Server 2017
        /// </summary>
        SqlServer2017 = 140,
        
        /// <summary>
        /// SQL Server 2019
        /// </summary>
        SqlServer2019 = 150,
        
        /// <summary>
        /// SQL Server 2022
        /// </summary>
        SqlServer2022 = 160,
        
        /// <summary>
        /// Latest supported version (currently SQL Server 2022)
        /// </summary>
        Latest = SqlServer2022
    }
}
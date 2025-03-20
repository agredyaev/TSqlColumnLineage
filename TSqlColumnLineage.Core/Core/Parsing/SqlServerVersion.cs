namespace TSqlColumnLineage.Core.Parsing
{
    /// <summary>
    /// Supported SQL Server versions
    /// </summary>
    public enum SqlServerVersion
    {
        SqlServer2016 = 130,
        SqlServer2017 = 140,
        SqlServer2019 = 150,
        SqlServer2022 = 160,
        Latest = SqlServer2022
    }
}

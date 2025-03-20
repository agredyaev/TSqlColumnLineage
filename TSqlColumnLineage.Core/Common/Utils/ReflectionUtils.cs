using System;
using System.Reflection;

namespace TSqlColumnLineage.Core.Common.Utils
{
    public class ReflectionUtils
    {
        public static object GetPropertyValue(object obj, string propertyName)
        {
            if (obj == null || string.IsNullOrEmpty(propertyName))
            {
                return null;
            }

            PropertyInfo propertyInfo = obj.GetType().GetProperty(propertyName);
            return propertyInfo?.GetValue(obj, null);
        }
    }
}

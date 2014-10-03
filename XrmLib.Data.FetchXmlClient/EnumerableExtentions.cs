using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace XrmLib.Data.FetchXmlClient
{
    public static class EnumerableExtentions
    {
        public static IEnumerable<T> Concat<T>(this IEnumerable<T> first, params T[] second)
        {
            return Enumerable.Concat(first, second);
        }
    }
}

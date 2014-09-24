using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace XrmLib.Data.FetchXmlClient
{
    public sealed class FetchXmlClientFactory : DbProviderFactory
    {
        public static readonly FetchXmlClientFactory Instance = new FetchXmlClientFactory();

        private FetchXmlClientFactory()
        {
        }

        public override DbConnection CreateConnection()
        {
            return new FetchXmlConnection();
        }

        public override DbCommand CreateCommand()
        {
            return new FetchXmlCommand();
        }
    }
}

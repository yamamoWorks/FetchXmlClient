using Microsoft.Xrm.Client;
using Microsoft.Xrm.Client.Services;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace XrmLib.Data.FetchXmlClient
{
    public sealed class FetchXmlConnection : DbConnection
    {
        private string connectionString;
        private CrmConnection crmConnection;

        public FetchXmlConnection()
        {
            this.crmConnection = new CrmConnection();
        }

        public FetchXmlConnection(string connectionStringName)
        {
            this.crmConnection = new CrmConnection(connectionStringName);
        }

        public FetchXmlConnection(ConnectionStringSettings connectionString)
        {
            this.crmConnection = new CrmConnection(connectionString);
        }

        internal OrganizationService OrganizationService
        {
            get;
            set;
        }

        protected override DbProviderFactory DbProviderFactory
        {
            get { return FetchXmlClientFactory.Instance; }
        }

        public override string ConnectionString
        {
            get { return this.connectionString; }
            set
            {
                this.connectionString = value;
                this.crmConnection = CrmConnection.Parse(value);
            }
        }

        public override int ConnectionTimeout
        {
            get { return this.crmConnection.Timeout.HasValue ? (int)this.crmConnection.Timeout.Value.TotalSeconds : 0; }
        }

        public override string DataSource
        {
            get { return this.crmConnection.HomeRealmUri.AbsolutePath; }
        }

        public override ConnectionState State
        {
            get { return this.OrganizationService == null ? ConnectionState.Closed : ConnectionState.Open; }
        }

        public override void Open()
        {
            this.OrganizationService = new OrganizationService(this.crmConnection);
        }

        public override void Close()
        {
            if (this.OrganizationService != null)
            {
                this.OrganizationService.Dispose();
                this.OrganizationService = null;
            }
        }

        public new FetchXmlCommand CreateCommand()
        {
            return new FetchXmlCommand { Connection = this };
        }

        protected override DbCommand CreateDbCommand()
        {
            return this.CreateCommand();
        }

        #region IDisposable

        private bool disposed;

        protected override void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    if (this.OrganizationService != null)
                    {
                        this.OrganizationService.Dispose();
                    }
                    this.OrganizationService = null;
                    this.crmConnection = null;
                }
            }
            this.disposed = true;
            base.Dispose(disposing);
        }

        #endregion

        #region NotSupported

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException();
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException();
        }

        public override string Database
        {
            get { return null; }
        }

        public override string ServerVersion
        {
            get { return null; }
        }

        #endregion
    }
}

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
        private ConnectionState state;

        public FetchXmlConnection()
        {
            CrmConnection = new CrmConnection();
        }

        public FetchXmlConnection(string connectionStringName)
        {
            CrmConnection = new CrmConnection(connectionStringName);
        }

        public FetchXmlConnection(ConnectionStringSettings connectionString)
        {
            CrmConnection = new CrmConnection(connectionString);
        }

        public override string ConnectionString
        {
            get { return this.connectionString; }
            set
            {
                this.connectionString = value;
                CrmConnection = CrmConnection.Parse(value);
            }
        }

        public override int ConnectionTimeout
        {
            get { return CrmConnection.Timeout.HasValue ? (int)CrmConnection.Timeout.Value.TotalSeconds : 0; }
        }

        public override string DataSource
        {
            get { return CrmConnection.HomeRealmUri.AbsolutePath; }
        }

        public override ConnectionState State
        {
            get { return this.state; }
        }

        internal CrmConnection CrmConnection
        {
            get;
            private set;
        }

        protected override DbProviderFactory DbProviderFactory
        {
            get { return FetchXmlClientFactory.Instance; }
        }

        public override void Open()
        {
            this.state = ConnectionState.Open;
        }

        public override void Close()
        {
            this.state = ConnectionState.Closed;
        }

        public new FetchXmlCommand CreateCommand()
        {
            return new FetchXmlCommand { Connection = this };
        }

        protected override DbCommand CreateDbCommand()
        {
            return this.CreateCommand();
        }

        internal void SetConnectionState(ConnectionState state)
        {
            this.state = state;
        }

        #region IDisposable

        private bool disposed;

        protected override void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    this.CrmConnection = null;
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

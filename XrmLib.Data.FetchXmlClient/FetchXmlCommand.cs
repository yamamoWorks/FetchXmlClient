using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace XrmLib.Data.FetchXmlClient
{
    public sealed class FetchXmlCommand : DbCommand
    {
        public FetchXmlCommand()
            : this(null, null)
        {
        }

        public FetchXmlCommand(FetchXmlConnection connection)
            : this(null, connection)
        {
        }

        public FetchXmlCommand(string commandText, FetchXmlConnection connection)
        {
            this.CommandText = commandText;
            this.Connection = connection;
            this.UseFormattedValue = true;
        }

        public bool UseFormattedValue
        {
            get;
            set;
        }

        public override string CommandText
        {
            get;
            set;
        }

        public new FetchXmlConnection Connection
        {
            get;
            set;
        }

        public new FetchXmlDataReader ExecuteReader()
        {
            return this.ExecuteReader(CommandBehavior.Default);
        }

        public new FetchXmlDataReader ExecuteReader(CommandBehavior behavior)
        {
            this.Connection.SetConnectionState(ConnectionState.Open);
            return new FetchXmlDataReader(this.Connection, this.CommandText, this.UseFormattedValue);
        }

        protected override DbConnection DbConnection
        {
            get { return this.Connection; }
            set { this.Connection = (FetchXmlConnection)value; }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return this.ExecuteReader(behavior);
        }

        public override bool DesignTimeVisible
        {
            get;
            set;
        }

        public override void Prepare()
        {
        }

        #region NotSupported

        public override void Cancel()
        {
            throw new NotSupportedException();
        }

        public override int CommandTimeout
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }

        public override CommandType CommandType
        {
            get { return CommandType.Text; }
            set { throw new NotSupportedException(); }
        }

        protected override DbParameter CreateDbParameter()
        {
            throw new NotSupportedException();
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get { throw new NotSupportedException(); }
        }

        protected override DbTransaction DbTransaction
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }

        public override int ExecuteNonQuery()
        {
            throw new NotSupportedException();
        }

        public override object ExecuteScalar()
        {
            throw new NotSupportedException();
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }

        #endregion
    }
}

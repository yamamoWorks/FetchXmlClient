using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace XrmLib.Data.FetchXmlClient
{
    public sealed class FetchXmlDataReader : DbDataReader
    {
        private IEnumerator<Entity> enumerator;
        private XElement fetchXml;
        private int pageNumber;
        private string pagingCookie;
        private EntityCollection entityCollection;
        private FetchXmlConnection connection;
        private string[] attribteNames;
        private bool useFormattedValue;

        internal FetchXmlDataReader(FetchXmlConnection connection, string xml, bool useFormattedValue)
        {
            this.useFormattedValue = useFormattedValue;

            this.fetchXml = XElement.Parse(xml);
            this.pageNumber = 1;
            this.pagingCookie = null;
            this.PageCount = 0;

            this.enumerator = null;
            this.connection = connection;

            this.attribteNames =
                this.fetchXml.Elements("entity").Elements("attribute").Select(x => x.Attribute("name").Value).Concat(
                this.fetchXml.Elements("entity").Elements("link-entity").SelectMany(x => x.Elements("attribute").Select(y => x.Attribute("alias").Value + "." + y.Attribute("name").Value))
                ).ToArray();

            this.Fetch();
        }

        public int PageCount { get; set; }

        #region IDataRecord

        public override bool HasRows
        {
            get { return this.entityCollection.TotalRecordCount > 0; }
        }

        public override int FieldCount
        {
            get { return this.attribteNames.Length; }
        }

        public override bool GetBoolean(int i)
        {
            return (bool)this.GetValue(i);
        }

        public override byte GetByte(int i)
        {
            return (byte)this.GetValue(i);
        }

        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            Array.Copy((byte[])this.GetValue(i), fieldOffset, buffer, bufferoffset, length);
            return length;
        }

        public override char GetChar(int i)
        {
            return (char)this.GetValue(i);
        }

        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            Array.Copy(this.GetString(i).ToCharArray(), fieldoffset, buffer, bufferoffset, length);
            return length;
        }

        public override string GetDataTypeName(int i)
        {
            return (string)this.GetValue(i);
        }

        public override DateTime GetDateTime(int i)
        {
            return (DateTime)this.GetValue(i);
        }

        public override decimal GetDecimal(int i)
        {
            return (decimal)this.GetValue(i);
        }

        public override double GetDouble(int i)
        {
            return (double)this.GetValue(i);
        }

        public override Type GetFieldType(int i)
        {
            if (this.entityCollection.Entities.Count() > 0)
            {
                var value = this.entityCollection.Entities.First().Attributes[this.attribteNames[i]];
                if (value != null)
                {
                    return value.GetType();
                }
            }
            return null;
        }

        public override float GetFloat(int i)
        {
            return (float)this.GetValue(i);
        }

        public override Guid GetGuid(int i)
        {
            return (Guid)this.GetValue(i);
        }

        public override short GetInt16(int i)
        {
            return (short)this.GetValue(i);
        }

        public override int GetInt32(int i)
        {
            return (int)this.GetValue(i);
        }

        public override long GetInt64(int i)
        {
            return (long)this.GetValue(i);
        }

        public override string GetName(int i)
        {
            return this.attribteNames[i];
        }

        public override int GetOrdinal(string name)
        {
            return Array.IndexOf<string>(this.attribteNames, name);
        }

        public override string GetString(int i)
        {
            return (string)this.GetValue(i);
        }

        public override object GetValue(int i)
        {
            if (this.enumerator.Current.Attributes.ContainsKey(this.attribteNames[i]))
            {
                return this.GetValue(this.attribteNames[i]);
            }
            return null;
        }

        public override int GetValues(object[] values)
        {
            var n = Math.Min(values.Length, this.attribteNames.Length);
            for (int i = 0; i < n; i++)
            {
                values[i] = this.GetValue(i);
            }
            return n;
        }

        public override bool IsDBNull(int i)
        {
            return this.enumerator.Current.Attributes[this.attribteNames[i]] == null;
        }

        public override object this[string name]
        {
            get { return this.enumerator.Current.Attributes[name]; }
        }

        public override object this[int i]
        {
            get { return this.GetValue(i); }
        }

        #endregion

        #region IDataReader

        public override DataTable GetSchemaTable()
        {
            throw new NotSupportedException();
        }

        public override int RecordsAffected
        {
            get { return -1; }
        }

        public override int Depth
        {
            get { return 1; }
        }

        public override bool IsClosed
        {
            get { return this.connection.State == ConnectionState.Closed; }
        }

        public override bool Read()
        {
            var hasData = this.enumerator.MoveNext();

            if (!hasData && this.entityCollection.MoreRecords)
            {
                this.Fetch();
                return this.Read();
            }

            return hasData;
        }

        public override void Close()
        {
            this.connection.Close();
        }

        public override bool NextResult()
        {
            return false;
        }

        #endregion

        public override IEnumerator GetEnumerator()
        {
            return this.enumerator;
        }

        private object GetValue(string key)
        {
            var value = this.enumerator.Current.Attributes[key];

            if (this.useFormattedValue)
            {
                if (this.enumerator.Current.FormattedValues.ContainsKey(key))
                {
                    return this.enumerator.Current.FormattedValues[key];
                }

                if (value is EntityReference)
                {
                    return ((EntityReference)value).Name;
                }

                if (value is AliasedValue)
                {
                    return ((AliasedValue)value).Value;
                }
            }

            return value;
        }

        private void Fetch()
        {
            if (this.pagingCookie != null)
            {
                this.fetchXml.SetAttributeValue("paging-cookie", this.pagingCookie);
                this.fetchXml.SetAttributeValue("page", this.pageNumber);
            }

            if (this.PageCount > 0)
            {
                this.fetchXml.SetAttributeValue("count", this.PageCount);
            }

            var fexp = new FetchExpression(this.fetchXml.ToString(SaveOptions.DisableFormatting));
            this.entityCollection = this.connection.OrganizationService.RetrieveMultiple(fexp);
            this.enumerator = this.entityCollection.Entities.GetEnumerator();

            if (this.entityCollection.Entities.Count() > 0)
            {
                this.attribteNames = this.entityCollection.Entities.First().Attributes.Keys.ToArray();
            }

            this.pageNumber++;
            this.pagingCookie = this.entityCollection.PagingCookie;
        }

        #region IDisposable

        private bool disposed;

        protected override void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    if (this.enumerator != null)
                    {
                        this.enumerator.Dispose();
                    }
                }
                this.entityCollection = null;
                this.enumerator = null;
            }
            this.disposed = true;

            base.Dispose(disposing);
        }

        ~FetchXmlDataReader()
        {
            this.Dispose(false);
        }

        #endregion
    }
}
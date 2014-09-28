using Microsoft.Xrm.Client.Services;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace XrmLib.Data.FetchXmlClient
{
    public sealed class FetchXmlDataReader : DbDataReader
    {
        private FetchXmlEnumerator enumerator;
        private FetchXmlConnection connection;
        private string[] attribtes;
        private IDictionary<string, string> aliases;
        private IDictionary<string, AttributeMetadata> metadatas;
        private bool useFormattedValue;

        internal FetchXmlDataReader(FetchXmlConnection connection, string fetchXml, bool useFormattedValue)
        {
            this.useFormattedValue = useFormattedValue;
            this.connection = connection;
            this.Initialize(XElement.Parse(fetchXml));
        }

        public int PageCount { get; set; }

        #region IDataRecord

        public override bool HasRows
        {
            get { return this.enumerator.HasRow; }
        }

        public override int FieldCount
        {
            get { return this.attribtes.Length; }
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
            var meta = this.metadatas[this.GetName(i)];
            if (meta.AttributeTypeName != null)
            {
                // 2013
                return TypeMappingService.GetType(meta.AttributeTypeName, this.useFormattedValue);
            }
            // 2011
            return TypeMappingService.GetType(meta.AttributeType.Value, this.useFormattedValue);
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
            return this.attribtes[i];
        }

        public override int GetOrdinal(string name)
        {
            return Array.IndexOf<string>(this.attribtes, name);
        }

        public override string GetString(int i)
        {
            return (string)this.GetValue(i);
        }

        public override object GetValue(int i)
        {
            if (this.enumerator.Current.Attributes.ContainsKey(this.attribtes[i]))
            {
                return this.GetValue(this.attribtes[i]);
            }
            return null;
        }

        public override int GetValues(object[] values)
        {
            var n = Math.Min(values.Length, this.attribtes.Length);
            for (int i = 0; i < n; i++)
            {
                values[i] = this.GetValue(i);
            }
            return n;
        }

        public override bool IsDBNull(int i)
        {
            return this.enumerator.Current.Attributes[this.attribtes[i]] == null;
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
            var table = new DataTable();
            var columnNameColumn = table.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
            var columnOrdinalColumn = table.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
            var columnSizeColumn = table.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int));
            var dataTypeColumn = table.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
            var isKeyColumn = table.Columns.Add(SchemaTableColumn.IsKey, typeof(bool));

            for (int i = 0; i < this.attribtes.Length; i++)
            {
                var row = table.NewRow();
                row[columnNameColumn] = this.attribtes[i];
                row[columnOrdinalColumn] = i;
                row[columnSizeColumn] = -1;
                row[dataTypeColumn] = this.GetFieldType(i);
                row[isKeyColumn] = this.metadatas[this.attribtes[i]].IsPrimaryId;
                table.Rows.Add(row);
            }

            return table;
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
            return this.enumerator.MoveNext();
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

        private void Initialize(XElement fetchXml)
        {
            var entity = fetchXml.Element("entity");
            var linkEntities = fetchXml.Elements("entity").Elements("link-entity");

            var entityName = entity.Attribute("name").Value;
            var linkEntitiesNames = linkEntities.Select(x => x.Attribute("name").Value);

            this.attribtes =
                entity.Elements("attribute").Attributes("name").Select(x => x.Value).Concat(
                linkEntities.Elements("attribute").Attributes("name").Select(x => x.Parent.Parent.Attribute("alias").Value + "." + x.Value)
                ).ToArray();

            this.aliases = linkEntities.ToDictionary(x => x.Attribute("alias").Value, x => x.Attribute("name").Value);

            var task = this.GetEntityMetadataAsync(linkEntitiesNames.Concat(entityName).ToArray());

            this.enumerator = new FetchXmlEnumerator(new OrganizationService(connection.CrmConnection), fetchXml);

            var metas = task.Result;

            this.metadatas = this.attribtes.ToDictionary(key => key, key =>
            {
                var keyArray = key.Split('.');
                var en = keyArray.Length > 1 ? this.aliases[keyArray.First()] : entityName;
                var an = keyArray.Last();
                return metas[en].Attributes.Single(a => a.LogicalName == an);
            });
        }

        private object GetValue(string key)
        {
            var value = this.enumerator.Current.Attributes[key];

            if (value is AliasedValue)
            {
                value = ((AliasedValue)value).Value;
            }

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
            }

            return value;
        }

        private Task<IDictionary<string, EntityMetadata>> GetEntityMetadataAsync(string[] entityNames)
        {
            return Task.Factory.StartNew<IDictionary<string, EntityMetadata>>(() =>
            {
                return entityNames.AsParallel().Select(name =>
                {
                    using (var service = new OrganizationService(this.connection.CrmConnection))
                    {
                        return service.RetrieveEntity(new RetrieveEntityRequest
                        {
                            LogicalName = name,
                            EntityFilters = EntityFilters.Attributes
                        }).EntityMetadata;
                    }
                })
                .ToDictionary(m => m.LogicalName);
            });
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
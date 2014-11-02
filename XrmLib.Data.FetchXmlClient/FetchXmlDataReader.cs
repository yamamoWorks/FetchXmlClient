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
        private string[] attribtues;
        private IDictionary<string, AttributeMetadata> attributesMetadata;
        private string[] displaryNames;
        private bool useFormattedValue;
        private bool useDisplayName;
        private bool hidePrimaryId;

        internal FetchXmlDataReader(FetchXmlConnection connection, string fetchXml, bool useFormattedValue, bool useDisplayName, bool hidePrimaryId)
        {
            this.hidePrimaryId = hidePrimaryId;
            this.useDisplayName = useDisplayName;
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
            get { return this.attribtues.Length; }
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
            var meta = this.attributesMetadata[this.attribtues[i]];
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
            return this.useDisplayName ? this.displaryNames[i] : this.attribtues[i];
        }

        public override int GetOrdinal(string name)
        {
            return this.useDisplayName ? Array.IndexOf(this.displaryNames, name) : Array.IndexOf(this.attribtues, name);
        }

        public override string GetString(int i)
        {
            return (string)this.GetValue(i);
        }

        public override object GetValue(int i)
        {
            if (this.enumerator.Current.Attributes.ContainsKey(this.attribtues[i]))
            {
                return this.GetValue(this.attribtues[i]);
            }
            return null;
        }

        public override int GetValues(object[] values)
        {
            var n = Math.Min(values.Length, this.attribtues.Length);
            for (int i = 0; i < n; i++)
            {
                values[i] = this.GetValue(i);
            }
            return n;
        }

        public override bool IsDBNull(int i)
        {
            return this.enumerator.Current.Attributes[this.attribtues[i]] == null;
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

            for (int i = 0; i < this.attribtues.Length; i++)
            {
                var row = table.NewRow();
                row[columnNameColumn] = this.GetName(i);
                row[columnOrdinalColumn] = i;
                row[columnSizeColumn] = -1;
                row[dataTypeColumn] = this.GetFieldType(i);
                row[isKeyColumn] = this.attributesMetadata[this.attribtues[i]].IsPrimaryId;
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
            var xMainEntity = fetchXml.Element("entity");
            var xAllEntities = fetchXml.Elements("entity").Concat(fetchXml.Descendants("link-entity"));

            var task = this.GetEntityMetadataAsync(xAllEntities.Select(x => x.Attribute("name").Value).ToArray());

            this.enumerator = new FetchXmlEnumerator(new OrganizationService(connection.CrmConnection), fetchXml);

            var mAllEntities = task.Result.ToDictionary(em => em.LogicalName);

            var attrMetaList = xAllEntities
                .SelectMany(xe =>
                {
                    var em = mAllEntities[xe.Attribute("name").Value];
                    return xe.Elements("attribute").Attributes("name")
                        .Select(xa =>
                        {
                            return new
                            {
                                Name = string.Join(".", xe.Attributes("alias").Select(a => a.Value).Concat(xa.Value)),
                                Meta = em.Attributes.Single(am => am.LogicalName == xa.Value)
                            };
                        });
                })
                .Where(a => !(this.hidePrimaryId && a.Meta.IsPrimaryId.Value));

            this.attribtues = attrMetaList.Select(a => a.Name).ToArray();
            this.attributesMetadata = attrMetaList.ToDictionary(a => a.Name, a => a.Meta);

            var aliases = xAllEntities.Except(xMainEntity)
                .ToDictionary(x => x.Attribute("alias").Value, x => x.Attribute("to").Value);

            var mainEntityMeta = mAllEntities[xMainEntity.Attribute("name").Value];

            this.displaryNames = this.attribtues
                .Select(a =>
                {
                    var am = this.attributesMetadata[a];
                    if (am.EntityLogicalName == mainEntityMeta.LogicalName)
                    {
                        return am.DisplayName.UserLocalizedLabel.Label;
                    }
                    var to = aliases[a.Split('.').First()];
                    var toMeta = mainEntityMeta.Attributes.Single(m => m.LogicalName == to);
                    return string.Format("{0} ({1})",
                        am.DisplayName.UserLocalizedLabel.Label,
                        toMeta.DisplayName.UserLocalizedLabel.Label);
                })
                .ToArray();
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

        private Task<IEnumerable<EntityMetadata>> GetEntityMetadataAsync(string[] entityNames)
        {
            return Task.Factory.StartNew<IEnumerable<EntityMetadata>>(() =>
            {
                return entityNames.Distinct().AsParallel().Select(name =>
                {
                    using (var service = new OrganizationService(this.connection.CrmConnection))
                    {
                        return service.RetrieveEntity(new RetrieveEntityRequest
                        {
                            LogicalName = name,
                            EntityFilters = EntityFilters.Attributes
                        }).EntityMetadata;
                    }
                });
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
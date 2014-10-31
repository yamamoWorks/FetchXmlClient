using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace XrmLib.Data.FetchXmlClient
{
    class FetchXmlEnumerator : IEnumerator<Entity>
    {
        private XElement fetchXml;
        private int page;
        private int maxPageCount;
        private string pagingCookie;
        private IOrganizationService service;
        private EntityCollection entityCollection;
        private IEnumerator<Entity> enumrator;

        public FetchXmlEnumerator(IOrganizationService service, XElement fetchXml, int maxPageCount = 0)
        {
            this.service = service;
            this.fetchXml = fetchXml;
            this.maxPageCount = maxPageCount;
            this.page = 1;
            this.Fetch();
        }

        public Entity Current
        {
            get { return this.enumrator.Current; }
        }

        public void Dispose()
        {
            if (this.enumrator != null) this.enumrator.Dispose();
        }

        object System.Collections.IEnumerator.Current
        {
            get { return this.Current; }
        }

        public bool MoveNext()
        {
            var hasNext = this.enumrator.MoveNext();
            if (!hasNext & this.entityCollection.MoreRecords)
            {
                this.Fetch();
                return this.MoveNext();
            }
            return hasNext;
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        internal bool HasRow
        {
            get { return this.entityCollection.TotalRecordCount > 0; }
        }

        private void Fetch()
        {
            if (this.pagingCookie != null)
            {
                this.fetchXml.SetAttributeValue("paging-cookie", this.pagingCookie);
                this.fetchXml.SetAttributeValue("page", this.page);
            }

            if (this.maxPageCount > 0)
            {
                this.fetchXml.SetAttributeValue("count", this.maxPageCount);
            }

            var fexp = new FetchExpression(this.fetchXml.ToString(SaveOptions.DisableFormatting));
            this.entityCollection = this.service.RetrieveMultiple(fexp);
            this.enumrator = this.entityCollection.Entities.GetEnumerator();

            this.page++;
            this.pagingCookie = entityCollection.PagingCookie;
        }
    }
}

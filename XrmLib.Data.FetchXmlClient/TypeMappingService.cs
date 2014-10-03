using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace XrmLib.Data.FetchXmlClient
{
    static class TypeMappingService
    {
        private static IDictionary<AttributeTypeCode, Type> _typeCodeMappings;
        private static IDictionary<AttributeTypeDisplayName, Type> _typeNameMappings;

        static TypeMappingService()
        {
            _typeCodeMappings = new Dictionary<AttributeTypeCode, Type>
            {
                { AttributeTypeCode.BigInt, typeof(long) },
                { AttributeTypeCode.Boolean, typeof(bool) },
                { AttributeTypeCode.CalendarRules, typeof(object) },
                { AttributeTypeCode.Customer, typeof(EntityReference) },
                { AttributeTypeCode.DateTime, typeof(DateTime) },
                { AttributeTypeCode.Decimal, typeof(decimal) },
                { AttributeTypeCode.Double, typeof(double) },
                { AttributeTypeCode.EntityName, typeof(string) },
                { AttributeTypeCode.Integer, typeof(int) },
                { AttributeTypeCode.Lookup, typeof(EntityReference) },
                { AttributeTypeCode.Memo, typeof(string) },
                { AttributeTypeCode.Money, typeof(Money) },
                { AttributeTypeCode.Owner, typeof(EntityReference) },
                { AttributeTypeCode.PartyList, typeof(EntityCollection) },
                { AttributeTypeCode.Picklist, typeof(OptionSetValue) },
                { AttributeTypeCode.State, typeof(OptionSetValue) },
                { AttributeTypeCode.Status, typeof(OptionSetValue) },
                { AttributeTypeCode.String, typeof(string) },
                { AttributeTypeCode.Uniqueidentifier, typeof(Guid) }
            };

            _typeNameMappings = new Dictionary<AttributeTypeDisplayName, Type>
            {
                { AttributeTypeDisplayName.BigIntType, typeof(long) },
                { AttributeTypeDisplayName.BooleanType, typeof(bool) },
                { AttributeTypeDisplayName.CalendarRulesType, typeof(object) },
                { AttributeTypeDisplayName.CustomerType, typeof(EntityReference) },
                { AttributeTypeDisplayName.DateTimeType, typeof(DateTime) },
                { AttributeTypeDisplayName.DecimalType, typeof(decimal) },
                { AttributeTypeDisplayName.DoubleType, typeof(double) },
                { AttributeTypeDisplayName.EntityNameType, typeof(string) },
                { AttributeTypeDisplayName.ImageType, typeof(object) },
                { AttributeTypeDisplayName.IntegerType, typeof(int) },
                { AttributeTypeDisplayName.LookupType, typeof(EntityReference) },
                { AttributeTypeDisplayName.MemoType, typeof(string) },
                { AttributeTypeDisplayName.MoneyType, typeof(Money) },
                { AttributeTypeDisplayName.OwnerType, typeof(EntityReference) },
                { AttributeTypeDisplayName.PartyListType, typeof(EntityCollection) },
                { AttributeTypeDisplayName.PicklistType, typeof(OptionSetValue) },
                { AttributeTypeDisplayName.StateType, typeof(OptionSetValue) },
                { AttributeTypeDisplayName.StatusType, typeof(OptionSetValue) },
                { AttributeTypeDisplayName.StringType, typeof(string) },
                { AttributeTypeDisplayName.UniqueidentifierType, typeof(Guid) }
            };
        }

        public static Type GetType(AttributeTypeCode attributeType, bool useFormattedValue)
        {
            if (useFormattedValue
             && attributeType != AttributeTypeCode.Uniqueidentifier
             && attributeType != AttributeTypeCode.PartyList)
            {
                return typeof(string);
            }

            return _typeCodeMappings[attributeType];
        }

        public static Type GetType(AttributeTypeDisplayName attributeType, bool useFormattedValue)
        {
            if (useFormattedValue
             && attributeType != AttributeTypeDisplayName.UniqueidentifierType
             && attributeType != AttributeTypeDisplayName.PartyListType)
            {
                return typeof(string);
            }

            return _typeNameMappings[attributeType];
        }
    }
}

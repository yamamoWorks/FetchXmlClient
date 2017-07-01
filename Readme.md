# Xrm FetchXml Data Provider
.NET Framework Data Provider for Dynamics CRM FetchXml

## Introduction
Get the entity record from Dynamics CRM in the same way to get from database.
Provide follow classes. You'll can guess how to use easily.
- FetchXmlClientFactory
- FetchXmlConnection
- FetchXmlCommand
- FetchXmlDataReader

## Features

Command text use FetchXML instead of sql statement.
Using [Simplified Connection to Microsoft Dynamics CRM](http://msdn.microsoft.com/en-us/library/gg695810(v=crm.5).aspx) in Connection strings. 

## Install
```powershell
PM> Install-Package XrmLib.Data.FetchXmlClient
```
https://www.nuget.org/packages/XrmLib.Data.FetchXmlClient/

## QuickStart
fetch.xml (ref. [FetchXML schema](http://msdn.microsoft.com/en-us/library/gg309405.aspx))
```xml
<fetch version="1.0" output-format="xml-platform" mapping="logical" distinct="false">
  <entity name="account">
    <attribute name="name" />
    <attribute name="telephone1" />
    <attribute name="primarycontactid" />
    <order attribute="name" descending="false" />
    <filter type="and">
      <condition attribute="statecode" operator="eq" value="0" />
    </filter>
    <link-entity name="contact" from="contactid" to="primarycontactid" visible="false" link-type="outer" alias="primarycontact">
      <attribute name="emailaddress1" />
    </link-entity>
  </entity>
</fetch>
```

app.config (ref. [Simplified Connection to Microsoft Dynamics CRM](http://msdn.microsoft.com/en-us/library/gg695810(v=crm.5).aspx))  
```xml
<?xml version="1.0" encoding="utf-8" ?>
<configuration>
  <connectionStrings>
    <add name="mycrm" connectionString="Url=https://server/org; Username=user01; Password=xxxx" />
  </connectionStrings>
</configuration>
```

sample code  
```cs
var query = File.ReadAllText("fetch.xml");
using (var cnn = new FetchXmlConnection("mycrm"))
using (var cmd = new FetchXmlCommand(query, cnn))
using (var reader = cmd.ExecuteReader())
{
    var dt = new DataTable();
    dt.Load(reader);
    dataGridView1.DataSource = dt;
}
```

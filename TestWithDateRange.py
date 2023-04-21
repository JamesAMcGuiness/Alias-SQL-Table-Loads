import pyodbc
import os
import pandas as pd 
from datetime import datetime
import json
import Client_Config
from simple_salesforce import Salesforce, SalesforceLogin, SFType

def getLatestRunDate(myQuery):
    qryResult = sf.query(myQuery)
    
    df       = pd.DataFrame(qryResult['records'])
    strval   = str(df["LastModifiedDate"])
    strstrip = strval[1:24]
       
    
    #print('****************************************************')
    #print('The last run load for this object was: ' + strstrip)
    #print('****************************************************')
    
    return strstrip

#Get Config vars
Client_Config.set_env_var()

#os.environ['ip_path']
inputDIR = os.environ['ip_path']

#Set working Input path
os.chdir(inputDIR)

#Connect To Salesforce
sf = Salesforce(username='aliasadmin@desertpowder.com', password='Welcome2Alias', consumer_key='3MVG9ux34Ig8G5epuXWEQpQ7Gz_zuuv2Soyr2ZwaDScXJyqC1EqxbHYqUZfZ7Ftgstaq_G0gfHorcViPUeX1a', consumer_secret='A10B5FBDBB8FF0B968BA8B44C32267F45477F3B23EA738B941D83038759E3476')

#create SQL Connection
connDCS = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',host='DPC-APP02', database ='DCS-Shop', user ='sa', password='E2@DesertPC')
connSCC = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',host='DPC-APP02', database ='SCC-Shop', user ='sa', password='E2@DesertPC')


# DSC_Customer Load
myQuery           = """Select LastModifiedDate from Account Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
AccountLastRunDate = getLatestRunDate(myQuery)
LastRunDate = AccountLastRunDate
sqlQuery = "SELECT Count(*) Over() as TotalRows,replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'DCS_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Customer.csv' as Source_File,'DESERT' as LoadForCompany FROM CustCode " + " WHERE LastModDate > " + "'"+ LastRunDate + "'" + " ORDER BY CustCode_ID"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Customer.csv')

# SCC_Customer load
myQuery           = """Select LastModifiedDate from Account Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
sqlQuery = "SELECT Count(*) Over() as TotalRows,replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'SCC_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,LastModDate as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Customer.csv' as Source_File,'STANDARD' as LoadForCompany FROM CustCode " + " WHERE LastModDate > " + "'"+ LastRunDate + "'" + " ORDER BY CustCode_ID"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Customer.csv')





#DCS Shipping Load
sqlQuery="SELECT Count(*) Over() as TotalRows,st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode" + " AND st.LastModDate > " + "'"+ LastRunDate + "'" 
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_ShipTo.csv',encoding="utf-8-sig")

#SCC Shipping Load
sqlQuery="SELECT Count(*) Over() as TotalRows, st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode" + " AND st.LastModDate > " + "'"+ LastRunDate + "'" 
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_ShipTo.csv',encoding="utf-8-sig")






#DCS Contact Load
myQuery           = """Select LastModifiedDate from Contact Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
sqlQuery = "SELECT Count(*) Over() as TotalRows, c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'DCS_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Contact.csv' as Source_File,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'DESERT' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''" + " AND c.LastModDate > " + "'"+ LastRunDate + "'" + " ORDER BY Contacts_ID"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Contact.csv',encoding="utf-8-sig")

#SCC Contact Load
sqlQuery = "SELECT Count(*) Over() as TotalRows, c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'SCC_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Contact.csv' as Source_File,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'CUSTOM' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''" + " AND c.LastModDate > " + "'"+ LastRunDate + "'" + " ORDER BY Contacts_ID"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Contact.csv',encoding="utf-8-sig")




#DCS Work Order Load
myQuery           = """Select LastModifiedDate from WorkOrder Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT Count(*) Over() as TotalRows, o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'DCS_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode" + " AND o.LastModDate > " + "'"+ LastRunDate + "'"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Order.csv',encoding="utf-8-sig")

#SCC Work Order Load
sqlQuery="SELECT Count(*) Over() as TotalRows, o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'SCC_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode" + " AND o.LastModDate > " + "'"+ LastRunDate + "'"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Order.csv',encoding="utf-8-sig")





#DCS Work Order Detail Load
myQuery           = """Select LastModifiedDate from WorkOrderLineItem Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT Count(*) Over() as TotalRows, od.QtyOrdered,ISNULL(od.UnitPrice, 0 ) as UnitPrice,od.PartDesc,od.Revision,od.JobNo,od.Status,'???' as QuoteNo,'DCS_' + CONVERT(varchar(100),od.OrderDet_ID) as OrderDet_ID, 'DCS_' + CONVERT(varchar(100),od.OrderNo) as LookupValToOrder,'02iDn000000AXurIAG' as DummyAssetID,'01uDn000003d5aFIAQ' as DummyPriceBookID,CASE WHEN od.WorkCode is Null THEN 'DUMMY'	WHEN od.WorkCode = '' THEN 'DUMMY'	ELSE od.WorkCode END as WorkCode,row_number() over(order by(od.OrderDet_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_OrderDet.csv' as Source_File,CONVERT(nvarchar,od.LastModDate, 23) as PreviousModDate From OrderDet od" + " WHERE od.LastModDate > " + "'"+ LastRunDate + "'"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_OrderDet.csv',encoding="utf-8-sig")


#SCC Work Order Detail Load
sqlQuery="SELECT Count(*) Over() as TotalRows, od.QtyOrdered,ISNULL(od.UnitPrice, 0 ) as UnitPrice,od.PartDesc,od.Revision,od.JobNo,od.Status,'???' as QuoteNo,'SCC_' + CONVERT(varchar(100),od.OrderDet_ID) as OrderDet_ID, 'SCC_' + CONVERT(varchar(100),od.OrderNo) as LookupValToOrder,'02iDn000000AXurIAG' as DummyAssetID,'01uDn000003d5aFIAQ' as DummyPriceBookID,CASE WHEN od.WorkCode is Null THEN 'DUMMY'	WHEN od.WorkCode = '' THEN 'DUMMY'	ELSE od.WorkCode END as WorkCode,row_number() over(order by(od.OrderDet_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_OrderDet.csv' as Source_File,CONVERT(nvarchar,od.LastModDate, 23) as PreviousModDate From OrderDet od" + " WHERE od.LastModDate > " + "'"+ LastRunDate + "'"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_OrderDet.csv',encoding="utf-8-sig")





#DCS Billing Load
myQuery           = """Select LastModifiedDate from Billing__c Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT Count(*) Over() as TotalRows,b.CustDesc,b.ShipToName,b.SAddr1,b.SAddr2,b.SCity,b.SSt,b.SZip,'DCS_' + InvoiceNo as InvoiceNo,CONVERT(nvarchar,b.InvDate,23) as InvoiceDate, 'DCS_' + InvoiceNo + ' - ' + CONVERT(nvarchar,b.InvDate,23) as Name,b.WorkCode,b.TermsCode,'Nofield' as SubTotal,b.SalesTaxChgs,b.ShippingChgs,b.InvoiceTotal,b.AmtPaidSoFar,'Balance Due' as BalanceDue,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,b.CustDesc,b.DateEnt,b.pymtstatus,'QuoteNo' as QuoteNo,'DCS_' + CONVERT(varchar(100), b.Billing_ID) as E2_Invoice__c,row_number() over(order by(b.Billing_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_Billing.csv' as Source_File,	  CONVERT(nvarchar,b.LastModDate, 23) as PreviousModDate From Billing b, CustCode cc Where b.CustCode = cc.CustCode" + " AND b.LastModDate > " + "'"+ LastRunDate + "'"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Billing.csv',encoding="utf-8-sig")

#SCC Billing Load
sqlQuery="SELECT Count(*) Over() as TotalRows, b.CustDesc,b.ShipToName,b.SAddr1,b.SAddr2,b.SCity,b.SSt,b.SZip,'SCC_' + InvoiceNo as InvoiceNo,CONVERT(nvarchar,b.InvDate,23) as InvoiceDate, 'SCC_' + InvoiceNo + ' - ' + CONVERT(nvarchar,b.InvDate,23) as Name,b.WorkCode,b.TermsCode,'Nofield' as SubTotal,b.SalesTaxChgs,b.ShippingChgs,b.InvoiceTotal,b.AmtPaidSoFar,'Balance Due' as BalanceDue,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,b.CustDesc,b.DateEnt,b.pymtstatus,'QuoteNo' as QuoteNo,'SCC_' + CONVERT(varchar(100), b.Billing_ID) as E2_Invoice__c,row_number() over(order by(b.Billing_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_Billing.csv' as Source_File,	  CONVERT(nvarchar,b.LastModDate, 23) as PreviousModDate From Billing b, CustCode cc Where b.CustCode = cc.CustCode" + " AND b.LastModDate > " + "'"+ LastRunDate + "'"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Billing.csv',encoding="utf-8-sig")




#DCS Billing Detail Load
myQuery           = """Select LastModifiedDate from BillingDet__c Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT Count(*) Over() as TotalRows,bd.QtyShipped,bd.UnitPrice,bd.LineTotal,'DCS_' + bd.InvoiceNo as InvoiceNo,bd.PartDesc,bd.PartNo,bd.DelTicketNo,bd.Revision,bd.PONum,'DCS_' + CONVERT(varchar(100), bd.BillingDet_ID) as BillingDet_ID,row_number() over(order by(bd.BillingDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_BillingDet.csv' as Source_File,	  CONVERT(nvarchar,bd.LastModDate, 23) as PreviousModDate FROM BillingDet bd" + " WHERE bd.LastModDate > " + "'"+ LastRunDate + "'"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_BillingDet.csv',encoding="utf-8-sig")

#SCC Billing Detail Load
sqlQuery="SELECT Count(*) Over() as TotalRows, bd.QtyShipped,bd.UnitPrice,bd.LineTotal,'SCC_' + bd.InvoiceNo as InvoiceNo,bd.PartDesc,bd.PartNo,bd.DelTicketNo,bd.Revision,bd.PONum,'SCC_' + CONVERT(varchar(100), bd.BillingDet_ID) as BillingDet_ID,row_number() over(order by(bd.BillingDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_BillingDet.csv' as Source_File,	  CONVERT(nvarchar,bd.LastModDate, 23) as PreviousModDate FROM BillingDet bd" + " WHERE bd.LastModDate > " + "'"+ LastRunDate + "'"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_BillingDet.csv',encoding="utf-8-sig")




#DCS Quote Load
myQuery           = """Select LastModifiedDate from Opportunity Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT Count(*) Over() as TotalRows,q.CustDesc as 'QUOTE_TO',q.Addr1,q.Addr2,q.City,q.st,q.Zip,'DCS_' + q.QuoteNo as QuoteNo,CONVERT(nvarchar,q.DateEnt, 23) as DateENT,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,q.QuotedBy,q.ShipVia,q.ContactName,q.InqNum,q.TermsCode,q.Phone,q.FAX,'Formula for Total??' as Total,'DCS_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,' ' as RecordTypeId,q.CustDesc + ' - ' + q.QuoteNo as Name,'Quote' as StageName,CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_Quote.csv' as Source_File,	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM Quote q, CustCode cc Where q.CustCode = cc.CustCode and q.CustCode is not null" +  " order by E2_Quote_Key"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Quote.csv',encoding="utf-8-sig")

#SCC Quote Load
sqlQuery="SELECT Count(*) Over() as TotalRows,q.CustDesc as 'QUOTE_TO',q.Addr1,q.Addr2,q.City,q.st,q.Zip,'SCC_' + q.QuoteNo as QuoteNo,CONVERT(nvarchar,q.DateEnt, 23) as DateENT,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,q.QuotedBy,q.ShipVia,q.ContactName,q.InqNum,q.TermsCode,q.Phone,q.FAX,'Formula for Total??' as Total,'SCC_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,' ' as RecordTypeId,q.CustDesc + ' - ' + q.QuoteNo as Name,'Quote' as StageName,CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_Quote.csv' as Source_File,	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM Quote q, CustCode cc Where q.CustCode = cc.CustCode and q.CustCode is not null" + " order by E2_Quote_Key"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Quote.csv',encoding="utf-8-sig")





#DCS Quote Detail Load
myQuery           = """Select LastModifiedDate from QuoteDet__c Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT Count(*) Over() as TotalRows,qd.ItemNo,qd.PartNo,qd.Qty1,qd.Price1,qd.JobNo,qd.JobNotes,qd.QuoteNo,qd.Status,SUBSTRING(qd.Descrip,1,80) as Name,'DCS_' + CONVERT(nvarchar,qd.QuoteDet_ID) as QuoteDet_ID,'DCS_'+ qd.QuoteNo as LookupValForOpp,CASE WHEN qd.WorkCode is Null		THEN 'DUMMY'	WHEN qd.WorkCode = ''		THEN 'DUMMY'	ELSE qd.WorkCode END as WorkCode,row_number() over(order by(qd.QuoteDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_QuoteDet.csv' as Source_File,	  CONVERT(nvarchar,qd.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM QuoteDet qd" +  " Order by QuoteNo"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_QuoteDet.csv',encoding="utf-8-sig")

#SCC Quote Detail Load
sqlQuery="SELECT Count(*) Over() as TotalRows,qd.ItemNo,qd.PartNo,qd.Qty1,qd.Price1,qd.JobNo,qd.JobNotes,qd.QuoteNo,qd.Status,SUBSTRING(qd.Descrip,1,80) as Name,'SCC_' + CONVERT(nvarchar,qd.QuoteDet_ID) as QuoteDet_ID,'SCC_'+ qd.QuoteNo as LookupValForOpp,CASE WHEN qd.WorkCode is Null		THEN 'DUMMY'	WHEN qd.WorkCode = ''		THEN 'DUMMY'	ELSE qd.WorkCode END as WorkCode,row_number() over(order by(qd.QuoteDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_QuoteDet.csv' as Source_File,	  CONVERT(nvarchar,qd.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM QuoteDet qd" +  " Order by QuoteNo"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_QuoteDet.csv',encoding="utf-8-sig")





USE [SCC-Shop]
GO

*****************************************************************************************************************************
                                                           Export For SCC CustCode
*****************************************************************************************************************************

SELECT 
	  Count(*) Over() as TotalRows,	
	  replace(replace(APContact,char(10),''),char(13),'') as APContact,
	  replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,
      replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,
      BCity,
      BState,
      BZIPCode,
      Phone,
      Website,
	  replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,
	  replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,
	  'SCC_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,
      LastModDate as PreviousModDate,
	  row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'SCC_Customer.csv' as Source_File,
    'STANDARD' as LoadForCompany
  FROM CustCode
    ORDER BY CustCode_ID


//* 
SELECT Count(*) Over() as TotalRows,replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'SCC_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,LastModDate as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Customer.csv' as Source_File,'STANDARD' as LoadForCompany FROM CustCode ORDER BY CustCode_ID
*//



//********************************************************* SCC Contact *******************************************//
SELECT c.Contact, 
' ' as FirstName, 
' ' as LastName,
c.EMail,
c.Phone,
c.Title,
c.Cell_Phone,
'SCC_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,
c.LastModDate as PreviousModDate,
	  row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'SCC_Contact.csv' as Source_File,
'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,
'STANDARD' as LoadForCompany
FROM Contacts c, CustCode cc 
WHERE c.code = cc.custcode
ORDER BY Contacts_ID


//********************************************************* SCC ShipTo *******************************************//
SELECT 
st.SAddr1,
st.SAddr2,
st.SCity,
st.SState,
st.SZipCode,
st.ShipContact,
st.ShipToName,
st.ShipTo_ID,
'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,
CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate, 
row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'SCC_ShipTo.csv' as Source_File
FROM ShipTo st, CustCode cc
WHERE st.CustCode = cc.CustCode 




//********************************************************* SCC Orders *******************************************//
SELECT 
o.CustDesc as SoldTo, 
o.ShipToName, 
o.ShipAddr1,
o.ShipAddr2,
o.ShipCity,
o.ShipSt,
o.ShipZIP,
'SCC_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,
CONVERT(nvarchar,DateENT, 23) as DateENT, 
o.CustDesc as Customer, 
o.PONum, 
o.ShipVia, 
o.TermsCode,
'FOB???' as FOB, 
'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key, 
'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2, 
o.QuoteNo,
row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'SCC_Order.csv' as Source_File,
	  CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate
From Orders o, CustCode cc
Where o.CustCode = cc.CustCode
and o.LastModDate > '2023-04-16T20:04:09'



//********************************************************* SCC Orders Detail *******************************************//
SELECT 
od.QtyOrdered,
ISNULL(od.UnitPrice, 0 ) as UnitPrice, 
od.PartDesc,
od.Revision,
od.JobNo,
od.Status,
'???' as QuoteNo,
'SCC_' + CONVERT(varchar(100),od.OrderDet_ID) as OrderDet_ID, 
'SCC_' + CONVERT(varchar(100),od.OrderNo) as LookupValToOrder,
'02iDn000000AXurIAG' as DummyAssetID,
'01uDn000003d5aFIAQ' as DummyPriceBookID,
CASE WHEN od.WorkCode is Null
		THEN 'DUMMY'
	WHEN od.WorkCode = ''
		THEN 'DUMMY'
	ELSE od.WorkCode
END as WorkCode,
row_number() over(order by(od.OrderDet_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'SCC_OrderDet.csv' as Source_File,
	  CONVERT(nvarchar,od.LastModDate, 23) as PreviousModDate
From OrderDet od




//********************************************************* SCC Billing *******************************************//
SELECT
b.CustDesc,
b.ShipToName,
b.SAddr1,
b.SAddr2,
b.SCity,
b.SSt,
b.SZip,
'SCC_' + InvoiceNo as InvoiceNo,
CONVERT(nvarchar,b.InvDate,23) as InvoiceDate, 
'SCC_' + InvoiceNo + ' - ' + CONVERT(nvarchar,b.InvDate,23) as Name,
b.WorkCode,
b.TermsCode,
'Nofield' as SubTotal,
b.SalesTaxChgs,
b.ShippingChgs,
b.InvoiceTotal,
b.AmtPaidSoFar,
'Balance Due' as BalanceDue,
'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,
'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,
b.CustDesc,
b.DateEnt,
b.pymtstatus,
'QuoteNo' as QuoteNo,
'SCC_' + CONVERT(varchar(100), b.Billing_ID) as E2_Invoice__c,
row_number() over(order by(b.Billing_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'SCC_Billing.csv' as Source_File,
	  CONVERT(nvarchar,b.LastModDate, 23) as PreviousModDate
From Billing b, CustCode cc
Where b.CustCode = cc.CustCode


//********************************************************* SCC Billing Detail *******************************************//
SELECT
bd.QtyShipped,
bd.UnitPrice,
bd.LineTotal,
'SCC_' + bd.InvoiceNo as InvoiceNo,
bd.PartDesc,
bd.PartNo,
bd.DelTicketNo,
bd.Revision,
bd.PONum,
'SCC_' + CONVERT(varchar(100), bd.BillingDet_ID) as BillingDet_ID,
row_number() over(order by(bd.BillingDet_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'SCC_BillingDet.csv' as Source_File,
	  CONVERT(nvarchar,bd.LastModDate, 23) as PreviousModDate
FROM BillingDet bd


//********************************************************* Quote *******************************************//
SELECT
q.CustDesc as 'QUOTE_TO', 
q.Addr1,
q.Addr2,
q.City,
q.st,
q.Zip,
'SCC_' + q.QuoteNo as QuoteNo,
CONVERT(nvarchar,q.DateEnt, 23) as DateENT,
'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,
q.QuotedBy,
q.ShipVia,
q.ContactName,
q.InqNum,
q.TermsCode,
q.Phone,
q.FAX,
'Formula for Total??' as Total,
'SCC_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,
' ' as RecordTypeId,
q.CustDesc + ' - ' + q.QuoteNo as Name,
'Quote' as StageName,
CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,
row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'SCC_Quote.csv' as Source_File,
	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,
'STANDARD' as LoadForCompany
FROM Quote q, CustCode cc
Where q.CustCode = cc.CustCode
and q.CustCode is not null




//********************************************************* SCC Quote Det *******************************************//
SELECT
qd.ItemNo,
qd.PartNo,
qd.Qty1,
qd.Price1,
qd.JobNo,
qd.JobNotes,
qd.QuoteNo,
qd.Status,
SUBSTRING(qd.Descrip,1,80) as Name,
'SCC_' + CONVERT(nvarchar,qd.QuoteDet_ID) as QuoteDet_ID,
'SCC_' + qd.QuoteNo as LookupValForOpp,
CASE WHEN qd.WorkCode is Null
		THEN 'Dummy'
	WHEN qd.WorkCode = ''
		THEN 'Dummy'
	ELSE qd.WorkCode
END as WorkCode,
row_number() over(order by(qd.QuoteDet_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'SCC_QuoteDet.csv' as Source_File,
	  CONVERT(nvarchar,qd.LastModDate, 23) as PreviousModDate,
'STANDARD' as LoadForCompany
FROM QuoteDet qd
Order by QuoteNo


Select Distinct WorkCode from QuoteDet

Missing Customer Codes (Exist on Quote, but missing in CustCode)
Select q.* from Quote q where q.CustCode not in (Select custcode from CustCode) order by QuoteNo



SELECT qd.ItemNo,qd.PartNo,qd.Qty1,qd.Price1,qd.JobNo,qd.JobNotes,qd.QuoteNo,qd.Status,SUBSTRING(qd.Descrip,1,80) as Name,'SCC_' + CONVERT(nvarchar,qd.QuoteDet_ID) as QuoteDet_ID,'SCC_'+ qd.QuoteNo as LookupValForOpp,CASE WHEN qd.WorkCode is Null		THEN 'DUMMY'	WHEN qd.WorkCode = ''		THEN 'DUMMY'	ELSE qd.WorkCode END as WorkCode,row_number() over(order by(qd.QuoteDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_QuoteDet.csv' as Source_File,	  CONVERT(nvarchar,qd.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM QuoteDet qd Order by QuoteNo
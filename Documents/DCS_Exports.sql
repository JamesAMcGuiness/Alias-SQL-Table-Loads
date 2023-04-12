USE [DCS-Shop]
GO

*****************************************************************************************************************************
                                                           Export For DCS CustCode
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
	  'DCS_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,
      CONVERT(nvarchar,LastModDate, 23) as PreviousModDate, 
	  row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'DCS_Customer.csv' as Source_File,
    'DESERT' as LoadForCompany
  FROM CustCode
  ORDER BY CustCode_ID

   
//********************************************************* DCS Contact *******************************************//
SELECT c.Contact, 
' ' as FirstName, 
' ' as LastName,
c.EMail,
c.Phone,
c.Title,
c.Cell_Phone,
'DCS_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,
CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate, 
	  row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'DCS_Contact.csv' as Source_File,
'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,
'DESERT' as LoadForCompany
FROM Contacts c, CustCode cc 
WHERE c.code = cc.custcode 
ORDER BY Contacts_ID


//********************************************************* DCS Ship To *******************************************//
SELECT 
st.SAddr1,
st.SAddr2,
st.SCity,
st.SState,
st.SZipCode,
st.ShipContact,
st.ShipToName,
st.ShipTo_ID,
'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,
CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate, 
row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'DCS_ShipTo.csv' as Source_File
FROM ShipTo st, CustCode cc
WHERE st.CustCode = cc.CustCode 



//********************************************************* DCS Orders (Work Order) *******************************************//
SELECT 
o.CustDesc as SoldTo, 
o.ShipToName, 
o.ShipAddr1,
o.ShipAddr2,
o.ShipCity,
o.ShipSt,
o.ShipZIP,
'DCS_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,
CONVERT(nvarchar,DateENT, 23) as DateENT, 
o.CustDesc as Customer, 
o.PONum, 
o.ShipVia, 
o.TermsCode,
'FOB???' as FOB, 
'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,
'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,
o.QuoteNo,
row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'DCS_Order.csv' as Source_File,
	  CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate
From Orders o, CustCode cc
Where o.CustCode = cc.CustCode


//********************************************************* DCS Orders Detail (Work Order Line Item *******************************************//
SELECT 
od.QtyOrdered,
ISNULL(od.UnitPrice, 0 ) as UnitPrice, 
od.PartDesc,
od.Revision,
od.JobNo,
od.Status,
'???' as QuoteNo,
'SCC_' + CONVERT(varchar(100), od.OrderDet_ID) as OrderDet_ID, 
od.OrderNo as LookupValToOrder,
'02iDn000000AXurIAG' as DummyAssetID,
'01uDn000003d5aFIAQ' as DummyPriceBookID,
row_number() over(order by(od.OrderDet_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'DCS_OrderDet.csv' as Source_File,
	  CONVERT(nvarchar,od.LastModDate, 23) as PreviousModDate
From OrderDet od


//********************************************************* DCS Billing *******************************************//
SELECT
b.CustDesc,
b.ShipToName,
b.SAddr1,
b.SAddr2,
b.SCity,
b.SSt,
b.SZip,
'DCS_' + InvoiceNo as InvoiceNo,
CONVERT(nvarchar,b.InvDate,23) as InvoiceDate, 
'DCS_' + InvoiceNo + ' - ' + CONVERT(nvarchar,b.InvDate,23) as Name,
b.WorkCode,
b.TermsCode,
'Nofield' as SubTotal,
b.SalesTaxChgs,
b.ShippingChgs,
b.InvoiceTotal,
b.AmtPaidSoFar,
'Balance Due' as BalanceDue,
'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,
'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,
b.CustDesc,
b.DateEnt,
b.pymtstatus,
'QuoteNo' as QuoteNo,
'DCS_' + CONVERT(varchar(100), b.Billing_ID) as E2_Invoice__c,
row_number() over(order by(b.Billing_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'DCS_Billing.csv' as Source_File,
	  CONVERT(nvarchar,b.LastModDate, 23) as PreviousModDate
From Billing b, CustCode cc
Where b.CustCode = cc.CustCode


//********************************************************* DCS Billing Detail *******************************************//
SELECT
bd.QtyShipped,
bd.UnitPrice,
bd.LineTotal,
'DCS_' + bd.InvoiceNo as InvoiceNo,
bd.PartDesc,
bd.PartNo,
bd.DelTicketNo,
bd.Revision,
bd.PONum,
'DCS_' + CONVERT(varchar(100), bd.BillingDet_ID) as BillingDet_ID,
row_number() over(order by(bd.BillingDet_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'DCS_BillingDet.csv' as Source_File,
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
'DCS_' + q.QuoteNo as QuoteNo,
CONVERT(nvarchar,q.DateEnt, 23) as DateENT,
'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,
q.QuotedBy,
q.ShipVia,
q.ContactName,
q.InqNum,
q.TermsCode,
q.Phone,
q.FAX,
'Formula for Total??' as Total,
'DCS_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,
' ' as RecordTypeId,
q.CustDesc + ' - ' + q.QuoteNo as Name,
'Quote' as StageName,
CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,
row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'DCS_Quote.csv' as Source_File,
	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,
'DESERT' as LoadForCompany
FROM Quote q, CustCode cc
Where q.CustCode = cc.CustCode
and q.CustCode is not null
order by E2_Quote_Key


//********************************************************* Quote Detail *******************************************//
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
'DCS_' + CONVERT(nvarchar,qd.QuoteDet_ID) as QuoteDet_ID,
'DCS_'+ qd.QuoteNo as LookupValForOpp,
row_number() over(order by(qd.QuoteDet_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'DCS_QuoteDet.csv' as Source_File,
	  CONVERT(nvarchar,qd.LastModDate, 23) as PreviousModDate,
'DESERT' as LoadForCompany
FROM QuoteDet qd
Order by QuoteNo


Missing Customer Codes (Exist on Quote, but missing in CustCode)
Select q.* from Quote q where q.CustCode not in (Select custcode from CustCode) order by QuoteNo

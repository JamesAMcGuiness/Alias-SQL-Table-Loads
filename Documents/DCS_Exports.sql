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



//********************************************************* DCS Orders *******************************************//
SELECT 
o.CustDesc as SoldTo, 
o.ShipToName, 
o.ShipAddr1,
o.ShipAddr2,
o.ShipCity,
o.ShipSt,
o.ShipZIP,
o.OrderNo, 
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


//********************************************************* DCS Orders Detail *******************************************//
SELECT 
od.QtyOrdered,
od.UnitPrice,
od.PartDesc,
od.Revision,
od.JobNo,
od.Status,
'???' as QuoteNo,
od.OrderDet_ID,
od.OrderNo as LookupValToOrder,
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
b.InvoiceNo,
b.InvDate,
b.WorkCode,
'TermsCode' as TermCode,
'Nofield' as SubTotal,
b.SalesTaxChgs,
b.ShippingChgs,
b.InvoiceTotal,
b.AmtPaidSoFar,
'Balance Due' as BalanceDue,
'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,
b.CustDesc,
b.DateEnt,
b.pymtstatus,
'QuoteNo' as QuoteNo,
b.Billing_ID
From Billing b, CustCode cc
Where b.CustCode = cc.CustCode


//********************************************************* DCS Order Detail *******************************************//
SELECT 
bd.QtyOrd,
bd.UnitPrice,
bd.PartDesc,
bd.Revision,
bd.JobNo,
'???' as Status,
'???' as QuoteNo,
b.Billing_ID,
b.InvoiceNo as LookupValToOrder,
row_number() over(order by(b.Billing_ID)) as RowNum_Of_Source_File,
	  'Y' as LoadedByPython,
	  GetDate() as LoadDate,
	  'DCS_OrderDet.csv' as Source_File,
	  CONVERT(nvarchar,bd.LastModDate, 23) as PreviousModDate
From BillingDet bd, Billing b
Where bd.InvoiceNo = b.InvoiceNo

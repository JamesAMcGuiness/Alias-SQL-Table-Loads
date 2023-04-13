import pyodbc
import os
import pandas as pd 
from datetime import datetime


#create SQL Connection
connDCS = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',host='DPC-APP02', database ='DCS-Shop', user ='sa', password='E2@DesertPC')
connSCC = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',host='DPC-APP02', database ='SCC-Shop', user ='sa', password='E2@DesertPC')

#print(conn)

# DSC_Customer Load
sqlQuery = "SELECT Count(*) Over() as TotalRows,replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'DCS_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Customer.csv' as Source_File,'DESERT' as LoadForCompany FROM CustCode ORDER BY CustCode_ID"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('C:\MyProjects\Alias-SQL-Table-Loads\Input\DCS_Customer.csv')

# SCC_Customer load
sqlQuery = "SELECT Count(*) Over() as TotalRows,replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'SCC_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,LastModDate as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Customer.csv' as Source_File,'STANDARD' as LoadForCompany FROM CustCode ORDER BY CustCode_ID"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('C:\MyProjects\Alias-SQL-Table-Loads\Input\SCC_Customer.csv')

#DCS Contact Load
sqlQuery = "SELECT c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'DCS_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Contact.csv' as Source_File,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'DESERT' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > '' ORDER BY Contacts_ID"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('C:\MyProjects\Alias-SQL-Table-Loads\Input\DCS_Contact.csv',encoding="utf-8-sig")

#SCC Contact Load
sqlQuery = "SELECT c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'SCC_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Contact.csv' as Source_File,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'CUSTOM' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''  ORDER BY Contacts_ID"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('C:\MyProjects\Alias-SQL-Table-Loads\Input\SCC_Contact.csv',encoding="utf-8-sig")

#DCS Shipping Load
sqlQuery="SELECT st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode" 
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('C:\MyProjects\Alias-SQL-Table-Loads\Input\DCS_ShipTo.csv',encoding="utf-8-sig")

#SCC Shipping Load
sqlQuery="SELECT st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode" 
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('C:\MyProjects\Alias-SQL-Table-Loads\Input\SCC_ShipTo.csv',encoding="utf-8-sig")

#DCS Work Order Load
sqlQuery="SELECT o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'DCS_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode" 
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('C:\MyProjects\Alias-SQL-Table-Loads\Input\DCS_Order.csv',encoding="utf-8-sig")

#SCC Work Order Load
sqlQuery="SELECT o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'SCC_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode" 
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('C:\MyProjects\Alias-SQL-Table-Loads\Input\SCC_Order.csv',encoding="utf-8-sig")


#print(df)

import pyodbc
import os
import pandas as pd 
from datetime import datetime


#create SQL Connection
conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',host='DPC-APP02', database ='DCS-Shop', user ='sa', password='E2@DesertPC')

#print(conn)
sqlQuery = "SELECT Count(*) Over() as TotalRows,replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'DCS_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Customer.csv' as Source_File,'DESERT' as LoadForCompany FROM CustCode ORDER BY CustCode_ID"

df = pd.read_sql(sql=sqlQuery, con=conn)

#print(df)
df.to_csv('C:\MyProjects\Alias-SQL-Table-Loads\Input\DCS_Customer.csv')
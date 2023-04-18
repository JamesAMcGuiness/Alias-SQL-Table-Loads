import pyodbc
import os
import pandas as pd 
from datetime import datetime
import json
from simple_salesforce import Salesforce, SalesforceLogin, SFType

#Connect To Salesforce
#sID, instance = SalesforceLogin(username='aliasadmin@desertpowder.com',password='Welcome2Alias',domain='login')

sf = Salesforce(username='aliasadmin@desertpowder.com', password='Welcome2Alias', consumer_key='3MVG9ux34Ig8G5epuXWEQpQ7Gz_zuuv2Soyr2ZwaDScXJyqC1EqxbHYqUZfZ7Ftgstaq_G0gfHorcViPUeX1a', consumer_secret='A10B5FBDBB8FF0B968BA8B44C32267F45477F3B23EA738B941D83038759E3476')
print(sf)

#create SQL Connection
connDCS = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',host='DPC-APP02', database ='DCS-Shop', user ='sa', password='E2@DesertPC')
connSCC = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',host='DPC-APP02', database ='SCC-Shop', user ='sa', password='E2@DesertPC')

def getLatestRunDate(myQuery):
    qryResult = sf.query(myQuery)
    
    df       = pd.DataFrame(qryResult['records'])
    strval   = str(df["LastModifiedDate"])
    strstrip = strval[1:24]
       
    
    print('****************************************************')
    print('The last run load for this object was: ' + strstrip)
    print('****************************************************')
    
    return strstrip



# DSC_Customer Load
myQuery           = """Select LastModifiedDate from Account Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
AccountLastRunDate = getLatestRunDate(myQuery)

LastRunDate = AccountLastRunDate
sqlQuery = "SELECT Count(*) Over() as TotalRows,replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'DCS_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Customer.csv' as Source_File,'DESERT' as LoadForCompany FROM CustCode " + " WHERE LastModDate > " + "'"+ LastRunDate + "'" + " ORDER BY CustCode_ID"
print(sqlQuery)
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('C:\MyProjects\Alias-SQL-Table-Loads\Input\DCS_Customer.csv')







#print(df)

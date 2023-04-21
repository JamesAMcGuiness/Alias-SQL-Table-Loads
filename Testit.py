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

2023-04-10T12:30:25

#DCS Work Order Load
myQuery           = """Select LastModifiedDate from WorkOrder Where E2_Customer_Key__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'DCS_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode AND o.LastModDate > '2023-04-10'"
df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Order.csv',encoding="utf-8-sig")

#SCC Work Order Load
myQuery           = """Select LastModifiedDate from WorkOrder Where E2_Customer_Key__c like 'SCC_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
sqlQuery="SELECT o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'SCC_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode AND o.LastModDate > '2023-04-10'"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Order.csv',encoding="utf-8-sig")

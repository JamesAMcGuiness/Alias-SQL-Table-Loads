import pyodbc
import os
import pandas as pd 
from datetime import datetime
import json
import Client_Config
from simple_salesforce import Salesforce, SalesforceLogin, SFType

def getLatestRunDate(myQuery):
    qryResult = sf.query(myQuery)
        
    print('****************************************************')
    print('The last run load for this object was: ' + qryResult['records'][0]['LastModifiedDate'])
    print('****************************************************')
    
    return str(qryResult['records'][0]['LastModifiedDate'])

#Get Config vars
Client_Config.set_env_var()

#os.environ['ip_path']
inputDIR = os.environ['ip_path']

#Set working Input path
os.chdir(inputDIR)

#Set from Config vars
myUserName       = os.environ['username']
myPassword       = os.environ['password']
myConsumerKey    = os.environ['client_id']
myConsumerSecret = os.environ['client_secret']


mySQLServerDriver           = os.environ['SQLServerDriver']
mySQLServerHost             = os.environ['SQLServerHost']
mySQLServerDesertDatabase   = os.environ['SQLServerDesertDatabase']
mySQLServerStandardDatabase = os.environ['SQLServerStandardDatabase']
mySQLServerUser             = os.environ['SQLServerUser'] 
mySQLServerPassword         = os.environ['SQLServerPassword']

#Connect To Salesforce
sf = Salesforce(username=myUserName, password=myPassword, consumer_key=myConsumerKey,consumer_secret=myConsumerSecret)
#print(sf)

#create SQL Connection
connDCS = pyodbc.connect(driver=mySQLServerDriver,host=mySQLServerHost, database=mySQLServerDesertDatabase, user=mySQLServerUser, password=mySQLServerPassword)
connSCC = pyodbc.connect(driver=mySQLServerDriver,host=mySQLServerHost, database=mySQLServerStandardDatabase, user=mySQLServerUser, password=mySQLServerPassword)

#print(connDCS)
#print(connSCC)

#DCS Work Order Load
myQuery           = """Select LastModifiedDate from WorkOrder Where E2_Customer_Key__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)

sqlQuery="SELECT o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'DCS_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode" + " AND (CONVERT(VARCHAR,o.LastModDate,127) > " + "'"+ LastRunDate + "'" + " or CONVERT(VARCHAR,o.DateEnt,127) > " + "'"+ LastRunDate + "'" + ")"
#print(sqlQuery)

#sqlQuery="SELECT o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt, o.ShipZIP,'DCS_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT, o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB, 'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key, 'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2, o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython, GetDate() as LoadDate,'DCS_Order.csv' as Source_File, o.LastModDate as PreviousModDate  From Orders o, CustCode cc Where o.CustCode = cc.CustCode  AND (CONVERT(VARCHAR,o.LastModDate,127) > '2023-05-18T14:45:18.000+0000' or CONVERT(VARCHAR,o.DateEnt,127) > '2023-05-18T14:45:18.000+0000')"

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Order.csv',encoding="utf-8-sig")




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




#DCS Quote Load
myQuery           = """Select LastModifiedDate from Opportunity Where E2_Quote_Key__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT q.CustDesc as 'QUOTE_TO',q.Addr1,q.Addr2,q.City,q.st,q.Zip,'DCS_' + q.QuoteNo as QuoteNo,CONVERT(nvarchar,q.DateEnt, 23) as DateENT,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,q.EntBy,q.ShipVia,q.ContactName,q.InqNum,q.TermsCode,q.Phone,q.FAX,'Formula for Total??' as Total,'DCS_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,' ' as RecordTypeId,q.CustDesc + ' - ' + q.QuoteNo as Name,'Quote' as StageName,CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_Quote.csv' as Source_File,	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM Quote q, CustCode cc Where q.CustCode = cc.CustCode and q.CustCode is not null" + " AND q.LastModDate > " + "'"+ LastRunDate + "'" + "order by E2_Quote_Key"

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Quote_Insert.csv',encoding="utf-8-sig")
df.to_csv('DCS_Quote_Update.csv',encoding="utf-8-sig")


#SCC Quote Load
myQuery           = """Select LastModifiedDate from Opportunity Where E2_Quote_Key__c like 'SCC_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT q.CustDesc as 'QUOTE_TO',q.Addr1,q.Addr2,q.City,q.st,q.Zip,'SCC_' + q.QuoteNo as QuoteNo,CONVERT(nvarchar,q.DateEnt, 23) as DateENT,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,q.EntBy,q.ShipVia,q.ContactName,q.InqNum,q.TermsCode,q.Phone,q.FAX,'Formula for Total??' as Total,'SCC_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,' ' as RecordTypeId,q.CustDesc + ' - ' + q.QuoteNo as Name,'Quote' as StageName,CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_Quote.csv' as Source_File,	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM Quote q, CustCode cc Where q.CustCode = cc.CustCode and q.CustCode is not null" + " AND q.LastModDate > " + "'"+ LastRunDate + "'" + "order by E2_Quote_Key"
#sqlQuery="SELECT q.CustDesc as 'QUOTE_TO',q.Addr1,q.Addr2,q.City,q.st,q.Zip,'SCC_' + q.QuoteNo as QuoteNo,CONVERT(nvarchar,q.DateEnt, 23) as DateENT,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,q.QuotedBy,q.ShipVia,q.ContactName,q.InqNum,q.TermsCode,q.Phone,q.FAX,'Formula for Total??' as Total,'SCC_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,' ' as RecordTypeId,q.CustDesc + ' - ' + q.QuoteNo as Name,'Quote' as StageName,CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_Quote.csv' as Source_File,	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM Quote q, CustCode cc Where q.CustCode = cc.CustCode and q.CustCode is not null" + " order by E2_Quote_Key"
df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Quote_Insert.csv',encoding="utf-8-sig")
df.to_csv('SCC_Quote_Update.csv',encoding="utf-8-sig")





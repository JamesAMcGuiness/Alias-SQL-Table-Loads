import pyodbc
import os
import csv
import pandas as pd 
from datetime import datetime
import json
import Client_Config
from simple_salesforce import Salesforce, SalesforceLogin, SFType


def createLatestRunDates(csv_file, text_string):

# Open the file in write mode
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
    
        # Write the text string as a single row
        writer.writerow([(text_string)])



def getLatestRunDate(myQuery):
    qryResult = sf.query(myQuery)
        
    #print('****************************************************')
    #print('The last run load for this object was: ' + qryResult['records'][0]['LastModifiedDate'])
    #print('****************************************************')
    
    return str(qryResult['records'][0]['LastModifiedDate'])


#Get Config vars
Client_Config.set_env_var()

#os.environ['ip_path']
inputDIR = os.environ['ip_path']

outputDIR = os.environ['op_path']

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


print(myUserName)      
print(myPassword)      
print(myConsumerKey)   
print(myConsumerSecret)


print(mySQLServerDriver)
print(mySQLServerHost)
print(mySQLServerDesertDatabase)
print(mySQLServerStandardDatabase)
print(mySQLServerUser)
print(mySQLServerPassword)



# Get the current date and time
current_datetime = datetime.now()

# Format the datetime as a string for the file name
file_datetime = current_datetime.strftime("%Y%m%d_%H%M%S")

# Specify the file path and name with the datetime string
f = outputDIR + "\\DateCompare\\"

print('***********************************************' + f)

#Connect To Salesforce
sf = Salesforce(username=myUserName, password=myPassword, consumer_key=myConsumerKey,consumer_secret=myConsumerSecret)
print(sf)

#create SQL Connection
connDCS = pyodbc.connect(driver=mySQLServerDriver,host=mySQLServerHost, database=mySQLServerDesertDatabase, user=mySQLServerUser, password=mySQLServerPassword)
connSCC = pyodbc.connect(driver=mySQLServerDriver,host=mySQLServerHost, database=mySQLServerStandardDatabase, user=mySQLServerUser, password=mySQLServerPassword)

print(connDCS)
print(connSCC)

# DSC_Customer Load
myQuery           = """Select LastModifiedDate from Account Where E2_Customer_Key__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
AccountLastRunDate = getLatestRunDate(myQuery)
DCSAccountLastRunDate = AccountLastRunDate


#sqlQuery = "SELECT replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'DCS_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Customer.csv' as Source_File,'DESERT' as LoadForCompany FROM CustCode " + " WHERE CONVERT(VARCHAR,LastModDate,127) > " + "'"+ DCSAccountLastRunDate + "'" + " or CONVERT(VARCHAR,EnterDate,127) > " + "'"+ DCSAccountLastRunDate + "'" + " ORDER BY CustCode_ID"
sqlQuery = "SELECT replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'DCS_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Customer.csv' as Source_File,'DESERT' as LoadForCompany FROM CustCode  ORDER BY CustCode_ID"

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Customer.csv')

# SCC_Customer load
myQuery           = """Select LastModifiedDate from Account Where E2_Customer_Key__c like 'SCC_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
AccountLastRunDate = getLatestRunDate(myQuery)
SCCAccountLastRunDate = AccountLastRunDate

# Specify the file path and name with the datetime string
csv_file = outputDIR + "\\DateCompare\\" +"Account-DCS_" + file_datetime + ".csv"

print('********************************* csv file is' + csv_file)
createLatestRunDates(csv_file, AccountLastRunDate)

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"Account-SCC_" + file_datetime + ".csv"
createLatestRunDates(csv_file, SCCAccountLastRunDate)



#sqlQuery = "SELECT replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'SCC_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Customer.csv' as Source_File,'STANDARD' as LoadForCompany FROM CustCode " + " WHERE CONVERT(VARCHAR,LastModDate,127) > " + "'"+ SCCAccountLastRunDate + "'" + " or CONVERT(VARCHAR,EnterDate,127) > " + "'"+ SCCAccountLastRunDate + "'" + " ORDER BY CustCode_ID"
sqlQuery = "SELECT replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'SCC_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Customer.csv' as Source_File,'STANDARD' as LoadForCompany FROM CustCode " + "  ORDER BY CustCode_ID"

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Customer.csv')


#DCS Shipping Load
#sqlQuery="SELECT st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'N' as LoadedByPython,GetDate() as LoadDate,'DCS_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode" + " AND (CONVERT(VARCHAR,st.LastModDate,127) > " + "'"+ DCSAccountLastRunDate + "'" + " or CONVERT(VARCHAR,st.EnterDate,127) > " + "'"+ DCSAccountLastRunDate + "'" + ")" 
sqlQuery="SELECT st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'N' as LoadedByPython,GetDate() as LoadDate,'DCS_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode"  

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_ShipTo.csv',encoding="utf-8-sig")



#SCC Shipping Load
#sqlQuery="SELECT st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'N' as LoadedByPython,GetDate() as LoadDate,'SCC_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode" + " AND (CONVERT(VARCHAR,st.LastModDate,127) > " + "'"+ SCCAccountLastRunDate + "'" + " or CONVERT(VARCHAR,st.EnterDate,127) > " + "'"+ SCCAccountLastRunDate + "'" + ")" 
sqlQuery="SELECT st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'N' as LoadedByPython,GetDate() as LoadDate,'SCC_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode" 

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_ShipTo.csv',encoding="utf-8-sig")




#DCS Contact Load
myQuery           = """Select LastModifiedDate from Contact Where E2_Contact_ID__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
#sqlQuery = "SELECT c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'DCS_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Contact.csv' as Source_File,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'DESERT' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''" + " AND (CONVERT(VARCHAR,c.LastModDate,127) > " + "'"+ LastRunDate + "'" +  " or CONVERT(VARCHAR,c.EnterDate,127) > " + "'"+ LastRunDate + "'" + ")" + " ORDER BY Contacts_ID"
sqlQuery = "SELECT c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'DCS_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Contact.csv' as Source_File,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'DESERT' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''"  + " ORDER BY Contacts_ID"

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Contact.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"Contact-DCS_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)



#SCC Contact Load
myQuery           = """Select LastModifiedDate from Contact Where E2_Contact_ID__c like 'SCC_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
#sqlQuery = "SELECT c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'SCC_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Contact.csv' as Source_File,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'STANDARD' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''" + " AND (CONVERT(VARCHAR,c.LastModDate,127) > " + "'"+ LastRunDate + "'" +  " or CONVERT(VARCHAR,c.EnterDate,127) > " + "'"+ LastRunDate + "'" + ")" + " ORDER BY Contacts_ID"
sqlQuery = "SELECT c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'SCC_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Contact.csv' as Source_File,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'STANDARD' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''" + " ORDER BY Contacts_ID"

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Contact.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"Contact-SCC_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)


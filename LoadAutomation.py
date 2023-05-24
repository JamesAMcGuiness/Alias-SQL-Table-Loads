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


sqlQuery = "SELECT replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'DCS_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Customer.csv' as Source_File,'DESERT' as LoadForCompany FROM CustCode " + " WHERE CONVERT(VARCHAR,LastModDate,127) > " + "'"+ DCSAccountLastRunDate + "'" + " or CONVERT(VARCHAR,EnterDate,127) > " + "'"+ DCSAccountLastRunDate + "'" + " ORDER BY CustCode_ID"
#sqlQuery = "SELECT replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'DCS_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Customer.csv' as Source_File,'DESERT' as LoadForCompany FROM CustCode  ORDER BY CustCode_ID"

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



sqlQuery = "SELECT replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'SCC_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Customer.csv' as Source_File,'STANDARD' as LoadForCompany FROM CustCode " + " WHERE CONVERT(VARCHAR,LastModDate,127) > " + "'"+ SCCAccountLastRunDate + "'" + " or CONVERT(VARCHAR,EnterDate,127) > " + "'"+ SCCAccountLastRunDate + "'" + " ORDER BY CustCode_ID"
#sqlQuery = "SELECT replace(replace(APContact,char(10),''),char(13),'') as APContact,replace(replace(replace(replace(BAddr1,char(10),''),char(13),''),'#',''),',','|') as BAddr1,replace(replace(replace(replace(BAddr2,char(10),''),char(13),''),'#',''),',','|') as BAddr2,BCity,BState,BZIPCode,Phone,Website,replace(replace(replace(CustName,char(10),''),char(13),''),',','|') as CustName,replace(replace(replace(CustCode,char(10),''),char(13),''),',','|') as CustCode,'SCC_' + CONVERT(varchar(100), CustCode_ID) as CustCode_ID,CONVERT(nvarchar,LastModDate, 23) as PreviousModDate,row_number() over(order by(CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Customer.csv' as Source_File,'STANDARD' as LoadForCompany FROM CustCode " + "  ORDER BY CustCode_ID"

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Customer.csv')


#DCS Shipping Load
sqlQuery="SELECT st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'N' as LoadedByPython,GetDate() as LoadDate,'DCS_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode" + " AND (CONVERT(VARCHAR,st.LastModDate,127) > " + "'"+ DCSAccountLastRunDate + "'" + " or CONVERT(VARCHAR,st.EnterDate,127) > " + "'"+ DCSAccountLastRunDate + "'" + ")" 
#sqlQuery="SELECT st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'N' as LoadedByPython,GetDate() as LoadDate,'DCS_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode"  

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_ShipTo.csv',encoding="utf-8-sig")



#SCC Shipping Load
sqlQuery="SELECT st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'N' as LoadedByPython,GetDate() as LoadDate,'SCC_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode" + " AND (CONVERT(VARCHAR,st.LastModDate,127) > " + "'"+ SCCAccountLastRunDate + "'" + " or CONVERT(VARCHAR,st.EnterDate,127) > " + "'"+ SCCAccountLastRunDate + "'" + ")" 
#sqlQuery="SELECT st.SAddr1,st.SAddr2,st.SCity,st.SState,st.SZipCode,st.ShipContact,st.ShipToName,st.ShipTo_ID,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,CONVERT(nvarchar,st.LastModDate, 23) as PreviousModDate,row_number() over(order by(ShipTo_ID)) as RowNum_Of_Source_File,'N' as LoadedByPython,GetDate() as LoadDate,'SCC_ShipTo.csv' as Source_File FROM ShipTo st, CustCode cc WHERE st.CustCode = cc.CustCode" 

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_ShipTo.csv',encoding="utf-8-sig")




#DCS Contact Load
myQuery           = """Select LastModifiedDate from Contact Where E2_Contact_ID__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
sqlQuery = "SELECT c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'DCS_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Contact.csv' as Source_File,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'DESERT' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''" + " AND (CONVERT(VARCHAR,c.LastModDate,127) > " + "'"+ LastRunDate + "'" +  " or CONVERT(VARCHAR,c.EnterDate,127) > " + "'"+ LastRunDate + "'" + ")" + " ORDER BY Contacts_ID"
#sqlQuery = "SELECT c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'DCS_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Contact.csv' as Source_File,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'DESERT' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''"  + " ORDER BY Contacts_ID"

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Contact.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"Contact-DCS_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)



#SCC Contact Load
myQuery           = """Select LastModifiedDate from Contact Where E2_Contact_ID__c like 'SCC_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
sqlQuery = "SELECT c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'SCC_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Contact.csv' as Source_File,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'STANDARD' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''" + " AND (CONVERT(VARCHAR,c.LastModDate,127) > " + "'"+ LastRunDate + "'" +  " or CONVERT(VARCHAR,c.EnterDate,127) > " + "'"+ LastRunDate + "'" + ")" + " ORDER BY Contacts_ID"
#sqlQuery = "SELECT c.Contact,' ' as FirstName,' ' as LastName,c.EMail,c.Phone,c.Title,c.Cell_Phone,'SCC_' + CONVERT(varchar(100), c.Contacts_ID) as Contacts_ID,CONVERT(nvarchar,c.LastModDate, 23) as PreviousModDate,row_number() over(order by(c.Contacts_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Contact.csv' as Source_File,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as CustCode_ID,'STANDARD' as LoadForCompany FROM Contacts c, CustCode cc WHERE c.code = cc.custcode and c.Contact > ''" + " ORDER BY Contacts_ID"

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Contact.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"Contact-SCC_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)





#DCS Work Order Load
myQuery           = """Select LastModifiedDate from WorkOrder Where E2_Customer_Key__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'DCS_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode" + " AND (CONVERT(VARCHAR,o.LastModDate,127) > " + "'"+ LastRunDate + "'" + " or CONVERT(VARCHAR,o.DateEnt,127) > " + "'"+ LastRunDate + "'" + ")"
#sqlQuery="SELECT o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'DCS_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode" 

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Order.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"WorkOrder-DCS_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)




#SCC Work Order Load
myQuery           = """Select LastModifiedDate from WorkOrder Where E2_Customer_Key__c like 'SCC_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
sqlQuery="SELECT o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'SCC_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode" + " AND (CONVERT(VARCHAR,o.LastModDate,127) > " + "'"+ LastRunDate + "'" + " or CONVERT(VARCHAR,o.DateEnt,127) > " + "'"+ LastRunDate + "'" + ")"
#sqlQuery="SELECT o.CustDesc as SoldTo,o.ShipToName,o.ShipAddr1,o.ShipAddr2,o.ShipCity,o.ShipSt,o.ShipZIP,'SCC_' + CONVERT(varchar(100), o.OrderNo) as OrderNo,CONVERT(nvarchar,DateENT, 23) as DateENT,o.CustDesc as Customer,o.PONum,o.ShipVia,o.TermsCode,'FOB???' as FOB,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,o.QuoteNo,row_number() over(order by(cc.CustCode_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_Order.csv' as Source_File,CONVERT(nvarchar,o.LastModDate, 23) as PreviousModDate From Orders o, CustCode cc Where o.CustCode = cc.CustCode" 

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Order.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"WorkOrder-SCC_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)





#DCS Work Order Detail Load
myQuery           = """Select LastModifiedDate from WorkOrderLineItem Where OrderDetID__c like 'DCS_%' and  Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT od.QtyOrdered,ISNULL(od.UnitPrice, 0 ) as UnitPrice,od.PartDesc,od.Revision,od.JobNo,od.Status,'???' as QuoteNo,'DCS_' + CONVERT(varchar(100),od.OrderDet_ID) as OrderDet_ID, 'DCS_' + CONVERT(varchar(100),od.OrderNo) as LookupValToOrder,'02iDn000000AXurIAG' as DummyAssetID,'01uDn000003d5aFIAQ' as DummyPriceBookID,CASE WHEN od.WorkCode is Null THEN 'DUMMY'	WHEN od.WorkCode = '' THEN 'DUMMY'	ELSE od.WorkCode END as WorkCode,row_number() over(order by(od.OrderDet_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_OrderDet.csv' as Source_File,CONVERT(nvarchar,od.LastModDate, 23) as PreviousModDate From OrderDet od" + " WHERE CONVERT(VARCHAR,od.LastModDate,127) > " + "'"+ LastRunDate + "'"
#sqlQuery="SELECT od.QtyOrdered,ISNULL(od.UnitPrice, 0 ) as UnitPrice,od.PartDesc,od.Revision,od.JobNo,od.Status,'???' as QuoteNo,'DCS_' + CONVERT(varchar(100),od.OrderDet_ID) as OrderDet_ID, 'DCS_' + CONVERT(varchar(100),od.OrderNo) as LookupValToOrder,'02iDn000000AXurIAG' as DummyAssetID,'01uDn000003d5aFIAQ' as DummyPriceBookID,CASE WHEN od.WorkCode is Null THEN 'DUMMY'	WHEN od.WorkCode = '' THEN 'DUMMY'	ELSE od.WorkCode END as WorkCode,row_number() over(order by(od.OrderDet_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'DCS_OrderDet.csv' as Source_File,CONVERT(nvarchar,od.LastModDate, 23) as PreviousModDate From OrderDet od" 

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Order_Det.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = "WorkOrderLineItem-DCS.csv"
csv_file = outputDIR + "\\DateCompare\\" +"WorkOrderLineItem-DCS_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)




#SCC Work Order Detail Load
myQuery           = """Select LastModifiedDate from WorkOrderLineItem Where OrderDetID__c like 'SCC_%' and  Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT od.QtyOrdered,ISNULL(od.UnitPrice, 0 ) as UnitPrice,od.PartDesc,od.Revision,od.JobNo,od.Status,'???' as QuoteNo,'SCC_' + CONVERT(varchar(100),od.OrderDet_ID) as OrderDet_ID, 'SCC_' + CONVERT(varchar(100),od.OrderNo) as LookupValToOrder,'02iDn000000AXurIAG' as DummyAssetID,'01uDn000003d5aFIAQ' as DummyPriceBookID,CASE WHEN od.WorkCode is Null THEN 'DUMMY'	WHEN od.WorkCode = '' THEN 'DUMMY'	ELSE od.WorkCode END as WorkCode,row_number() over(order by(od.OrderDet_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_OrderDet.csv' as Source_File,CONVERT(nvarchar,od.LastModDate, 23) as PreviousModDate From OrderDet od" + " WHERE CONVERT(VARCHAR,od.LastModDate,127) > " + "'"+ LastRunDate + "'"
#sqlQuery="SELECT od.QtyOrdered,ISNULL(od.UnitPrice, 0 ) as UnitPrice,od.PartDesc,od.Revision,od.JobNo,od.Status,'???' as QuoteNo,'SCC_' + CONVERT(varchar(100),od.OrderDet_ID) as OrderDet_ID, 'SCC_' + CONVERT(varchar(100),od.OrderNo) as LookupValToOrder,'02iDn000000AXurIAG' as DummyAssetID,'01uDn000003d5aFIAQ' as DummyPriceBookID,CASE WHEN od.WorkCode is Null THEN 'DUMMY'	WHEN od.WorkCode = '' THEN 'DUMMY'	ELSE od.WorkCode END as WorkCode,row_number() over(order by(od.OrderDet_ID)) as RowNum_Of_Source_File,'Y' as LoadedByPython,GetDate() as LoadDate,'SCC_OrderDet.csv' as Source_File,CONVERT(nvarchar,od.LastModDate, 23) as PreviousModDate From OrderDet od" 

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Order_Det.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"WorkOrderLineItem-SCC_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)



#DCS Billing Load
myQuery           = """Select LastModifiedDate from Billing__c Where E2_Invoice__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT REPLACE(b.CustDesc, ',', ' ') as CustDesc,REPLACE(b.CustDesc, ',',' ') as ShipToName,REPLACE(b.SAddr1,',',' ') as SAddr1,REPLACE(b.SAddr2,',',' ') as SAddr2,REPLACE(b.SCity,',',' ') as SCity,b.SSt,b.SZip,'DCS_' + InvoiceNo as InvoiceNo,CONVERT(nvarchar,b.InvDate,23) as InvoiceDate, 'DCS_' + InvoiceNo + ' - ' + CONVERT(nvarchar,b.InvDate,23) as Name,b.WorkCode,b.TermsCode,'Nofield' as SubTotal,b.SalesTaxChgs,b.ShippingChgs,b.InvoiceTotal,b.AmtPaidSoFar,'Balance Due' as BalanceDue,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,REPLACE(b.CustDesc,',',' ') as CustDesc,b.DateEnt,b.pymtstatus,'QuoteNo' as QuoteNo,'DCS_' + CONVERT(varchar(100), b.Billing_ID) as E2_Invoice__c,row_number() over(order by(b.Billing_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_Billing.csv' as Source_File,	  CONVERT(nvarchar,b.LastModDate, 23) as PreviousModDate From Billing b, CustCode cc Where b.CustCode = cc.CustCode" + " AND (CONVERT(VARCHAR,b.LastModDate,127) > " + "'"+ LastRunDate + "'"  + " or CONVERT(VARCHAR,b.DateEnt,127) > " + "'"+ LastRunDate + "'" + ")"
#sqlQuery="SELECT REPLACE(b.CustDesc,',',' ') as CustDesc,REPLACE(b.CustDesc,',',' ') as ShipToName,REPLACE(b.SAddr1,',',' ') as SAddr1,REPLACE(b.SAddr2,',',' ') as SAddr2,REPLACE(b.SCity,',',' ') as SCity,b.SSt,b.SZip,'DCS_' + InvoiceNo as InvoiceNo,CONVERT(nvarchar,b.InvDate,23) as InvoiceDate, 'DCS_' + InvoiceNo + ' - ' + CONVERT(nvarchar,b.InvDate,23) as Name,b.WorkCode,b.TermsCode,'Nofield' as SubTotal,b.SalesTaxChgs,b.ShippingChgs,b.InvoiceTotal,b.AmtPaidSoFar,'Balance Due' as BalanceDue,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,REPLACE(b.CustDesc,',',' ') as CustDesc,b.DateEnt,b.pymtstatus,'QuoteNo' as QuoteNo,'DCS_' + CONVERT(varchar(100), b.Billing_ID) as E2_Invoice__c,row_number() over(order by(b.Billing_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_Billing.csv' as Source_File,	  CONVERT(nvarchar,b.LastModDate, 23) as PreviousModDate From Billing b, CustCode cc Where b.CustCode = cc.CustCode" 

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Billing.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = "Billing-DCS.csv"
csv_file = outputDIR + "\\DateCompare\\" +"Billing-DCS_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)




#SCC Billing Load
myQuery           = """Select LastModifiedDate from Billing__c Where E2_Invoice__c like 'SCC_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)

sqlQuery="SELECT REPLACE(b.CustDesc, ',', ' ') as CustDesc,REPLACE(b.CustDesc, ',',' ') as ShipToName,REPLACE(b.SAddr1,',',' ') as SAddr1,REPLACE(b.SAddr2,',',' ') as SAddr2,REPLACE(b.SCity,',',' ') as SCity,b.SSt,b.SZip,'SCC_' + InvoiceNo as InvoiceNo,CONVERT(nvarchar,b.InvDate,23) as InvoiceDate, 'SCC_' + InvoiceNo + ' - ' + CONVERT(nvarchar,b.InvDate,23) as Name,b.WorkCode,b.TermsCode,'Nofield' as SubTotal,b.SalesTaxChgs,b.ShippingChgs,b.InvoiceTotal,b.AmtPaidSoFar,'Balance Due' as BalanceDue,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,REPLACE(b.CustDesc,',',' ') as CustDesc,b.DateEnt,b.pymtstatus,'QuoteNo' as QuoteNo,'SCC_' + CONVERT(varchar(100), b.Billing_ID) as E2_Invoice__c,row_number() over(order by(b.Billing_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_Billing.csv' as Source_File,	  CONVERT(nvarchar,b.LastModDate, 23) as PreviousModDate From Billing b, CustCode cc Where b.CustCode = cc.CustCode" + " AND (CONVERT(VARCHAR,b.LastModDate,127) > " + "'"+ LastRunDate + "'"  + " or CONVERT(VARCHAR,b.DateEnt,127) > " + "'"+ LastRunDate + "'" + ")"
#sqlQuery="SELECT REPLACE(b.CustDesc, ',', ' ') as CustDesc,REPLACE(b.CustDesc, ',',' ') as ShipToName,REPLACE(b.SAddr1,',',' ') as SAddr1,REPLACE(b.SAddr2,',',' ') as SAddr2,REPLACE(b.SCity,',',' ') as SCity,b.SSt,b.SZip,'SCC_' + InvoiceNo as InvoiceNo,CONVERT(nvarchar,b.InvDate,23) as InvoiceDate, 'SCC_' + InvoiceNo + ' - ' + CONVERT(nvarchar,b.InvDate,23) as Name,b.WorkCode,b.TermsCode,'Nofield' as SubTotal,b.SalesTaxChgs,b.ShippingChgs,b.InvoiceTotal,b.AmtPaidSoFar,'Balance Due' as BalanceDue,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key2,REPLACE(b.CustDesc,',',' ') as CustDesc,b.DateEnt,b.pymtstatus,'QuoteNo' as QuoteNo,'SCC_' + CONVERT(varchar(100), b.Billing_ID) as E2_Invoice__c,row_number() over(order by(b.Billing_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_Billing.csv' as Source_File,	  CONVERT(nvarchar,b.LastModDate, 23) as PreviousModDate From Billing b, CustCode cc Where b.CustCode = cc.CustCode" 

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Billing.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"Billing-SCC_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)





#DCS Billing Detail Load
myQuery           = """Select LastModifiedDate from BillingDet__c Where BillingDet_ID__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT bd.QtyShipped,bd.UnitPrice,bd.LineTotal,'DCS_' + bd.InvoiceNo as InvoiceNo,REPLACE(bd.PartDesc,',',' ') as PartDesc,REPLACE(bd.PartNo,',',' ') as PartNo,bd.DelTicketNo,bd.Revision,bd.PONum,'DCS_' + CONVERT(varchar(100), bd.BillingDet_ID) as BillingDet_ID,row_number() over(order by(bd.BillingDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_BillingDet.csv' as Source_File,	  CONVERT(nvarchar,bd.LastModDate, 23) as PreviousModDate FROM BillingDet bd" + " WHERE CONVERT(VARCHAR,bd.LastModDate,127) > " + "'"+ LastRunDate + "'"
#sqlQuery="SELECT bd.QtyShipped,bd.UnitPrice,bd.LineTotal,'DCS_' + bd.InvoiceNo as InvoiceNo,REPLACE(bd.PartDesc,',',' ') as PartDesc,REPLACE(bd.PartNo,',',' ') as PartNo,bd.DelTicketNo,bd.Revision,bd.PONum,'DCS_' + CONVERT(varchar(100), bd.BillingDet_ID) as BillingDet_ID,row_number() over(order by(bd.BillingDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_BillingDet.csv' as Source_File,	  CONVERT(nvarchar,bd.LastModDate, 23) as PreviousModDate FROM BillingDet bd" 

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Billing_Det.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"BillingDet-DCS_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)





#SCC Billing Detail Load
myQuery           = """Select LastModifiedDate from BillingDet__c Where BillingDet_ID__c like 'SCC_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT  bd.QtyShipped,bd.UnitPrice,bd.LineTotal,'SCC_' + bd.InvoiceNo as InvoiceNo,REPLACE(bd.PartDesc,',',' ') as PartDesc,REPLACE(bd.PartNo,',',' ') as PartNo,bd.DelTicketNo,bd.Revision,bd.PONum,'SCC_' + CONVERT(varchar(100), bd.BillingDet_ID) as BillingDet_ID,row_number() over(order by(bd.BillingDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_BillingDet.csv' as Source_File,	  CONVERT(nvarchar,bd.LastModDate, 23) as PreviousModDate FROM BillingDet bd" + " WHERE CONVERT(VARCHAR,bd.LastModDate,127) > " + "'"+ LastRunDate + "'"
#sqlQuery="SELECT  bd.QtyShipped,bd.UnitPrice,bd.LineTotal,'SCC_' + bd.InvoiceNo as InvoiceNo,REPLACE(bd.PartDesc,',',' ') as PartDesc,REPLACE(bd.PartNo,',',' ') as PartNo,bd.DelTicketNo,bd.Revision,bd.PONum,'SCC_' + CONVERT(varchar(100), bd.BillingDet_ID) as BillingDet_ID,row_number() over(order by(bd.BillingDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_BillingDet.csv' as Source_File,	  CONVERT(nvarchar,bd.LastModDate, 23) as PreviousModDate FROM BillingDet bd" 

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Billing_Det.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"BillingDet-SCC_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)





#DCS Quote Load
myQuery           = """Select LastModifiedDate from Opportunity Where E2_Quote_Key__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT REPLACE(q.CustDesc,',',' ') as 'QUOTE_TO',REPLACE(q.Addr1,',',' ') as Addr1,REPLACE(q.Addr2,',',' ') as Addr2,q.City,q.st,q.Zip,'DCS_' + q.QuoteNo as QuoteNo,CONVERT(nvarchar,q.DateEnt, 23) as DateENT,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,q.EntBy,q.ShipVia,q.ContactName,q.InqNum,q.TermsCode,q.Phone,q.FAX,'Formula for Total??' as Total,'DCS_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,' ' as RecordTypeId,REPLACE(q.CustDesc,',',' ') + ' - ' + q.QuoteNo as Name,'Quote' as StageName,CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_Quote.csv' as Source_File,	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM Quote q, CustCode cc Where q.CustCode = cc.CustCode and q.CustCode is not null" + " AND (CONVERT(VARCHAR,q.LastModDate,127) > " + "'"+ LastRunDate + "'"  + " or CONVERT(VARCHAR,q.DateEnt,127) > " + "'"+ LastRunDate + "'" + ")" + " order by E2_Quote_Key"
#sqlQuery="SELECT REPLACE(q.CustDesc,',',' ') as 'QUOTE_TO',REPLACE(q.Addr1,',',' ') as Addr1,REPLACE(q.Addr2,',',' ') as Addr2,q.City,q.st,q.Zip,'DCS_' + q.QuoteNo as QuoteNo,CONVERT(nvarchar,q.DateEnt, 23) as DateENT,'DCS_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,q.EntBy,q.ShipVia,q.ContactName,q.InqNum,q.TermsCode,q.Phone,q.FAX,'Formula for Total??' as Total,'DCS_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,' ' as RecordTypeId,REPLACE(q.CustDesc,',',' ') + ' - ' + q.QuoteNo as Name,'Quote' as StageName,CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_Quote.csv' as Source_File,	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM Quote q, CustCode cc Where q.CustCode = cc.CustCode and q.CustCode is not null"  + " order by E2_Quote_Key"

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Quote_Insert.csv',encoding="utf-8-sig")
df.to_csv('DCS_Quote_Update.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"Opportunity-DCS_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)




#SCC Quote Load
myQuery           = """Select LastModifiedDate from Opportunity Where E2_Quote_Key__c like 'SCC_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT REPLACE(q.CustDesc,',',' ') as 'QUOTE_TO',REPLACE(q.Addr1,',',' ') as Addr1,REPLACE(q.Addr2,',',' ') as Addr2,q.City,q.st,q.Zip,'SCC_' + q.QuoteNo as QuoteNo,CONVERT(nvarchar,q.DateEnt, 23) as DateENT,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,q.EntBy,q.ShipVia,q.ContactName,q.InqNum,q.TermsCode,q.Phone,q.FAX,'Formula for Total??' as Total,'SCC_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,' ' as RecordTypeId,REPLACE(q.CustDesc,',',' ') + ' - ' + q.QuoteNo as Name,'Quote' as StageName,CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_Quote.csv' as Source_File,	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,'STANDARD' as LoadForCompany FROM Quote q, CustCode cc Where q.CustCode = cc.CustCode and q.CustCode is not null" + " AND (CONVERT(VARCHAR,q.LastModDate,127) > " + "'"+ LastRunDate + "'"  + " or CONVERT(VARCHAR,q.DateEnt,127) > " + "'"+ LastRunDate + "'" + ")" + " order by E2_Quote_Key"
#sqlQuery="SELECT REPLACE(q.CustDesc,',',' ') as 'QUOTE_TO',REPLACE(q.Addr1,',',' ') as Addr1,REPLACE(q.Addr2,',',' ') as Addr2,q.City,q.st,q.Zip,'SCC_' + q.QuoteNo as QuoteNo,CONVERT(nvarchar,q.DateEnt, 23) as DateENT,'SCC_' + CONVERT(varchar(100), cc.CustCode_ID) as E2_Customer_Key,q.EntBy,q.ShipVia,q.ContactName,q.InqNum,q.TermsCode,q.Phone,q.FAX,'Formula for Total??' as Total,'SCC_' + CONVERT(nvarchar,QuoteNo) as E2_Quote_Key,' ' as RecordTypeId,REPLACE(q.CustDesc,',',' ') + ' - ' + q.QuoteNo as Name,'Quote' as StageName,CONVERT(nvarchar,q.ExpireDate, 23) as CloseDate,row_number() over(order by(q.Quote_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_Quote.csv' as Source_File,	  CONVERT(nvarchar,q.LastModDate, 23) as PreviousModDate,'STANDARD' as LoadForCompany FROM Quote q, CustCode cc Where q.CustCode = cc.CustCode and q.CustCode is not null"  + " order by E2_Quote_Key"

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Quote_Insert.csv',encoding="utf-8-sig")
df.to_csv('SCC_Quote_Update.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"Opportunity-SCC_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)




#DCS Quote Detail Load
myQuery           = """Select LastModifiedDate from QuoteDet__c Where QuoteDet_ID__c like 'DCS_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT qd.ItemNo,qd.PartNo,qd.Qty1,qd.Qty2,qd.Qty3,qd.Qty4,qd.Qty5,qd.Qty6,qd.Qty7,qd.Qty8,qd.Price1,qd.Price2,qd.Price3,qd.Price4,qd.Price5,qd.Price6,qd.Price7,qd.Price8,qd.JobNo,qd.JobNotes,qd.QuoteNo,qd.Status,SUBSTRING(qd.Descrip,1,80) as Name,'DCS_' + CONVERT(nvarchar,qd.QuoteDet_ID) as QuoteDet_ID,'DCS_'+ qd.QuoteNo as LookupValForOpp,CASE WHEN qd.WorkCode is Null		THEN 'DUMMY'	WHEN qd.WorkCode = ''		THEN 'DUMMY'	ELSE qd.WorkCode END as WorkCode,row_number() over(order by(qd.QuoteDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_QuoteDet.csv' as Source_File,	  CONVERT(nvarchar,qd.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM QuoteDet qd" + " WHERE CONVERT(VARCHAR,qd.LastModDate,127) > " + "'"+ LastRunDate + "'" + " Order by QuoteNo"
#sqlQuery="SELECT qd.ItemNo,qd.PartNo,qd.Qty1,qd.Qty2,qd.Qty3,qd.Qty4,qd.Qty5,qd.Qty6,qd.Qty7,qd.Qty8,qd.Price1,qd.Price2,qd.Price3,qd.Price4,qd.Price5,qd.Price6,qd.Price7,qd.Price8,qd.JobNo,qd.JobNotes,qd.QuoteNo,qd.Status,SUBSTRING(qd.Descrip,1,80) as Name,'DCS_' + CONVERT(nvarchar,qd.QuoteDet_ID) as QuoteDet_ID,'DCS_'+ qd.QuoteNo as LookupValForOpp,CASE WHEN qd.WorkCode is Null		THEN 'DUMMY'	WHEN qd.WorkCode = ''		THEN 'DUMMY'	ELSE qd.WorkCode END as WorkCode,row_number() over(order by(qd.QuoteDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'DCS_QuoteDet.csv' as Source_File,	  CONVERT(nvarchar,qd.LastModDate, 23) as PreviousModDate,'DESERT' as LoadForCompany FROM QuoteDet qd" + " Order by QuoteNo"

df = pd.read_sql(sql=sqlQuery, con=connDCS)
df.to_csv('DCS_Quote_Det.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"QuoteDet-DCS_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)




#SCC Quote Detail Load
myQuery           = """Select LastModifiedDate from QuoteDet__c Where QuoteDet_ID__c like 'SCC_%' and Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1""" 
LastRunDate = getLatestRunDate(myQuery)
sqlQuery="SELECT qd.ItemNo,qd.PartNo,qd.Qty1,qd.Qty2,qd.Qty3,qd.Qty4,qd.Qty5,qd.Qty6,qd.Qty7,qd.Qty8,qd.Price1,qd.Price2,qd.Price3,qd.Price4,qd.Price5,qd.Price6,qd.Price7,qd.Price8,qd.JobNo,qd.JobNotes,qd.QuoteNo,qd.Status,SUBSTRING(qd.Descrip,1,80) as Name,'SCC_' + CONVERT(nvarchar,qd.QuoteDet_ID) as QuoteDet_ID,'SCC_'+ qd.QuoteNo as LookupValForOpp,CASE WHEN qd.WorkCode is Null		THEN 'DUMMY'	WHEN qd.WorkCode = ''		THEN 'DUMMY'	ELSE qd.WorkCode END as WorkCode,row_number() over(order by(qd.QuoteDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_QuoteDet.csv' as Source_File,	  CONVERT(nvarchar,qd.LastModDate, 23) as PreviousModDate,'STANDARD' as LoadForCompany FROM QuoteDet qd" + " WHERE CONVERT(VARCHAR,qd.LastModDate,127) > " + "'"+ LastRunDate + "'" + " Order by QuoteNo"
#sqlQuery="SELECT qd.ItemNo,qd.PartNo,qd.Qty1,qd.Qty2,qd.Qty3,qd.Qty4,qd.Qty5,qd.Qty6,qd.Qty7,qd.Qty8,qd.Price1,qd.Price2,qd.Price3,qd.Price4,qd.Price5,qd.Price6,qd.Price7,qd.Price8,qd.JobNo,qd.JobNotes,qd.QuoteNo,qd.Status,SUBSTRING(qd.Descrip,1,80) as Name,'SCC_' + CONVERT(nvarchar,qd.QuoteDet_ID) as QuoteDet_ID,'SCC_'+ qd.QuoteNo as LookupValForOpp,CASE WHEN qd.WorkCode is Null		THEN 'DUMMY'	WHEN qd.WorkCode = ''		THEN 'DUMMY'	ELSE qd.WorkCode END as WorkCode,row_number() over(order by(qd.QuoteDet_ID)) as RowNum_Of_Source_File,	  'Y' as LoadedByPython,	  GetDate() as LoadDate,	  'SCC_QuoteDet.csv' as Source_File,	  CONVERT(nvarchar,qd.LastModDate, 23) as PreviousModDate,'STANDARD' as LoadForCompany FROM QuoteDet qd" + " Order by QuoteNo"

df = pd.read_sql(sql=sqlQuery, con=connSCC)
df.to_csv('SCC_Quote_Det.csv',encoding="utf-8-sig")

# Specify the file path and name
csv_file = outputDIR + "\\DateCompare\\" +"QuoteDet-SCC_" + file_datetime + ".csv"
createLatestRunDates(csv_file, LastRunDate)




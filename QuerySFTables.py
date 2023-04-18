import pyodbc
import os
import pandas as pd 
from datetime import datetime
import json
from simple_salesforce import Salesforce, SalesforceLogin, SFType

sf = Salesforce(username='aliasadmin@desertpowder.com', password='Welcome2Alias', consumer_key='3MVG9ux34Ig8G5epuXWEQpQ7Gz_zuuv2Soyr2ZwaDScXJyqC1EqxbHYqUZfZ7Ftgstaq_G0gfHorcViPUeX1a', consumer_secret='A10B5FBDBB8FF0B968BA8B44C32267F45477F3B23EA738B941D83038759E3476')

#create SQL Connection
#connDCS = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',host='DPC-APP02', database ='DCS-Shop', user ='sa', password='E2@DesertPC')
#connSCC = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',host='DPC-APP02', database ='SCC-Shop', user ='sa', password='E2@DesertPC')

#print(conn)
# Select MAX(LastModifiedDate) from Contact Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null
# WHERE LastModDate > '2023-01-01'
# Need to convert 2023-04-15T14:28:24.000+0000 to 2023-04-15T14:28:24 

try:
    #querySOQL = """Select MAX(LastModifiedDate) from Contact Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null"""
    querySOQL = """Select LastModifiedDate from Contact Where Loaded_From_Python_Process__c = 'Y' and LastModifiedDate <> null order by LastModifiedDate desc limit 1"""
    
    #print(querySOQL)
    qryResult = sf.query(querySOQL)
    
    df       = pd.DataFrame(qryResult['records'])
    strval   = str(df["LastModifiedDate"])
    strstrip = strval[1:24]
       
    
    print('****************************************************')
    print('The last run load for this object was: ' + strstrip)
    print('****************************************************')
except Exception as e:
    #print type(e)
    print(e)
    #print("Connection Failed due to socket - {}").format(error)
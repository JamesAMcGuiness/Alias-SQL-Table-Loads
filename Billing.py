#Change Log
# 3/10/2023 Initial Version

import requests as req
import json
import csv
import urllib.parse
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
import os
import datetime
import errorLog

#########################################################################
# For any field, this will apply any special logic defined for that field
#########################################################################
def logic_to_apply(row):

    #The List Of fields we have unique logic for...
    theColumnList =	["Invoice_Status__c","Paid_to_Date__c"]

    for columnname in theColumnList:

        #**************************************************************************************
        #                              Invoice_Status__c
        #**************************************************************************************   
        if columnname == 'Invoice_Status__c':
            #print('Checking Invoice Status=*' + row["Invoice_Status__c"] + '*')
            if row["Invoice_Status__c"] == 'P':
                #print('It is a P')
                row["Invoice_Status__c"] = 'Paid'
            else:
                #print('It is NOT a P')
                row["Invoice_Status__c"] = 'Unpaid'        

        #**************************************************************************************
        #                              Invoice_Date__c
        #**************************************************************************************   
        #if columnname == 'Invoice_Date__c':
            
            #try:

            #    if row["Invoice_Date__c"] != None and row["Invoice_Date__c"] != '': 
            #        row["Invoice_Date__c"] = datetime.datetime.strptime(row["Invoice_Date__c"], "%m/%d/%y").strftime("%Y-%m-%d")
            #except ValueError:
            
            #    try:
            #        if row["Invoice_Date__c"] != None and row["Invoice_Date__c"] != '': 
            #            row["Invoice_Date__c"] = datetime.datetime.strptime(row["Invoice_Date__c"], "%m/%d/%Y").strftime("%Y-%m-%d")
            #            #print('Successfully used the 4 digit format!')
            #    except ValueError:
            #        #If it reaches here the date is already in a valid format
            #        #print("Date already in valid format! *" + row["Invoice_Date__c"] + "*")
                        

        #**************************************************************************************
        #                              Paid To Date
        #**************************************************************************************   
        if columnname == 'Paid_to_Date__c':
            if (row["Paid_to_Date__c"] == None or row["Paid_to_Date__c"] == 'NULL'):    
                row["Paid_to_Date__c"] = 0

def post_batch_salesforce(disbursals, bulk, job):
    csv_iter = CsvDictsAdapter(iter(disbursals))
    batch = bulk.post_batch(job, csv_iter)
    # bulk.wait_for_batch(job, batch)
    print("Done. Batch Uploaded.")
    return batch


def salesforce_connect_and_upload(filename, thost, tsessionId, tsandbox, tusername, tpassword, tsecurity_token, tclient_id,key, tobject_name, theader, tex_id, concurrency_type,runtype,client=''):

    bulk = SalesforceBulk(host=thost, sessionId=tsessionId, sandbox=tsandbox,
        username=tusername,
        password= tpassword,
        security_token = tsecurity_token,
        client_id=tclient_id)
    print('****************************************************')						  
    print('In salesforce_connect_and_upload for Billing')						
    print('****************************************************')						  

    job = bulk.create_upsert_job(object_name = tobject_name, external_id_name=tex_id, concurrency='Serial')
    
    print(job)

    thedate    = datetime.datetime.now()
    theyear    = thedate.year
    themonth   = thedate.month
    theday     = thedate.day
    thedatestr = str(theday) + " " + str(themonth) + " " + str(theyear)
    header     = theader

    print('****************************************************')						  	
    print('file to process is ' + filename)
    print('****************************************************')						  
	
    with open(filename, encoding="utf-8-sig") as file:
        reader = csv.DictReader(file,fieldnames=header)    
        disbursals = []
        batches    = []    
        count      = 1
        ignr_head = True
        
        for row in reader:
            if ignr_head:
                ignr_head = False
                continue        
        				
            
			
            ############################## To apply any special logic for a field #########################################################################
            logic_to_apply(row)
            ################################################################################################################################################### 			
			
            # For any header columns with _del in them, we want to remove them from processing
            for head in header:
                if "_del" in head or "_Del" in head:
                    row.pop(head)

            
            #print(row)
            count = count + 1
            disbursals.append(row)
            print('Row to add is ') 
            print(row)
            print('*********************************************************************************')
            if (count / 10000) == 1:
                batches.append(post_batch_salesforce(disbursals, bulk, job))
                count = 1
                disbursals[:] = []
                
        if len(disbursals) > 0:        
            batches.append(post_batch_salesforce(disbursals, bulk, job))
        
        
        bulk.close_job(job)
        
        for batch in batches:
            bulk.wait_for_batch(job, batch, 60 * 60)
            print('Waiting for batch...')

        datetime_object = datetime.datetime.now()
        #print(datetime_object)
        
        row = [datetime_object]    
        theFilePath = os.environ.get("op_path") + '\Trigger.txt'

        #Create error file if not     
        errorLog.error_log(bulk, job, batches, filename, "ERROR", "SUCCESS",runtype,"utf-8-sig")
    

        #with open(theFilePath,'w') as csvFile:
        #        writer = csv.writer(csvFile)
        #        writer.writerow(row)
        #        print('==========================================>Created a touch file')
            
        #        csvFile.close()  
    
    return

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
    theColumnList =	["payout__Commission_Batch_Unique_ID__c"]

    for columnname in theColumnList:

        #**************************************************************************************
        #
        #**************************************************************************************   
        if columnname == '<payout__Commission_Batch_Unique_ID__c>':
            print('We have setup logic for this field: ' + columnname)			                
        else:
            print('We have not setup logic for this field: ' + columnname)			



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
    print('In salesforce_connect_and_upload for Company')						
    print('****************************************************')						  

    job = bulk.create_upsert_job(object_name = tobject_name, external_id_name=tex_id, concurrency=concurrency_type)
    
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
			
            ############################## To apply any special logic for this client #########################################################################
            #logic_to_apply(client,row)
            ################################################################################################################################################### 			
			
            # For any header columns with _del in them, we want to remove them from processing
            for head in header:
                if "_del" in head or "_Del" in head:
                    row.pop(head)

            #*****************************************************************
            # Date Transformations...
            #***************************************************************** 					   											
            #try:

            #*****************************************************************
            # payout__Statement_Date__c - Transformation to SF Date
            #***************************************************************** 					   											
            #    if row["payout__Statement_Date__c"] != None and row["payout__Statement_Date__c"] != '': 
            #        row["payout__Statement_Date__c"] = datetime.datetime.strptime(row["payout__Statement_Date__c"], "%m/%d/%y").strftime("%Y-%m-%d")
            #except ValueError:
            
            #    try:
            #        if row["payout__Statement_Date__c"] != None and row["payout__Statement_Date__c"] != '': 
            #            row["payout__Statement_Date__c"] = datetime.datetime.strptime(row["payout__Statement_Date__c"], "%m/%d/%Y").strftime("%Y-%m-%d")
            #            print('Successfully used the 4 digit format!')
            #    except ValueError:
            #        print("Date ValueERROR for payout__Statement_Date__c! *" + row["payout__Statement_Date__c"] + "*")
			
            count = count + 1
            disbursals.append(row)
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
        errorLog.error_log(bulk, job, batches, filename, "ERROR", "SUCCESS",runtype,"latin-1")
    

        #with open(theFilePath,'w') as csvFile:
        #        writer = csv.writer(csvFile)
        #        writer.writerow(row)
        #        print('==========================================>Created a touch file')
            
        #        csvFile.close()  
    
    return

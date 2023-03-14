
#Change Log
# 1/12/2021 Initial Version

import requests as req
import json
import csv
import urllib.parse
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
import os
import datetime
import errorLog


def logic_to_apply(client,row):

          print('The client we are processing for is ****' + client + '****')


          #theColumnList =	["payout__Line_of_Business__c","payout__X12b1_Batch__c","RecordTypeId","payout__Payor__c","payout__Commission_Batch_Unique_ID__c","payout__Statement_Date__c","payout__Auto_Calc_Total_Gross__c"]
          #listofclients = ["PKS","BCG"]

          #The List Of fields we have unique logic for...
          theColumnList =	["payout__Commission_Batch_Unique_ID__c"]
		  
          for columnname in theColumnList:

            #**************************************************************************************
            #payout__Commission_Batch_Unique_ID__c
            #**************************************************************************************   
            print('payout__Commission_Batch_Unique_ID__c')

            if columnname == 'payout__Commission_Batch_Unique_ID__c':
			
                 if client == 'PKS':

                      temp = row["Source__c"] + " - " + row["payout__Clearing_Firm__c_del"] + " - "
                      if row["Source__c"] == "DTCC":
                           temp = temp + row["payout__Transaction_Date__c_del"] + " - " 

                      else:
                           temp = temp + row["payout__Statement_Date__c"] + " - " 
                 
                      temp = temp + row["payout__Line_of_Business__c"] + " Trail - " + row["payout__Trail__c_del"] 
                      row["payout__Commission_Batch_Unique_ID__c"] = temp				 
				 
				 
				 
                 elif client == 'BCG': 

                      trailstr = ""            
                      temp = row["Source__c"] + " - " + row["payout__Clearing_Firm__c_del"] + " - " + row["payout__Statement_Date__c"] + " - " + row["payout__Line_of_Business__c"]
                      if row["payout__Clearing_Firm__c_del"] == "DTOIM":
                           if row["payout__Trail__c_del"] == "Y":
                                if row["TransType"] == "12B1":
                                     trailstr = " - 12B1"
                                else:
                                     trailstr = " - TRAIL"					  
                           else:		   
                                trailstr = " - REGULAR"

                      else:
                           if row["payout__Trail__c_del"] == "Y":
                                trailstr = " - TRAIL"                
                           else:
                                trailstr = " - REGULAR"
					  
                      temp = temp + trailstr
                      row["payout__Commission_Batch_Unique_ID__c"] = temp
					  
					  
                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			   


		  
            #******************************************************************
            #payout__Line_of_Business__c
            #******************************************************************

            elif columnname == 'payout__Line_of_Business__c':
			

                 if client == 'BCG': 
                      print('payout__Line_of_Business__c')
                      if row["payout__Line_of_Business__c"] != "Advisory":
                           row["payout__Line_of_Business__c"] = "Investments"

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			   

					  
					  
            #**************************************************************************************
            #payout__X12b1_Batch__c
            #**************************************************************************************              			
            elif columnname == 'payout__X12b1_Batch__c':

                 if client in listofclients: 
                      print('payout__X12b1_Batch__c')
                      if row["payout__Trail__c_del"] == 'Y':
                           row["payout__X12b1_Batch__c"] = "True" 
                      else:
                           row["payout__X12b1_Batch__c"] = "False" 

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			   

					  
            #**************************************************************************************
            #RecordTypeId
            #**************************************************************************************
						  
            elif columnname == 'RecordTypeId':
			
				 
                 if client in listofclients: 
                      print('RecordTypeId')			
                      row["RecordTypeId"] = os.environ['DefaultLOBRTID']
                      if row["payout__Line_of_Business__c"] == "Advisory":
                           row["RecordTypeId"] = os.environ['AdvisoryRTID']

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			   
					  
					  
					  
					  
            #**************************************************************************************
            #payout__Payor__c
            #**************************************************************************************              			
					  
            elif columnname == 'payout__Payor__c':

                 if client in listofclients: 
                      row["payout__Payor__r.payout__SSN__c"] = row["payout__Clearing_Firm__c_del"]

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			   
						  
					  
					  



            #**************************************************************************************
            #payout__Statement_Date__c
            #**************************************************************************************    
		
            elif columnname == 'payout__Statement_Date__c':
			

                 if client == 'BCG': 
                      if row["Source__c"] == "DTCC":
                           row["payout__Statement_Date__c"] = row["payout__Transaction_Date__c_del"]					  


                 else:
                      print('ERROR: - The client needs to be setup - ' + client)


            #********************************   
            #payout__Auto_Calc_Total_Gross__c
            #********************************   

            elif columnname == 'payout__Auto_Calc_Total_Gross__c':
			
                 

                 if client == 'BCG': 
                      row["payout__Auto_Calc_Total_Gross__c"] = "True"

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)



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
    print('In salesforce_connect_and_upload for CBU_CommBatch')						
    print('****************************************************')						  
	
    print(bulk)
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
        batches = []    
        count = 1
        ignr_head = True
        for row in reader:
            if ignr_head:
                ignr_head = False
                continue
				
            #**************************************************************************************
            #payout__X12b1_Batch__c
            #**************************************************************************************              			
            if row["payout__Trail__c_del"] == 'Y':
               row["payout__X12b1_Batch__c"] = "True" 
            else:
               row["payout__X12b1_Batch__c"] = "False" 
		

            #**************************************************************************************
            #RecordTypeId
            #**************************************************************************************              			
            row["RecordTypeId"] = os.environ['CBDefaultLOBRTID']
            if row["payout__Line_of_Business__c"] == "Advisory":
               row["RecordTypeId"] = os.environ['CBAdvisoryRTID']
			
            else:
                if row["payout__Line_of_Business__c"] == "Insurance":			
                     row["RecordTypeId"] = os.environ['CBInsuranceRTID']
                     print('Assigned an Insurance LOB') 			
			
            #**************************************************************************************
            #payout__Payor__c
            #**************************************************************************************              			
            # 9/5/21		
            row["payout__Payor__r.payout__dl_AccountNumber_del__c"] = row["payout__Clearing_Firm__c_del"]
            
			
			
			
            #***********************************************************************************************************************************************************************
            #payout__Commission_Batch_Unique_ID__c - This is now being loaded directly from Commission Batch CSV (CBUniqueID) - comment out below when we get the new CB CSV from DP
            #***********************************************************************************************************************************************************************              			
            #temp = row["Source__c"] + " - " + row["payout__Clearing_Firm__c_del"] + " - "
            #if row["Source__c"] == "DTCC":
            #     temp = temp + row["payout__Transaction_Date__c_del"] + " - " 

            #else:
            #     temp = temp + row["payout__Statement_Date__c"] + " - " 
                 
            #temp = temp + row["payout__Line_of_Business__c"] + " Trail - " + row["payout__Trail__c_del"] 
            #row["payout__Commission_Batch_Unique_ID__c"] = temp            
            #print('==========================================> UniqueID for CB*****' + temp + '***********')
			
			
            #**************************************************************************************
            #payout__Statement_Date__c
            #**************************************************************************************              			
            #payout__Statement_Date__c is initially mapped in header to load with ValuationDate

            # Load the SettleDate not ValuationDate for DTCC
            if row["Source__c"] == "DTCC":
               row["payout__Statement_Date__c"] = row["payout__Transaction_Date__c_del"]
            
            
			#Set FileType picklist to 'Cirrus Commission'
            #row["payout__File_Type__c"] = 'Cirrus Commission'
			
            ############################## To apply any special logic for this client #########################################################################
            #logic_to_apply(client,row)
            ################################################################################################################################################### 			
           	
            #row["payout__Manufacturer__r.payout__dl_AccountNumber_del__c"] = row["payout__Clearing_Firm__c_del"]   
			
            for head in header:
                if "_del" in head or "_Del" in head:
                    row.pop(head)

            #*****************************************************************
            # Date Transformations...
            #***************************************************************** 					   											
            try:

            #*****************************************************************
            # payout__Statement_Date__c - Transformation to SF Date
            #***************************************************************** 					   											
                if row["payout__Statement_Date__c"] != None and row["payout__Statement_Date__c"] != '': 
                    row["payout__Statement_Date__c"] = datetime.datetime.strptime(row["payout__Statement_Date__c"], "%m/%d/%y").strftime("%Y-%m-%d")
            except ValueError:
            
                try:
                    if row["payout__Statement_Date__c"] != None and row["payout__Statement_Date__c"] != '': 
                         row["payout__Statement_Date__c"] = datetime.datetime.strptime(row["payout__Statement_Date__c"], "%m/%d/%Y").strftime("%Y-%m-%d")
                         print('Successfully used the 4 digit format!')
                except ValueError:
                    print("Date ValueERROR for payout__Statement_Date__c! *" + row["payout__Statement_Date__c"] + "*")


            print(' ')					
            print('======================================================> RecordTypeId assigned is ' + row["RecordTypeId"])
            print(' ')            
			
			
			
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
    

        with open(theFilePath,'w') as csvFile:
                writer = csv.writer(csvFile)
                writer.writerow(row)
                print('==========================================>Created a touch file')
            
                csvFile.close()  
    
    return


  

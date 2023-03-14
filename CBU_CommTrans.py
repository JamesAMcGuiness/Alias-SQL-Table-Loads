#Change Log
# 1/12/2021 Initial Version 1.0
# 2/16/2021 Made CB as Upsert instead of Insert
#           Added logic for using Clearing Trade Rep Id as part of the holding lookup if one is provided else we use FA_CUSIP or concatenation of FA + CUSIP 
# 2/23/2021 Use mapping from new Commission Batch CSV - Only change was to map CBUniqueID to payout__Commission_Batch_Unique_ID__c
#           Use mapping from new Commission Trans CSV - Use CBUniqueID to map  payout__Commission_Batch__r
#
# 3/5/2021  Assign RecordTypeId just as we do in CB logic
# 3/9/2021 Use payout__Clearing_SSN__c instead of payout__Clearing_SSN__c_del since we are now mapping this field as to be actually loaded
# 3/22/2021 Had to find/replace all references to Account_del and replace with new mapped field payout__Clearing_FA_number__c (For MPs latest mapping request in Case 00019131)
# 3/29/2021 Recordlocking code update
# 4/7/2021 New logic for Holding lookup is now the following:
#            if row["LOB_del"] == 'Investments':
#                if runtype != 'MISSINGHLD' and runtype != 'MISSINGFA':
#                     if row["payout__This_Trade_Only_Rep_ID__c"] != None and row["payout__This_Trade_Only_Rep_ID__c"] != '':
#                            row["payout__Holding__r.payout__FA_CUSIP__c"] = row["FACUSIP_del"] + row["payout__This_Trade_Only_Rep_ID__c"] 						
#                     else:
#                           row["payout__Holding__r.payout__FA_CUSIP__c"] = row["FACUSIP_del"]
#
#
#
################################################################################################################################################################
# Version: CBU 1.0
# DataPro Version: V 4.43 +
# Payout Version:  V 12.20+ 
#  
################################################################################################################################################################
#######################################################################################################################################################
# Version CommBL_1.14
# 6/29/21 Load Insurance LOB for recordtype
#
# Version CommBL_1.17
# 9/5/21 - Addded row["payout__Manufacturer__r.payout__dl_AccountNumber_del__c"] = row["payout__Clearing_Firm__c"] 


import requests as req
import csv
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
import os
import sys
import datetime
from datetime import datetime as dt
import errorLog

def logic_to_apply(client,row):

          #theColumnList = ["payout__Statement_Advisor__c","payout__Source_Data__c","payout__Unique_Transaction_ID__c","payout__Holding__c","payout__Financial_Account__c","payout__Commission_Batch__c","payout__Data_Source__c","payout__Trail__c","payout__Account_Value__c","payout__Statement_Rep_ID__c","payout__Security_Type__c","TC_Paid_by_Client__c"]

          #The List Of fields we have unique logic for...
          theColumnList = ["payout__Commission_Batch__r"]
          listofclients = ["PKS","BCG"]

          #################################################################### NOTE ################################################################################################################		  
          #                        The logic for FA and Holding lookups are done in the "code common area" outside this function 		  
          ##########################################################################################################################################################################################		  
		  
          print('In logic_to_apply...The client we are processing for is ****' + client + '****')
		  
          for columnname in theColumnList:
            print('The column we are processing is ' + columnname)   
          		  
            #**************************************************************************************
            #payout__Commission_Batch__r - Links back to the Commission Batch 
            #**************************************************************************************   

            if columnname == 'payout__Commission_Batch__r':
                 print('The column we are procesing is payout__Commission_Batch__r') 			
                 if client == 'PKS':

                      #print('1')				 
                      temp = row["payout__Data_Source__c"] + " - " + row["payout__Clearing_Firm__c"] + " - "
                      #print('Temp = ' + temp)
					  

                      #print('2')				 					  
                      if row["payout__Data_Source__c"] == "DTCC":
                           temp = temp + row["payout__Transaction_Date__c"] + " - " 

                           #print('3')				 						   
                      else:
                           temp = temp + row["ValuationDate_del"] + " - " 

                      #print('4')				 

                      #print(row["payout__Trail__c"] )
					  
                      temp = temp + row["LOB_del"] + " Trail - " + row["payout__Trail__c"] 
					  
                      #print('5')				 					  
                      row["payout__Commission_Batch__r.payout__Commission_Batch_Unique_ID__c"] = temp				 
                      #print('5')				 				 
				 
				 
                 elif client == 'BCG': 
                      print('Client is BCG')
                      trailstr = ""            
                      temp = row["payout__Data_Source__c"] + " - " + row["payout__Clearing_Firm__c"] + " - " + row["ValuationDate_del"] + " - " + row["LOB_del"]
                      if row["payout__Clearing_Firm__c"] == "DTOIM":
                           if row["payout__Trail__c"] == "Y":
                                if row["TransType"] == "12B1":
                                     trailstr = " - 12B1"
                                else:
                                     trailstr = " - TRAIL"					  
                           else:		   
                                trailstr = " - REGULAR"

                      else:
                           if row["payout__Trail__c"] == "Y":
                                trailstr = " - TRAIL"                
                           else:
                                trailstr = " - REGULAR"
					  
                      temp = temp + trailstr
                      row["payout__Commission_Batch__r.payout__Commission_Batch_Unique_ID__c"] = temp
                      print('the lookup value was set to ' + temp)
					  
                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			   

		  
		  
		  
            #************************************
            #payout__Commission_Batch__c
            #************************************
            elif columnname == 'payout__Commission_Batch__c':
			
                 if client == 'PKS': 
				 
                      if row["payout__Unique_Transaction_ID__c"] == None or row["payout__Unique_Transaction_ID__c"] == "": 
            
                           temp = row["payout__Data_Source__c"] + " - " + row["payout__Clearing_Firm__c"] + " - "
                           if row["payout__Data_Source__c"] == "DTCC":
                                temp = temp + row["payout__Transaction_Date__c_del"] 

                           else:
                                temp = temp + row["ValuationDate_del"] 
                  
                      temp = temp + " -" + row["LOB_del"] + " Trail - " + row["payout__Trail__c"] 
                      row["payout__Commission_Batch__c"] = temp 
                      #print('Set payout__Commission_Batch__c to ' + row["payout__Commission_Batch__c"])					  
				 
                 elif client == 'BCG': 
 
                      trailstr = ""            
                      temp = row["payout__Data_Source__c"] + " - " + row["payout__Clearing_Firm__c"] + " - " + row["ValuationDate_del"] + " - " + row["LOB_del"]
                      if row["payout__Clearing_Firm__c"] == "DTOIM":
                           if row["payout__Trail__c"] == "Y":
                                if row["TransType"] == "12B1":
                                     trailstr = " - 12B1"
                                else:
                                     trailstr = " - TRAIL"					  
                           else:		   
                                trailstr = " - REGULAR"

                      else:
                           if row["payout__Trail__c"] == "Y":
                                trailstr = " - TRAIL"                
                           else:
                                trailstr = " - REGULAR"
					  
                      temp = temp + trailstr
                      row["payout__Commission_Batch__c"] = temp 
                      #print('Set payout__Commission_Batch__c to ' + row["payout__Commission_Batch__c"])					  


                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			


		  
            elif columnname == 'payout__Trail__c':
                 if row["payout__Trail__c"] == "Y":
                      #print('Trail is Y') 
                      row["payout__Trail__c"] = 1
                      #print('Just set Trail to 1')     
                 else:
                      row["payout__Trail__c"] = 0
		  

            #******************************************************************
            #payout__Statement_Advisor__c (Not currently being mapped per Mike)
            #******************************************************************

				 

            #**********************************
            #payout__Source_Data__c
            #**********************************

            elif columnname == 'payout__Source_Data__c':
                 if client in client:
                      row["payout__Source_Data__c"] = "SSN - " + row["payout__Clearing_SSN__c"] + " FA - " + row["payout__Clearing_FA_number__c"] + " Cusip - " + row["payout__CUSIP_Value__c"] + " RepID - " + row["payout__Statement_Rep_ID__c"]                      

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			
			
			

            #********************************
            #payout__Unique_Transaction_ID__c
            #********************************
            
            elif columnname == 'payout__Unique_Transaction_ID__c':

                 if client in client:
                      row["payout__Unique_Transaction_ID__c"] = row["payout__Clearing_Trade_Ref_Number__c"] + row["payout__Commission_Batch__r.payout__Commission_Batch_Unique_ID__c"]
					  


                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			
			
			
            #*******************************
            #payout__Holding__c - Check on what logic to use both are shown below
            #*******************************						
			
            elif columnname == 'payout__Holding__c':
                 row["payout__Holding__r.payout__FA_CUSIP__c"] = None				
                 
                 if client == 'PKS': 

                      #ONLY LOAD For LOB='Investments'....
                      # Use FA + Cusip instead of FACUSIP to link Holding providing it is LOB of Investments and we are not loading the Missing Holding records file or Missing FA file
                      #When Source = DTCC: vlookup( payout__Holdings__c, ID, payout__FA_Cusip__c, AccountNumber & CUSIP & if(not(isnull(ClrgTrdRepID)), ClrgTrdRepID, "")
                     #payout__This_Trade_Only_Rep_ID__c holds the ClrgTrdRepID value
                     if row["LOB_del"] == 'Investments':
                          if runtype != 'MISSINGHLD' and runtype != 'MISSINGFA':
                               #Below we will be using in phase II				
                               #if row["payout__Data_Source__c"] == "DTCC":
                               #    if row["payout__This_Trade_Only_Rep_ID__c"] != None and row["payout__This_Trade_Only_Rep_ID__c"] != '':
                               #        row["payout__Holding__r.payout__FA_CUSIP__c"] = row["payout__Clearing_FA_number__c"] + row["payout__CUSIP_Value__c"] + row["payout__This_Trade_Only_Rep_ID__c"] 						
                                    #else:
				
                               #   #print('Missing Holding Run for Investments LOB - Set the Holding lookup')
                                       if row["payout__Data_Source__c"] != "NFSC": 					
                                          row["payout__Holding__r.payout__FA_CUSIP__c"] = row["payout__Clearing_FA_number__c"] + row["payout__CUSIP_Value__c"]
                                       else:
                                          row["payout__Holding__r.payout__FA_CUSIP__c"] = row["FACUSIP_del"]				 




										  
				 
                 elif client == 'BCG': 
                      if runtype != 'MISSINGFA' and runtype != 'MISSINGHLD': 
                           row["payout__Holding__r.payout__FA_CUSIP__c"] = row["FACUSIP_del"] 

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)				 
       			
			
            #*******************************
            #payout__Financial_Account__c
            #*******************************
			
            elif columnname == 'payout__Financial_Account__c':			
                 row["payout__Financial_Account__r.payout__Financial_Account_Number__c"] = None
                 		
                 if client in listofclients: 

			          #Only set lookup of FA, if we are not running the Missing FA load...
                      if runtype != 'MISSINGFA':
                           #print('Trying to assign the FA lookup')			
                           row["payout__Financial_Account__r.payout__Financial_Account_Number__c"] = row["payout__Clearing_FA_number__c"]
                           #print('Successfully assigned the FA lookup')					 


                 else:
                      print('ERROR: - The client needs to be setup - ' + client)				 
			
			

			
            #********************************************************
            #payout__Data_Source__c - Check on if ok to use PKS logic for BCG
            #********************************************************
			
            elif columnname == 'payout__Data_Source__c':

                 if client in listofclients: 

                      row["payout__Source_Data__c"] = "SSN - " + row["payout__Clearing_SSN__c"] + " FA - " + row["payout__Clearing_FA_number__c"] + " Cusip - " + row["payout__CUSIP_Value__c"] + " RepID - " + row["payout__Statement_Rep_ID__c"]                                                                                        
				 

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)				 
			
			

            #*********************************************************
            # payout__Account_Value__c
            #********************************************************* 			

            elif columnname == 'payout__Account_Value__c':
                 row["payout__Account_Value__c"] = 0
                 if client == 'PKS':
                      if row["payout__Data_Source__c"] == "DTCC":
                           row["payout__Account_Value__c"] = row["payout__Transaction_Amount__c"]
					  

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			

					  
					  
            #*********************************************************
            # payout__Statement_Rep_ID__c
            #********************************************************* 			
					  
            elif columnname == 'payout__Statement_Rep_ID__c':

                 if client == 'PKS': 
                      if row["payout__Data_Source__c"] == "DTCC":
                           row["payout__Account_Value__c"] = row["payout__Transaction_Amount__c"]
                
                           #When Source = DTCC: If(ISNULL(ClrgTrdRepID), RepID, ClrgTrdRepID)    
                           #Overlay the assignment in the header which assigns it to REPID

                           if row["payout__This_Trade_Only_Rep_ID__c"] != "":
                                row["payout__Statement_Rep_ID__c"] = row["payout__This_Trade_Only_Rep_ID__c"]	
					  

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			
			

            #*********************************************************
            # payout__Security_Type__c
            #********************************************************* 			
			
            elif columnname == 'payout__Security_Type__c':
			
                 if client == 'PKS': 

                      if row["payout__Data_Source__c"] != "DTCC" and row["payout__Data_Source__c"] != "DST IDC" and row["payout__Data_Source__c"] != "DAZL" and row["payout__Data_Source__c"] != "NSCC":    
                           row["payout__Security_Type__c"] = row["Payout__Clearing_Security_Type__c"]
				 
					  

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)
			
			



def post_batch_salesforce(disbursals, bulk, job):
    csv_iter = CsvDictsAdapter(iter(disbursals))
    batch = bulk.post_batch(job, csv_iter)
    # bulk.wait_for_batch(job, batch)
    print("Done. Batch Uploaded.")
    return batch			
			
def salesforce_connect_and_upload(filename, thost, tsessionId, tsandbox, tusername, tpassword, tsecurity_token,
                                          tclient_id, key, tobject_name, theader, tex_id, concurrency_type, runtype='',client=''):
 
    bulk = SalesforceBulk(host=thost, sessionId=tsessionId, sandbox=tsandbox,
                          username=tusername,
                          password=tpassword,
                          security_token=tsecurity_token,
                          client_id=tclient_id)
    print(bulk)
    job = bulk.create_upsert_job(object_name=tobject_name, external_id_name=tex_id, concurrency=concurrency_type)
    print(job)
    print('*********************************** the file to process is: ' + filename)
    header = theader
    reader = csv.DictReader(open(filename, encoding="utf-8-sig"), fieldnames=header)
    
    thedate    = datetime.datetime.now()
    theyear    = thedate.year
    themonth   = thedate.month
    theday     = thedate.day
    thedatestr = str(theday) + " " + str(themonth) + " " + str(theyear)
    

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
            #RecordTypeId
            #**************************************************************************************              			
            row["RecordTypeId"] = os.environ['CTDefaultLOBRTID']
            if row["LOB_del"] == "Advisory":
               print('Advisory')			
               row["RecordTypeId"] = os.environ['CTAdvisoryRTID']				

            else:
                if row["LOB_del"] == "Insurance":			
                     row["RecordTypeId"] = os.environ['CTInsuranceRTID']
			   
			
            			   
            #*********************************************************
            # payout__Unique_Transaction_ID__c
            #********************************************************* 		
            row["payout__Unique_Transaction_ID__c"] = row["payout__Clearing_Trade_Ref_Number__c"] + row["payout__Unique_Transaction_ID__c"]

            #*********************************************************
            # payout__Source_Data__c
            #********************************************************* 		
            row["payout__Source_Data__c"] = "SSN - " + row["payout__Clearing_SSN__c"] + " FA - " + row["payout__Clearing_FA_number__c"] + " Cusip - " + row["payout__CUSIP_Value__c"] + " RepID - " + row["payout__Statement_Rep_ID__c"]

            #*********************************************************
            # payout__Account_Value__c
            #********************************************************* 		
            row["payout__Account_Value__c"] = 0
            if row["payout__Data_Source__c"] == "DTCC":
                row["payout__Account_Value__c"] = row["payout__Transaction_Amount__c"]
                

                if row["payout__This_Trade_Only_Rep_ID__c"] != "":
                    row["payout__Statement_Rep_ID__c"] = row["payout__This_Trade_Only_Rep_ID__c"]
                    
                    
                # Mike said we could comment this assignment out    
                #When Source = DTCC: vlookup( Account, ID,  AccountNumber, If(ISNULL(ClrgTrdRepID), RepID, ClrgTrdRepID))
                #row["payout__Statement_Advisor__r.payout__SSN__c"] = row["payout__Financial_Account__r.payout__Financial_Account_Number__c"]
                #if row["payout__This_Trade_Only_Rep_ID__c"] == "" or row["payout__This_Trade_Only_Rep_ID__c"] == None:
                #    row["payout__Statement_Advisor__r.payout__SSN__c"] = row["payout__Statement_Rep_ID__c"] 
                #else:
                #    row["payout__Statement_Advisor__r.payout__SSN__c"] = row["payout__This_Trade_Only_Rep_ID__c"]

            #*********************************************************
            # payout__Security_Type__c
            #********************************************************* 		
            ## Removed this check on 6/16/2021 Per Mike if row["payout__Data_Source__c"] != "DTCC" and row["payout__Data_Source__c"] != "DST IDC" and row["payout__Data_Source__c"] != "DAZL" and row["payout__Data_Source__c"] != "NSCC":    
            row["payout__Security_Type__c"] = row["Payout__Clearing_Security_Type__c"]
			   
            #*********************************************************
            # payout__Unique_Transaction_ID__c
            #********************************************************* 					   
            #row["payout__Unique_Transaction_ID__c"] = row["payout__Clearing_Trade_Ref_Number__c"] + row["payout__Commission_Batch__r.payout__Commission_Batch_Unique_ID__c"]
			
            #Default to NOT loading the the lookups for FA and Holdings			
            row["payout__Financial_Account__r.payout__Financial_Account_Number__c"] = None
            row["payout__Holding__r.payout__FA_CUSIP__c"] = None
			
            # ONLY LOAD For LOB='Investments'....
            #*********************************************************
            # payout__Holding__r.payout__FA_CUSIP__c
            #********************************************************* 					   			
            if row["LOB_del"] == 'Investments':
                if runtype != 'MISSINGHLD' and runtype != 'MISSINGFA':
                     if row["payout__This_Trade_Only_Rep_ID__c"] != None and row["payout__This_Trade_Only_Rep_ID__c"] != '':
                            row["payout__Holding__r.payout__FA_CUSIP__c"] = row["FACUSIP_del"] + row["payout__This_Trade_Only_Rep_ID__c"] 						
                     else:
                           row["payout__Holding__r.payout__FA_CUSIP__c"] = row["FACUSIP_del"]


							   
			#Only set lookup of FA, if we are not running the Missing FA load...
            #*****************************************************************
            # payout__Financial_Account__r.payout__Financial_Account_Number__c
            #***************************************************************** 					   						
            if runtype != 'MISSINGFA':
                #print('Trying to assign the FA lookup')			
                row["payout__Financial_Account__r.payout__Financial_Account_Number__c"] = row["payout__Clearing_FA_number__c"]
                #print('Successfully assigned the FA lookup')		



            #************************************************************************************************************************************************************************************************
            #payout__Commission_Batch__r - Links back to the Commission Batch This is now being loaded directly from Commission Trans CSV (CBUniqueID) - comment out below when we get the new CB CSV from DP
            #************************************************************************************************************************************************************************************************   

            #temp = row["payout__Data_Source__c"] + " - " + row["payout__Clearing_Firm__c"] + " - "
            #print('Temp = ' + temp)
            
            #if row["payout__Data_Source__c"] == "DTCC":
            #    temp = temp + row["payout__Transaction_Date__c"] + " - " 

            #    print('3')				 						   
            #else:
            #    temp = temp + row["ValuationDate_del"] + " - " 

            #    print(row["payout__Trail__c"] )
					  
            #temp = temp + row["LOB_del"] + " Trail - " + row["payout__Trail__c"] 
            #print('5')				 					  
            #row["payout__Commission_Batch__r.payout__Commission_Batch_Unique_ID__c"] = temp				 
            #print('================================================> The lookup value for the CT to CB is ' + temp)

			
            ############################## To apply any special logic for this client #########################################################################
            #logic_to_apply(client,row)
            ################################################################################################################################################### 			
						
			
            #**************************************************************************************
            #payout__Trail__c 
            #**************************************************************************************   
			
            if row["payout__Trail__c"] == "Y":
               #print('Trail is Y') 
               row["payout__Trail__c"] = 1
                      #print('Just set Trail to 1')     
            else:
               row["payout__Trail__c"] = 0

					
            for head in header:
                if "del" in head.lower():
                    row.pop(head)

					
            #*****************************************************************
            # Date Transformations...
            #***************************************************************** 					   											
            try:
           
		   
            #*****************************************************************
            # payout__Trade_Date__c
            #***************************************************************** 					   											
		   
                if row["payout__Trade_Date__c"] != "" and row["payout__Trade_Date__c"] != None:
                   row["payout__Trade_Date__c"] = datetime.datetime.strptime(row["payout__Trade_Date__c"], "%m/%d/%Y").strftime("%Y-%m-%d")  
                   #print("Successfully translated...")
            except ValueError:
                print("Error trying to translate payout__Trade_Date__c")    
 
            #*****************************************************************
            # payout__Transaction_Date__c
            #***************************************************************** 					   											
 
            try:
                if row["payout__Transaction_Date__c"] != "" and row["payout__Transaction_Date__c"] != None:
                   row["payout__Transaction_Date__c"] = datetime.datetime.strptime(row["payout__Transaction_Date__c"], "%m/%d/%Y").strftime("%Y-%m-%d")  
              
           
                #print("Successfully translated...")
            except ValueError:
                print("Error trying to translate payout__Transaction_Date__c") 


            #Please map mgtfee to TC_Paid_by_Client__c
            #row["TC_Paid_by_Client__c"] = row["payout__Clearing_Mgt_Fee__c"]
			   
			
			   
            count = count + 1
            disbursals.append(row)
            if (count / 10000) == 1:
               batches.append(post_batch_salesforce(disbursals, bulk, job))
               count = 1
               disbursals[:] = []
                
        
            #print("***********************************************************************Reading row for runtype..." + runtype)
            #print(row)
            
        if len(disbursals) > 0:        
            batches.append(post_batch_salesforce(disbursals, bulk, job))
                
       
        bulk.close_job(job)
        
        for batch in batches:
            bulk.wait_for_batch(job,batch)
            print('Waiting for batch...')
           
        datetime_object = datetime.datetime.now()

        #print(datetime_object)
        #print(theFilePath)    
          

        #Create error file    
        print('*****************************************About to call errorLog with runtype = *' + runtype + '*')
        if runtype == '':		
            errorLog.error_log(bulk, job, batches, filename, "ERROR", "SUCCESS","")
        else:    
            errorLog.error_log(bulk, job, batches, filename, "ERROR", "SUCCESS",runtype) 
    
    return

                
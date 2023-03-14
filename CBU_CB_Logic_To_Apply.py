def logic_to_apply(client):

          #The List Of fields we have logic for...
          #theColumnList =	["payout__Line_of_Business__c","payout__X12b1_Batch__c","RecordTypeId","payout__Payor__c","payout__Commission_Batch_Unique_ID__c","payout__Statement_Date__c","payout__Auto_Calc_Total_Gross__c"]
          theColumnList =	["payout__Commission_Batch_Unique_ID__c"]
          listofclients = ["PKS","BCG"]
		  
          for columnname in theColumnList:

            #**************************************************************************************
            #payout__Commission_Batch_Unique_ID__c
            #**************************************************************************************   
            print('payout__Commission_Batch_Unique_ID__c')

            if columnname == 'payout__Commission_Batch_Unique_ID__c':
			
                 if client == 'PKS':

                      temp = row["payout__Data_Source__c"] + " - " + row["payout__Clearing_Firm__c_del"] + " - "
                      if row["payout__Data_Source__c"] == "DTCC":
                           temp = temp + row["payout__Transaction_Date__c_del"] + " - " 

                      else:
                           temp = temp + row["payout__Statement_Date__c"] + " - " 
                 
                      temp = temp + row["payout__Line_of_Business__c"] + " Trail - " + row["payout__Trail__c_del"] 
                      row["payout__Commission_Batch_Unique_ID__c"] = temp				 
				 
				 
				 
                 elif client == 'BCG': 

                      trailstr = ""            
                      temp = row["payout__Data_Source__c"] + " - " + row["payout__Clearing_Firm__c_del"] + " - " + row["payout__Statement_Date__c"] + " - " + row["payout__Line_of_Business__c"]
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
                      if row["payout__Data_Source__c"] == "DTCC":
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

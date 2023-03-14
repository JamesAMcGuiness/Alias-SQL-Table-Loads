def logic_to_apply(client):

          #The List Of fields we have logic for...
          #theColumnList = ["payout__Statement_Advisor__c","payout__Source_Data__c","payout__Unique_Transaction_ID__c","payout__Holding__c","payout__Financial_Account__c","payout__Commission_Batch__c","payout__Data_Source__c","payout__Trail__c","payout__Account_Value__c","payout__Statement_Rep_ID__c","payout__Security_Type__c","TC_Paid_by_Client__c"]
          theColumnList = ["payout__Commission_Batch__c","TC_Paid_by_Client__c"]
          listofclients = ["PKS","BCG"]
		  
          for columnname in theColumnList:

		  
            #************************************
            #payout__Commission_Batch__c
            #************************************
            if columnname == 'payout__Commission_Batch__c':
			
                 if client == 'PKS': 
				 
                      if row["payout__Unique_Transaction_ID__c"] == None or row["payout__Unique_Transaction_ID__c"] == "": 
            
                           temp = row["payout__Data_Source__c"] + " - " + row["payout__Clearing_Firm__c"] + " - "
                           if row["payout__Data_Source__c"] == "DTCC":
                                temp = temp + row["payout__Transaction_Date__c_del"] 

                           else:
                                temp = temp + row["ValuationDate_del"] 
                  
                      temp = temp + " -" + row["LOB_del"] + " Trail - " + row["payout__Trail__c"] 
                      row["payout__Commission_Batch__c"] = temp 
				 
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


                 else:
                      print('ERROR: - The client needs to be setup - ' + client)			
		  

            #*********************************************************
            #TC_Paid_by_Client__c
            #********************************************************* 			
			
            elif columnname == 'TC_Paid_by_Client__c':
			
                 if client == 'PKS': 

                      row["TC_Paid_by_Client__c"] = row["payout__Clearing_Mgt_Fee__c"]     
          				

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)

		  

            #******************************************************************
            #payout__Statement_Advisor__c (Not currently being mapped per Mike)
            #******************************************************************

				 

            #**********************************
            #payout__Source_Data__c
            #**********************************

            elif columnname == 'payout__Source_Data__c':
                 if client in client:
                      row["payout__Source_Data__c"] = "SSN - " + row["ClientSSN_del"] + " FA - " + row["AccountNumber_del"] + " Cusip - " + row["payout__CUSIP_Value__c"] + " RepID - " + row["payout__Statement_Rep_ID__c"]                      

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
                               #        row["payout__Holding__r.payout__FA_CUSIP__c"] = row["AccountNumber_del"] + row["payout__CUSIP_Value__c"] + row["payout__This_Trade_Only_Rep_ID__c"] 						
                               #else:
				
                               #   #print('Missing Holding Run for Investments LOB - Set the Holding lookup')
                               if row["payout__Data_Source__c"] != "NFSC": 					
                                    row["payout__Holding__r.payout__FA_CUSIP__c"] = row["AccountNumber_del"] + row["payout__CUSIP_Value__c"]
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
                           row["payout__Financial_Account__r.payout__Financial_Account_Number__c"] = row["AccountNumber_del"]
                           #print('Successfully assigned the FA lookup')					 


                 else:
                      print('ERROR: - The client needs to be setup - ' + client)				 
			
			

			
            #********************************************************
            #payout__Data_Source__c - Check on if ok to use PKS logic for BCG
            #********************************************************
			
            elif columnname == 'payout__Data_Source__c':

                 if client in listofclients: 

                      row["payout__Source_Data__c"] = "SSN - " + row["ClientSSN_del"] + " FA - " + row["AccountNumber_del"] + " Cusip - " + row["payout__CUSIP_Value__c"] + " RepID - " + row["payout__Statement_Rep_ID__c"]                                                                                        
				 

                 else:
                      print('ERROR: - The client needs to be setup - ' + client)				 
			

			
            #*********************************************************
            # payout__Trail__c
            #********************************************************* 			
			
            elif columnname == 'payout__Trail__c':
                 if client in listofclients:

                      if row["payout__Trail__c"] == "Y":
                           #print('Trail is Y') 
                           row["payout__Trail__c"] = 1
                           #print('Just set Trail to 1')     
                      else:
                           row["payout__Trail__c"] = 0
                 

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
			
			

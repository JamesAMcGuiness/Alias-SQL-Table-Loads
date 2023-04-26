#######################################################################################################################################################################################################################
#Change Log
# 3/8/2023 Initial Version 
#######################################################################################################################################################################################################################
import importlib
import requests as req
import csv
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
import os
import sys
import datetime
from datetime import datetime as dt
import errorLog
#Below are the tables to load from SQL Server
import Customer 
import Contact
import CustomerShipTo
import Billing
import BillingDet
import Order
import OrderDet
import Quote
import QuoteDet
import Order
import LogFile

#Below is the Salesforce Config file for connecting to ORG
import Client_Config

def lambda_handler(key):
    payload = {
        'client_id':     os.environ.get('client_id'),
        'client_secret': os.environ.get('client_secret'),
        'username':      os.environ.get('username'),
        'password':      os.environ.get('password'),
        'grant_type':'password'
    }
    
    #print("My file :" + key)
    
    #Replace any "+" in file name 
    if "+" in key:
        print("file pathname have spaces")
        key = key.replace("+", " ")
        
    
    oauthurl         = os.environ.get('oauthurl')
    salesforceobject = Salesfoceconnect(oauthurl, payload)
    token            = salesforceobject.conn_and_get_token()
    
    print('************************************************')
    print('The connection was successful token is ' + token)
    print('************************************************')


    host           = os.environ['host']
    sessionId      = token
    sandbox        = True
    username       = os.environ.get('username')
    password       = os.environ.get('password')
    security_token = os.environ.get('security_token')
    client_id      = os.environ.get('client_id')
    
    #We want to process Serially if we are running the job to reprocess locked rows, the default is 'Parallel'
    ProcessingMode = 'Parallel'
    runtype = ''
        
    print('***************************************')
    print("Runtype is " + runtype)
    print("Mode is " + ProcessingMode)
    print('***************************************')
    
	# Running for Recordlocks of Commission Transactions
    if runtype == 'RECORDLOCKS' and "COMMISSIONS" in key:
	
        print('***************************************')	
        print('Processing Recordlock for COMM Trans FILES')

        head, filename = os.path.split(key)
        object_name = "payout__Commission_Transactions__c"
        ex_id = "payout__Unique_Transaction_ID__c"

		
        header = "payout__clearing_Short_Name__c,FirstName_del,MiddleName_del,payout__Statement_Rep_ID__c,payout__Clearing_SSN__c,payout__Clearing_FA_number__c,payout__Description__c,payout__Clearing_Cum_Discount__c,payout__Data_Source__c,Shares_del,payout__Price__c,ValuationDate_del,payout__Clearing_Asset_Name__c,Payout__Clearing_Security_Type__c,payout__Clearing_Firm__c,payout__Clearing_Symbol__c,payout__CUSIP_Value__c,payout__Trade_Date__c,payout__Transaction_Date__c,payout__Transaction_Amount__c,payout__Clearing_Transaction_Type__c,payout__Units__c,payout__Clearing_Dist_Indicator__c,payout__Clearing_Percent_Sales_Charge__c,payout__Clearing_Dealer_Commission_Code__c,payout__Gross_Commission__c,payout__Clearing_Underwriter_Commission__c,payout__Tran_Type__c,payout__Unique_Transaction_ID__c,payout__Clearing_Distribution_Amount__c,RecordType_del,payout__Dealer__c,payout__Clearing_Branch_Prefix__c,payout__Dealer__c,payout__Source_Code__c,payout__Clearing_Trade_Ref_Number__c,payout__Clearing_Charge__c,payout__Clearing_Mgt_Fee__c,payout__Clearing_MF_Type__c,payout__Clearing_Blotter_Code__c,payout__Clearing_Market_Code__c,payout__Clearing_Product_Code__c,payout__Clearing_Offset_Account__c,payout__Clearing_Product_Type__c,payout__Clearing_Execution_Count__c,payout__Clearing_Misc_Fee__c,payout__Albridge_Key__c,payout__Clearing_Addl_Fee_Code__c,payout__Clearing_Addl_Fee__c,payout__Clearing_Commission_Discount__c,payout__Clearing_Fee_Code__c,payout__Clearing_Trade_Cancel_Indicator__c,payout__Clearing_Fee__c,payout__Clearing_Execution_Fee__c,payout__Clearing_Postage__c,payout__Clearing_Notes__c,payout__Clearing_Purchase_Type__c,payout__Clearing_Dist_Indicator__c,payout__Clearing_Split_Percentage__c,payout__Trail_Frequency__c,payout__Clearing_Order_Type__c,payout__Clearing_Order_Ref_Number__c,LOB_del,payout__Trail__c,payout__This_Trade_Only_Rep_ID__c,payout__Clearing_Total_Units__c,payout__Solicited__c,payout__Clearing_State__c,FACUSIP_del,payout__Clearing_Net__c,DLR_Clearance_Fee_del,payout__Clearing_Primary_Exchange__c,CRDNbr_del,payout__Distributor_Agent_ID__c,payout__National_Producer_Number__c,payout__Carrier_Assigned_Agent_ID__c,payout__NSCC_Settle_Amount__c,payout__Clearing_Foreign_Surcharge__c,payout__Clearing_Foreign_Code__c,payout__Clearing_Concession_Code__c,payout__Clearing_Reg_Rep__c,payout__Statement_Rep_Name__c,payout__Commission_Batch__r.payout__Commission_Batch_Unique_ID__c,payout__Filename__c,payout__DistAcctID__c,payout__DistTransID__c,payout__Security_Type__c".split(",")          

        CBU_CommTrans.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])	
	
    elif "LogFile" in key: 
        print('***************************************')    
        print('Running for LogFile....')

        head, filename = os.path.split(key)
        object_name = "Data_Loads_Stats__c"
        ex_id = "Id"
        
        header = "Client_del,Load_Date__c,Load_File__c,Rows_Processed__c,Total_Errors__c,RecordLocks_del,Error_Rate__c,Warning_Flag__c,JobID__c".split(",")
        
        LogFile.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])
	

    # Running for Customer
    elif "Customer" in key: 
        print('***************************************')    
        print('Running for Customer....')
        print('***************************************')    

        head, filename = os.path.split(key)
        object_name = "Account"
        ex_id = "E2_Customer_Key__c"
        
        header = "CSV_Row_Num_del,Ship_To_Contact__c,BillingStreet,SecondLineOfStreet_del,BillingCity,BillingState,BillingPostalCode,Phone,Website,Name,Customer_Code__c,E2_Customer_Key__c,PreviousModDate_del,RowNum_Of_Source_File_del,Loaded_From_Python_Process__c,LoadDate_del,Source_File_del,LoadForCompany_del".split(",")          
        
        Customer.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])
        
    # Running for CompanyShipTo        
    elif "ShipTo" in key: 
        print('***************************************')    
        print('Running for CompanyShipTo....')

        head, filename = os.path.split(key)
        object_name = "Account"
        ex_id = "E2_Customer_Key__c"
        
        header = "CSV_Row_Num_del,ShippingStreet,ShipAddr2_del,ShippingCity,ShippingState,ShippingPostalCode,Ship_To_Contact__c,Shipping_Address_Company_Name__c,E2_Ship_To_Key__c,E2_Customer_Key__c,LastModDate_del,RowNum_Of_Source_File_del,Loaded_From_Python_Process__c,LoadDate_del,Source_File_del".split(",")          
        
        CustomerShipTo.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])

    # Running for Contacts        
    elif "Contact" in key: 
        print('***************************************')    
        print('Running for Contact....')

        head, filename = os.path.split(key)
        object_name = "Contact"
        ex_id = "E2_Contact_ID__c"
        
        header = "CSV_Row_Num_del,Contact_del,FirstName,LastName,Custom_Email__c,Phone,Title,MobilePhone,E2_Contact_ID__c,LastModDate_del,RowNum_Of_Source_File_del,Loaded_From_Python_Process__c,LoadDate_del,Source_File_del,Account.E2_Customer_Key__c,LoadForCompany_del".split(",")          
        
        Contact.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])


    # Running for OrderDet (Work Order Line Item)
    elif "OrderDet" in key: 
        print('***************************************')    
        print('Running for OrderDet....')

        head, filename = os.path.split(key)
        object_name = "WorkOrderLineItem"
        ex_id = "OrderDetId__c"
        
        header = "CSV_Row_Num_del,Quantity_del,UnitPrice_del,Description,Revision_del,JobNumber_del,Status,Quote Numbner_del,OrderDetID__c,WorkOrder.Order_Number__c,AssetID_del,PricebookEntryID_del,Product_WorkCode__r.Work_Code__c,RowNum_Of_Source_File_del,Loaded_From_Python_Process__c,LoadDate_del,Source_File_del,LastModDate_del".split(",")          
        
        OrderDet.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])

# Running for Order (Work Order)
    elif "Order" in key: 
        print('***************************************')    
        print('Running for Order....')

        head, filename = os.path.split(key)
        object_name = "WorkOrder"
        ex_id = "Order_Number__c"
        
        header = "CSV_Row_Num_del,Sold_To_Account__c,Ship_To_Account__c,Street,Street_del,City,State,PostalCode,Order_Number__c,Order_Date__c,AccountName__c_del,P_O_Number__c,Ship_Via__c,TermCode_Del,FOB__c_del,AccountName__r.E2_Customer_Key__c,E2_Customer_Key__c,Quote_Number__c,RowNum_Of_Source_File_del,Loaded_From_Python_Process__c,LoadDate_del,Source_File_del,LastModDate_del".split(",")          
        
        Order.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id,'Serial',runtype,os.environ["ClientName"])
    

    elif "BillingDet" in key: 
        print('***************************************')    
        print('Running for Billing Detail....')

        head, filename = os.path.split(key)
        object_name = "BillingDet__c"
        ex_id = "BillingDet_ID__c"
        header = "CSV_Row_Num_del,Quantity__c,Unit_Price__c,Amount__c,Invoice__r.Invoice_Number__c,Part_Description__c,Part_Number__c,Packing_List_Number__c,Revision__c,P_O_Number__c,BillingDet_ID__c,RowNum_Of_Source_File_del,Loaded_From_Python_Process__c,LoadDate_del,Source_File_del,LastModDate_del".split(",")          
        
        BillingDet.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])


    elif "Billing" in key: 
        print('***************************************')    
        print('Running for Billing....')

        head, filename = os.path.split(key)
        object_name = "Billing__c"
        ex_id = "E2_Invoice__c"
        
        header = "CSV_Row_Num_del,Sold_To__c,Ship_To__c,Street__c,Street_2__c_del,City__c,State__c,Zip_Postal_Code__c,Invoice_Number__c,Invoice_Date__c,Name,Work_Code__c,TermsCode_del,SubTotal_del,Sales_Tax__c,Shipping_Charges__c,Invoice_Total__c,Paid_to_Date__c,BalanceDue_del,E2_Customer_Key__c,Account__r.E2_Customer_Key__c,Account_Name__c,DateEnt_del,Invoice_Status__c,Quote_ID__c_del,E2_Invoice__c,RowNum_Of_Source_File_del,Loaded_From_Python_Process__c,LoadDate_del,Source_File_del,LastModDate_del".split(",")          
        
        Billing.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])


    # Running for QuoteLine        
    elif "QuoteDet" in key: 
        print('***************************************')    
        print('Running for Quote Detail....')

        head, filename = os.path.split(key)
        object_name = "QuoteDet__c"
        ex_id = "QuoteDet_ID__c"
        
        header = "CSV_Row_Num_del,Item_Number__c,Part_Number_Description__c,Quantity__c,Quantity_2__c,Quantity_3__c,Quantity_4__c,Quantity_5__c,Quantity_6__c,Quantity_7__c,Quantity_8__c,Price__c,Price_2__c,Price_3__c,Price_4__c,Price_5__c,Price_6__c,Price_7__c,Price_8__c,Job_Number__c,Job_Notes__c,Quote_Number__c,Status__c,Name,QuoteDet_ID__c,Quote_Header__r.E2_Quote_Key__c,Product2Id_del,RowNum_Of_Source_File_del,Loaded_From_Python_Process__c,LoadDate_del,Source_File_del,LastModDate_del,loadforcompany_del".split(",")
        
        QuoteDet.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])

    # Running for Quote        
    elif "Quote" in key: 
        print('***************************************')    
        print('Running for Quote....')

        head, filename = os.path.split(key)
        object_name = "Opportunity"
        ex_id = "E2_Quote_Key__c"
        
        header = "CSV_Row_Num_del,Quote_TO__c,Custom_Street__c,Custom_Street_2__c_del,Custom_City__c,Custom_State__c,Custom_Zip__c,Quote_Number__c,Quote_Date__c,Account.E2_Customer_Key__c,Quote_Entered_By__c,Ship_Via__c,Contact_Name__c,Inquiry__c,TermsCode_del,Phone__c,FAX__c,Amount_del,E2_Quote_Key__c,RecordTypeId,Name,CloseDate,RowNum_Of_Source_File_del,Loaded_From_Python_Process__c,LoadDate_del,Source_File_del,LastModDate_del,LoadForCompany_del".split(",")
        
        Quote.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])

    # Running for Logfile
    

    else:
        print(" ............................................. ")
        print(" File name is not as expected................. ", key)
        print(" ............................................. ")




def create_touch_file():
	thedate    = datetime.datetime.now()
	theyear    = thedate.year
	themonth   = thedate.month
	theday     = thedate.day 
	thedatestr = str(theday) + "/" + str(themonth) + "/" + str(theyear)
	
	theFilePath = os.environ.get("op_path") + '\Trigger.txt'
					
	#fields = ['Date']
	row=[thedatestr]
	with open(theFilePath,'w') as csvFile:
		writer = csv.writer(csvFile)
		#writer.writerow(fields)
		writer.writerow(row)
			
		print('==========================================>Created a touch file')
            
		csvFile.close()  


						
#Not being implemented yet, for fututre release
def process_RECORDLOCKS_file():

        print('===================================================================================================>************ In process_RECORDLOCKS_file ************')
        source      = os.environ.get("op_path")
        destination = os.environ.get("ip_path")


        #Try 5 times to reprorcess the record locks
        for x in range(1):
                file_lst = os.listdir(source)
            
                os.chdir(source)
                for file in file_lst:
                    print('***************************************')    				
                    print('======================================================================================>Checking this file in output folder ' + file + ' Run ct = ' + str(x))


                    if "RECORDLOCKS" in file:
                        if "ERROR" not in file:
                            try:
                                print("==========================================================================>Found RECORDLOCK File to move from output to input - " + file)
                                #print("Source to move " + source)
                                #print("Destination to move " + destination)
                    
                    
                                #os.rename(source + "/" + file, destination + "/" + file)
                                
                                #Move from Output folder to Input folder
                                os.rename(source + "/" + file, destination + "/" + file)
                                #print('Move was successful!')
                    
                    
                                file_lst = os.listdir(destination)
                                os.chdir(destination)
                                for fileinput in file_lst:  
                                    print('========================================================================>***************************************')                                                                                                                                    
                                    print('========================================================================>****************File(s) now in Input folder ' + fileinput)
                      
                                print('Calling code to re-process the record locks - passing in this file to re-process: ' + file)

                                #print("============================================================================>found the Recordlocking file in the input folder and was named correctly", objtype, file)
                                f = open(file, encoding="latin-1")
                                reader = csv.reader(f)
                                lines = len(list(csv.reader(f)))
                                f.close()
                                if lines == 1:
                                     print("Recordlocks file is empty.")				
                                     #Move from Input folder
                                     os.remove(destination + "/" + file)
                                     errorLog.error_bat_job(file, "input file does not have any records")
                                else:     

                                     lambda_handler(file)   
                    
                                     #Move from Input folder
                                     os.remove(destination + "/" + file)
                                     #print('Deleted from Input folder Move was successful!')
                    
                            except FileExistsError:
                                print("file already exists")
                                print("removing already exists file")
                                os.remove(destination + "/" + file)
                                print("copying ip file")
                                os.rename(source + "/" + file, destination + "/" + file)
                            
                            
                


							
							
							
							
def error_bat_job(filename,errmsg):
    #source      = os.environ.get("ip_path")
    #destination = os.environ.get("op_path")
    print("In error_bat_job")
    errorfile =  os.environ.get("op_path") + "\\" + filename + "_BAT_JOB_ERRORS.txt"   
    print(errorfile)
    error  = open(errorfile, "w", encoding="utf-8-sig")
    error.write("Error," + errmsg);
    error.close()
    #os.rename(source + "/" + filename, destination + "/" + filename)

def post_batch_salesforce(disbursals, bulk, job):
    csv_iter = CsvDictsAdapter(iter(disbursals))
    batch = bulk.post_batch(job, csv_iter)
    # bulk.wait_for_batch(job, batch)
    print('***************************************')    	
    print("Done. Batch Uploaded.")
    
    return batch

class Salesfoceconnect:
    def __init__(self, login_url, payload):
        self.login_url = login_url
        self.payload   = payload
        self.session   = req.Session()




    def conn_and_get_token(self):
        # print(self.login_url, self.payload)

        res = self.session.post(self.login_url, data=self.payload)
        op_json = res.json()
        #print(op_json)
        try:
            token = op_json['access_token']
        except KeyError as e:
            print("Token Not received system exiting.................. ", e)
            sys.exit(0)
        # return False
        return token



try:
    runtype = ''
    if __name__ == "__main__":
	
        #args = sys.argv[1:]
        #print(args)

        #Below is example of how to IMPORT python modules dynamically from a passed in parameter value 		
        #This will import the correct Configuration for the Client passed in from the .bat file 		
        #config = importlib.import_module(args[2])
        #print('Config file to use is : ')
        #print(config)
        #print('*********************************')
        #config.set_env_var()
		
        Client_Config.set_env_var()
		
        source      = os.environ.get("ip_path")
        destination = os.environ.get("op_path")

        #file_lst will contain all the CSVs to be loaded
        file_lst = os.listdir(source)
                
        #print(file_lst)        
        
        #List the files in the desired load order
        SQLFiles = ['Customer.csv','Contact.csv','ShipTo.csv','Order.csv','OrderDet.csv','Billing.csv','BillingDet.csv','Quote.csv','QuoteDet.csv']

        #For each SQLFile defined, look for that file in the input folder
        for objtype in SQLFiles:
            
             os.chdir(source)
             
             for file in file_lst:
                 
                 #If the file found is the 
                  if objtype in file:
                      print(' ')
                      print('***************************************')
                      print('Found a file to process..' + str(file))
                      print('***************************************')
                      print(' ')
                      f      = open(file, encoding="latin-1")
                      reader = csv.reader(f)
                      lines  = len(list(csv.reader(f)))
                      f.close()
                      
                      if lines == 1:
                           print('***************************************')
                           print("input file does not have any records.")
                           print('***************************************')	
                           print(' ')
                           
                           
                           try:
                                os.rename(source + "/" + file, destination + "/" + file)
                                
                           except FileExistsError:
                                
                                os.remove(destination + "/" + file)
                                
                                try:
                                    #print('We are finally trying to mover this ' + source + ' filename = ' + file + ' to the Output folder.')
                                    os.rename(source + "/" + file, destination + "/" + file)
                                    
                                except Exception as e:
                                    print(str(e))
                           
                           
                           
                           
                           
                           print('Moved empty file to output folder.')
                           print(' ')
                           #errorLog.error_bat_job(file, "input file does not have any records")
                      else:

                           lambda_handler(file)
            
                           try:
                                #print('***************************************')    			
                                #print("File to move" + file)
                                #print("Source to move" + source)
                                #print("Destination to move" + destination)
                                #print('***************************************')    
                                              
                                os.rename(source + "/" + file, destination + "/" + file)
                                
                           except FileExistsError:
                                print('***************************************')    			
                                print("file already exists")
                                print("removing already exists file")
                                print('***************************************')    
                                
                                os.remove(destination + "/" + file)
                                print("copying ip file")
                                os.rename(source + "/" + file, destination + "/" + file)
				
                           print('*****************************************')    				
                           print('SUCCESSFULLY Finshed processing the file(s) ')
                           print('*****************************************')
                           print(' ')    				
                           print(' ')    				
                           print(' ')    				
                           
                               				     
                           
                                
                           print('*****************************************')    				
                           print('SUCCESSFULLY Finshed loading Run Stats.  ')
                           print('*****************************************')
                           print(' ')    				
                           print(' ')    				
                           print(' ')    				              
                           
        #Load all LogFiles to Salesforce
        print('Now move all LogFile(s) to the Input folder.')
        source = os.environ.get("op_path") + '\\logs'
        os.chdir(source)
                                    
        #file_lst will contain all the LogFile CSVs to be loaded
        file_lst = os.listdir(source)
        #print(file_lst)         
        for file in file_lst:
               lambda_handler(file)
               os.remove(file)
        file_lst = os.listdir(source)     
        for file in file_lst:
               os.remove(file)
               
        #Remove All LogFile files from the Output folder
        source = os.environ.get("op_path")

        file_lst = os.listdir(source)
        #print(file_lst)         
        for file in file_lst:
            if "LogFile" in file:
                #print('Remove ' + file )   
                os.remove(source + "/" + file)

                    
               
                           
    else:
        
        pass
        
        
        
except Exception as e:
    source      = os.environ.get("ip_path")
    destination = os.environ.get("op_path")
    print(' ')
    print("Error has occurred, captured in Driver.py")
    print(' ')
    print(str(e))
    
    file_lst = os.listdir(source)
            
    os.chdir(source)
    for file in file_lst:
        try:
            os.rename(source + "/" + file, destination + "/" + file)
            error_bat_job(file,str(e))

        except FileExistsError:
            
            os.remove(destination + "/" + file)
            
            try:
                #print('We are finally trying to mover this ' + source + ' filename = ' + file + ' to the Output folder.')
                os.rename(source + "/" + file, destination + "/" + file)
                
            except Exception as e:
                print(str(e))
                


                
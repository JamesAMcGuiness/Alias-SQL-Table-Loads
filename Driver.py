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
import Company 
import CompanyShipTo
import Contacts
import VendorInv
import VendorInvDet
import Orders
import WorkCode
import OrderDet
import Quote
import QuoteDet
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
        
    
    oathurl          = os.environ.get('oathurl')
    salesforceobject = Salesfoceconnect(oathurl, payload)
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
	
	

    # Running for Company
    elif "Company" in key: 
        print('***************************************')    
        print('Running for COMPANY....')

        head, filename = os.path.split(key)
        object_name = "Acount"
        ex_id = "AccountID"
        
        header = "CompanyCode__c,Addr1__c,Addr2__c,City__c,State__c,ZIPCode__c,Phone__c,Website__c".split(",")          
        
        Company.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])
        
    # Running for CompanyShipTo        
    elif "CompanyShipTo" in key: 
        print('***************************************')    
        print('Running for CompanyShipTo....')

        head, filename = os.path.split(key)
        object_name = "CompanyShipTo__c"
        ex_id = "CompanyShipTo_ID"
        
        header = "Addr1__c,Addr2__c,City__c,State__c,ZIPCode__c,CompanyShipTo_ID__c".split(",")          
        
        CompanyShipTo.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])

    # Running for Contacts        
    elif "Contacts" in key: 
        print('***************************************')    
        print('Running for CompanyShipTo....')

        head, filename = os.path.split(key)
        object_name = "CompanyShipTo__c"
        ex_id = "CompanyShipTo_ID"
        
        header = "Contact__c,Title__c,Phone__c,EMail__c,Cell_Phone__c,Contacts_ID__c".split(",")          
        
        Contacts.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])

    # Running for Quote        
    elif "Quote" in key: 
        print('***************************************')    
        print('Running for Quote....')

        head, filename = os.path.split(key)
        object_name = "Opportunity__c"
        ex_id = "???"
        
        header = "Addr1__c ,Addr2__c ,City__c,St__c,Zip__c,SalesID__c,CustCode__c,ShipVia__c,Quote_ID__c,ContactName__c,InqNum__c,TermsCode__c,Phone__c,FAX__c,DateEnt__c,Location__c".split(",")          
        
        Quote.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])

    # Running for QuoteLine        
    elif "QuoteLine" in key: 
        print('***************************************')    
        print('Running for Quote....')

        head, filename = os.path.split(key)
        object_name = "QuoteDet__c"
        ex_id = "???"
        
        header = "QuoteNo__c,PartNo__c,ItemNo__c,Qty1__c,Price1__c,Qty2__c,Price2__c,Qty3__c,Price3__c,Qty4__c,Price4__c,Qty5__c,Price5__c,Qty6__c,Price6__c,Qty7,Price7__c,Qty8__c,Price8__c,Descrip__c,QuoteDet_ID__c".split(",")          
        
        QuoteLine.salesforce_connect_and_upload(filename, host, sessionId, sandbox, username, password, security_token,
        client_id, key, object_name, header, ex_id, ProcessingMode,runtype,os.environ["ClientName"])

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


def process_Missing_FA_file():
		print('In process_Missing_FA_file....')
		destination      = os.environ.get("ip_path")
		source           = os.environ.get("op_path")
		backup           = os.environ.get("op_path") + "/Backup"
		#print(source)
		#print(destination)
		#print(os.listdir(source))
		file_lst = os.listdir(source)
                
		#print(file_lst)        
                
		os.chdir(destination)
		for file in file_lst:
			
                        
			if "MISSING_FA" in file:
				if "ERROR" not in file and "RECORDLOCKS" not in file and "_BATCH" not in file:
					print('***************************************')    				
					print('We Found a Missing FA file to process..')
			
					try:
						#print("Found MISSING FA File to move " + file)
						#print("Source to move " + source)
						#print("Destination to move " + destination)
                    
                    
						#os.rename(source + "/" + file, destination + "/" + file)
                                
						#Move from Output folder to Input folder
						os.rename(source + "/" + file, destination + "/" + file)
						#print('Move of Missing FA was successful!')            
						
						lambda_handler(file)   
                    
						#Move from Input folder
						os.remove(destination + "/" + file)
						#os.rename(destination + "/" + file, backup + "/" + file)						
						
						print('Move to output folder Move was successful!')

					except FileExistsError:
						print("file already exists in Input folder")
						print("removing already exists file")
						os.remove(source + "/" + file)
						print("copying ip file")
						os.rename(source + "/" + file, destination + "/" + file)
					return                             
                            

						

def process_Missing_HLD_file():
		print('***************************************')    
		print('In process_Missing_HLD_file....')
		
		destination      = os.environ.get("ip_path")
		source           = os.environ.get("op_path")
		backup           = os.environ.get("op_path") + "/Backup"
		#print(source)
		#print(destination)
		#print(os.listdir(source))
		file_lst = os.listdir(source)
                
		#print(file_lst)        
                
		os.chdir(destination)
		for file in file_lst:
			
                        
			if "MISSING_HOLDING" in file:
				if "ERROR" not in file and "RECORDLOCKS" not in file and "_BATCH" not in file:
					print('***************************************')    				
					print('Found a MISSING HOLDING file to process..')
			
					try:
						#print("Found HLD File to move " + file)
						#print("Source to move " + source)
						#print("Destination to move " + destination)
                    
                    
						#os.rename(source + "/" + file, destination + "/" + file)
                                
						#Move from Output folder to Input folder
						os.rename(source + "/" + file, destination + "/" + file)
						#print('Move of Missing FA was successful!')            
						
						
						
						print("found the Holding file in the input folder and was named correctly", objtype, file)
						f = open(file, encoding="latin-1")
						reader = csv.reader(f)
						lines = len(list(csv.reader(f)))
						f.close()
						if lines == 1:
							print("Holding Input file is empty.")				
							 #Move from Input folder
							os.remove(destination + "/" + file)
							errorLog.error_bat_job(file, "input file does not have any records")
							return

						lambda_handler(file)   

						
						#Move from Input folder
						os.remove(destination + "/" + file)
						#os.rename(destination + "/" + file, backup + "/" + file)						
																		
						#print('Move from Input folder Move was successful!')

					except FileExistsError:
						print("file already exists in Input folder")
						print("removing already exists file")
						os.remove(source + "/" + file)
						print("copying ip file")
						os.rename(source + "/" + file, destination + "/" + file)
					return 						
						
						
						
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
        print(op_json)
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

        #List all the possible CSV's 
        input_key_list = ['_COMMISSIONBATCHES','_COMMISSIONS']
        
        #List the files in the desired load order
        SQLFiles = ['Company.csv','CompanyShipTo.csv','Contacts.csv','VendorInv.csv','VendorInvDet.csv','Orders.csv','WorkCode.csv','OrderDet.csv','Quote.csv','QuoteDet.csv']

        #For each SQLFile defined, look for that file in the input folder
        for objtype in SQLFiles:
            
             os.chdir(source)
             
             for file in file_lst:
                 
                 #If the file found is the 
                  if objtype in file:
                      print('Found a file to process..' + str(file))
                      
                      f = open(file, encoding="latin-1")
                      
                      reader = csv.reader(f)
                      lines = len(list(csv.reader(f)))
                      f.close()
                      
                      if lines == 1:
                           print("input file does not have any records.")	
                           			
                           
                           os.remove(destination + "/" + file)
                           errorLog.error_bat_job(file, "input file does not have any records")
                      else:

                           lambda_handler(file)
                        
            
                           try:
                                print('***************************************')    			
                                print("File to move" + file)
                                print("Source to move" + source)
                                print("Destination to move" + destination)
                                print('***************************************')    
                                              
                                os.rename(source + "/" + file, destination + "/" + file)
                
                           except FileExistsError:
                                print('***************************************')    			
                                print("file already exists")
                                print("removing already exists file")
                                print('***************************************')    
                                os.remove(destination + "/" + file)
                                print("copying ip file")
                                os.rename(source + "/" + file, destination + "/" + file)
				
                           print('***************************************')    				
                           print('Finshed processing the files ' + 'runtype = *' + runtype + '*')     


    else:
        pass
        
except Exception as e:
    source      = os.environ.get("ip_path")
    destination = os.environ.get("op_path")
    
    print("Error has occurred, captured in Driver.py")
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
                
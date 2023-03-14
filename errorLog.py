import os
import datetime
#Change Log

			  
def error_log(bulk, job, batches, filename, errorprefix, successprefix,runtype,filetype="utf-8-sig"):

    #Error Reporting Vars
    reclockct = 0
    missfact  = 0
    misshldct = 0
    toterrorct = 0                         
    
    rowsProcessed = 0
    errorCt = 0
    client = os.environ['ClientName']

    c_time = datetime.datetime.now().strftime("%y-%m-%d-%H-%M-%S-")
    errorfile      = errorprefix   + '_' + os.path.basename(filename)
    logfile        = 'LogFile' + '_' + os.path.basename(filename)
    
    lockerrorsfile = 'RECORDLOCKS' + '_' + os.path.basename(filename)
    MissingFAFile  = 'FILE_OF_MISSING_FA' + '_' + os.path.basename(filename)
    MissingHoldingFile  = 'FILE_OF_MISSING_HOLDINGS' + '_' + os.path.basename(filename)
	
    logfile        = os.environ['op_path'] + '\\' + 'logs' + '\\' + logfile    
    errorfile      = os.environ['op_path'] + '\\' + errorfile 
    lockerrorsfile = os.environ['op_path'] + '\\' + lockerrorsfile
    MissingFAFile  = os.environ['op_path'] + '\\' + MissingFAFile
    MissingHoldingFile  = os.environ['op_path'] + '\\' + MissingHoldingFile

    error       = open(errorfile, "w", encoding=filetype)
    errorLock   = open(lockerrorsfile, "w", encoding=filetype)
		
    if runtype == '':
        print('***************************************runtype is NORMAL')	
        errorFA     = open(MissingFAFile, "w", encoding=filetype)
        errorHLD    = open(MissingHoldingFile, "w", encoding=filetype)
        lf          = open(logfile, "w", encoding=filetype)
    else:	
        print('Runtype is not NORMAL it is: ' + runtype)
		
    with open(filename, encoding=filetype) as source:
        header = source.readline();
                
        error.write("Error," + header);
        errorLock.write(header);  
		
        if runtype == '':		
            errorFA.write(header);
            errorHLD.write(header);
        
		
        print('===================================================================> in errorLog.py')
    
        
        for batch in batches:
    
            print('We have a batch to process.')
            for uploadresult in bulk.get_batch_results(batch, job):
            
                    #print('Job was found!')
                    line = source.readline();
                    rowsProcessed = rowsProcessed + 1    
                    #print(line)
                    
                    if uploadresult.success == "false":
                        errorCt = errorCt + 1
                        #print("Success")
                        #success.write(uploadresult.id + "," + line)
                        #print("failure")
                        #print(uploadresult.error)
                        toterrorct = toterrorct + 1                         
                        if "UNABLE_TO_LOCK" in uploadresult.error or "TooManyLockFailure" in uploadresult.error:
                            #print('Error lock was found!')
                            #haveLocks = true
                            errorLock.write(line)
                            reclockct = reclockct + 1
							
                        if "not found for field payout__Financial_Account_Number__c" in uploadresult.error and runtype =='':
                            print('runtype was null, and no FA found, write to FA Error File')
                            #haveLocks = true
                            errorFA.write(line)
                            missfact  = missfact   + 1
                            print('===================================================>Total Missing FA is ' + str(missfact))
                            print('===================================================>Total Missing HLD is ' + str(misshldct))
						
							
                        if "not found for field payout__FA_Cusip__c" in uploadresult.error and runtype =='':
                            print('runtype was null, and no Holding found, write to HLD Error File')
                            #haveLocks = true
                            errorHLD.write(line)
                            misshldct  = misshldct   + 1
                            print('===================================================>Total Missing FA is ' + str(missfact))
                            print('===================================================>Total Missing HLD is ' + str(misshldct))
						
                        error.write(uploadresult.error + "," + line)
                    
    
		
        
		
        #if runtype == '':
        #    thestr = 'Total Errors=' + str(toterrorct) + '   Errors of type RecordlockLocks=' + str(reclockct) + '   Errors of type Missing FA=' + str(missfact) + '     Errors of type Missing HLD=' + str(misshldct) 
        #    lf.write(thestr);
		
    
    error.close()
    errorLock.close()

    print('Calling create_Error_Log_File')
    create_Error_Log_File(client,filename,c_time,errorfile,rowsProcessed,errorCt,reclockct,job,filetype)
    if runtype == '':	
        errorFA.close()
        errorHLD.close()   
        #lf.close()    

def create_Error_Log_File(client,fileName,runTime,errorFile,rowsProcessed,totalErrors,reclockct,job,filetype="utf-8-sig"):
    print('In create_Error_Log_File')
	#print(client,fileName + "," + str(runTime) + "," + errorFile + "," + str(rowsProcessed) + "," + str(totalErrors) + "," + str(myErrorRate) + "," + myWarningFlag)
    logfile = 'LogFile' + '_' + os.path.basename(fileName)
    header = "Client,LoadDate,ErrorFile,RowsProcessed,TotalErrors,Recordlocks,ErrorRate,WarningFlag,Job"
    
    myErrorRate = 0
    myWarningFlag = "N"
    if rowsProcessed > 0:
        myErrorRate = (totalErrors / rowsProcessed) * 100
        if myErrorRate >= 5:
            myWarningFlag = "Y"


    errlog = open(logfile, "w", encoding=filetype)
    errlog.write(header + "\n");
    errlog.write(client + ","  + str(runTime) + "," + errorFile + "," + str(rowsProcessed) + "," + str(totalErrors) + "," + str(reclockct) + "," + str(myErrorRate) + "," + myWarningFlag + "," + str(job))
    errlog.close()
    

	
	

def error_bat_job(filename,errmsg,filetype="utf-8-sig"):
    errorfile = os.environ.get("op_path") + "\\" + filename + "_BAT_JOB_ERRORS"   
    #print(errorfile)
    error  = open(errorfile, "w", encoding=filetype)
    error.write("Error," + errmsg);
    error.close()



    
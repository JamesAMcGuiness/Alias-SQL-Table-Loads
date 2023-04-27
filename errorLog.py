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

    c_time         = datetime.datetime.now().strftime("%y-%m-%d-%H-%M-%S-")
    errorfile      = c_time + '_' + errorprefix   + '_' + os.path.basename(filename)
    logfile        = c_time + '_' + 'LogFile' + '_' + os.path.basename(filename)
    
    lockerrorsfile = c_time + '_' + 'RECORDLOCKS' + '_' + os.path.basename(filename)
    
	
    logfile        = os.environ['op_path'] + '\\' + 'logs' + '\\' + logfile    
    errorfile      = os.environ['op_path'] + '\\' + errorfile 
    lockerrorsfile = os.environ['op_path'] + '\\' + lockerrorsfile
    

    error       = open(errorfile, "w", encoding=filetype)
    errorLock   = open(lockerrorsfile, "w", encoding=filetype)
		
    if runtype == '':
        print('***************************************runtype is NORMAL')	
        
        lf          = open(logfile, "w", encoding=filetype)
    else:	
        print('Runtype is not NORMAL it is: ' + runtype)
		
    with open(filename, encoding="utf-8-sig") as source:
        header = source.readline();
        #temp = header.replace("\u2022","")  
        #print(temp)        
        error.write("Error," + header);
        errorLock.write(header);  
			
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
                        
                        temp1 = uploadresult.error.replace(","," ")
                        temp2 = temp1.replace("\u2022","")
                            
                        error.write(temp2 + "," + line)
                    
    
		
        
		
        #if runtype == '':
        #    thestr = 'Total Errors=' + str(toterrorct) + '   Errors of type RecordlockLocks=' + str(reclockct) + '   Errors of type Missing FA=' + str(missfact) + '     Errors of type Missing HLD=' + str(misshldct) 
        #    lf.write(thestr);
		
    
    error.close()
    errorLock.close()

    print('Calling create_Error_Log_File')
    create_Error_Log_File(client,logfile,c_time,errorfile,rowsProcessed,errorCt,reclockct,job,filetype)
    

def create_Error_Log_File(client,logfile,runTime,errorFile,rowsProcessed,totalErrors,reclockct,job,filetype="utf-8-sig"):
    print('In create_Error_Log_File')
	#print(client,fileName + "," + str(runTime) + "," + errorFile + "," + str(rowsProcessed) + "," + str(totalErrors) + "," + str(myErrorRate) + "," + myWarningFlag)
    
    
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



    
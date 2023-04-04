import os

#Change Log
#3/8/2023 - Initial 

def set_env_var():

    #Set to where the Python source code will reside
    os.environ['base_SF_BULK_DIR'] = "c:\\MyProjects\Alias-SQL-Table-Loads"           
			
    #Always used for all configurations (No changes necessary!)
    os.environ['ip_path']          = os.environ['base_SF_BULK_DIR'] + "\\Input"
    os.environ['op_path']          = os.environ['base_SF_BULK_DIR'] + "\\Output"
    os.environ['ClientName']       = "DesertPowder"

    # Desert PROD
    os.environ['oauthurl']            = "https://desertcoatingsolutions.my.salesforce.com/services/oauth2/token"		
    os.environ['client_id']           = "3MVG9ux34Ig8G5epuXWEQpQ7Gz_zuuv2Soyr2ZwaDScXJyqC1EqxbHYqUZfZ7Ftgstaq_G0gfHorcViPUeX1a"
    os.environ['client_secret']       = "A10B5FBDBB8FF0B968BA8B44C32267F45477F3B23EA738B941D83038759E3476"
    os.environ['username']            = "aliasadmin@desertpowder.com"
    os.environ['password']            = "Welcome2Alias"
    os.environ['host']                = "https://desertcoatingsolutions.my.salesforce.com/"
    os.environ['security_token']      = ""	
    os.environ['DesertRTID']          = "012Dn000000F74NIAS"	
    os.environ['StandardRTID']        = "012Dn000000F74SIAS"	
    os.environ['DesertContactRTID']   = "012Dn000000F74XIAS"	
    os.environ['StandardContactRTID'] = "012Dn000000F74TIAS"	
    os.environ['DesertOppRTID']       = "012Dn000000F74cIAC"	
    os.environ['StandardOppRTID']     = "012Dn000000F74JIAS"	
    
    
                
    #print('Config variables...')
    #print('****************************************')
    #print(os.environ['username'])
    #print(os.environ['ClientName'])
    #print(os.environ['ip_path'])
    #print(os.environ['op_path'])
    #print('os variables successfully set!')
    #print('****************************************')	
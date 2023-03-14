import os

#Change Log
#3/8/2023 - Initial 

def set_env_var():

    #Set to where the Python source code will reside
    os.environ['base_SF_BULK_DIR'] = "c:\\Desert_E2_Dataloads"                      
			
    #Always used for all configurations (No changes necessary!)
    os.environ['ip_path']          = os.environ['base_SF_BULK_DIR'] + "\\Input"
    os.environ['op_path']          = os.environ['base_SF_BULK_DIR'] + "\\Output"
    os.environ['ClientName']       = 'Desert'

    # Desert PROD
    #os.environ['oauthurl']        = "https://corengine--corefull.my.salesforce.com/services/oauth2/token"		
    #os.environ['client_id']       = "3MVG9FG3dvS828gL.pckzEFAegb3ldYrPPIMDcmOfLnkRkkKUC1KoKgc7Ckl_6u8oZ8.oBcJBBHT_26VUWk04"
    #os.environ['client_secret']   = "1041F262D708D350653AB51A6ABF03A8305160B844F94F0D669F545100856CF5"
    #os.environ['username']        = "mike.overdorf@3trialforce.com.corefull"
    #os.environ['password']        = "winter16"
    #os.environ['host']            = "https://corengine--corefull.my.salesforce.com/"
    #os.environ['security_token']  = ""	
	
    print('Config variables...****************************************')
    print(os.environ['url1'])
    print(os.environ['username'])
    print(os.environ['ClientName'])
    print('os variables successfully set!')
    print('****************************************')	
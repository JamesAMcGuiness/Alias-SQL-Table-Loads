import json
import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin, SFType


class Salesforceconnect:
    def __init__(self, login_url, payload):
        self.login_url = login_url
        self.payload = payload
        self.session = req.Session()

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

    def lambda_handler():
        payload = {'client_id': os.environ.get('client_id'),
        'client_secret': os.environ.get('client_secret'),
        'username': os.environ.get('username'),
        'password': os.environ.get('password'),
        'grant_type': 'password'
        }
        
		
    oauthURL = os.environ.get('oauthurl')
    
    salesforceobject = Salesforceconnect(oauthURL, payload)
    token = salesforceobject.conn_and_get_token()
    
    host = os.environ['host']
    sessionId = token
    sandbox = True
    username = os.environ.get('username')
    password = os.environ.get('password')
    security_token = os.environ.get('security_token')
    client_id = os.environ.get('client_id')

    salesforce_connect_and_upload_client(filename, host, sessionId, sandbox, username,
                                                        password, security_token, client_id, key, object_name,
                                                        header, ex_id,fileCode)


def set_env_var():

    os.environ['base_SF_BULK_DIR'] = "e:\\CustBL_1.13"    
    os.environ['ip_path'] = os.environ['base_SF_BULK_DIR'] + "\\Input"
    os.environ['op_path'] = os.environ['base_SF_BULK_DIR'] + "\\Output"

# Northland PROD
    os.environ['oauthurl'] =  "https://na82.salesforce.com/services/oauth2/token"
    os.environ['client_id'] = "3MVG9uudbyLbNPZOEWIjmjLliEBbhuPt5_FecWtzEcB_zta2c1kNuMhKUzYgnEaU7HAfXP.FL5LzKZz2_7bkn"
    os.environ['client_secret'] = "6C2468802446E2CFBA01775080019C40E3715617E4602967323E1EA968D4F7F6"
    os.environ['username'] = "mike.overdorf@19trialforce.com"
    os.environ['password'] = "summer@2019gLLjUyMX71y7wo2YvaZSA9Nt"
    os.environ['host'] = "https://na82.salesforce.com"
    os.environ['security_token'] = "gLLjUyMX71y7wo2YvaZSA9Nt"
    os.environ['ClientName'] = "NORTHLAND"                                                        
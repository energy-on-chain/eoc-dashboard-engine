###############################################################################
# FILENAME: main.py
# CPROJECT: EOC-Dashboard-Engine
# AUTHOR: Matt Hartigan
# DATE CREATED: 10-Jun-2022
# DESCRIPTION: Pull API data from financialmodelingprep.com. Designed to be used as a google
# cloud function.
# COPYRIGHT: Powered by Financial Modeling Prep API (https://site.financialmodelingprep.com/)
# TERMS OF USE: https://site.financialmodelingprep.com/developer/docs/terms-of-service/
###############################################################################
import os
import sys
import json
import time
import datetime
import requests
import pandas as pd 
sys.path.append('../')    # enable imports from parent directory

from google.cloud import storage
from google.cloud import secretmanager


# CREDENTIALS
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="credentials.json"    # FIXME: dev only
client = secretmanager.SecretManagerServiceClient()    # get authentication info
project_id = "eoc-dashboard-352623"
FINANCIAL_MODELING_PREP_API_KEY = 'FINANCIAL_MODELING_PREP_API_KEY'
fmp_api_key = client.access_secret_version({"name": f"projects/{project_id}/secrets/{FINANCIAL_MODELING_PREP_API_KEY}/versions/latest"}).payload.data.decode('UTF-8')


# CONFIG
bucket_name = 'eoc-dashboard-bucket'
output_cloud_directory = 'data/stock_histories'
base_file_name = 'fmp_stock_history_24h_'
date_from = '2012-01-01'
date_to = datetime.date.today().strftime("%Y-%m-%d")
stock_list = [
    'AAPL',
    'AMZN',
    'GOOG',
    'META',
    '^GSPC',
    '^DJI',
    '^IXIC',
    '^TYX',
    '^FVX',
    '^TNX',
    '^VIX',
    'DX-Y.NYB',
    'ZGUSD',
    'CLUSD',
    'NGUSD'
]


# FUNCTIONS
def fmp_stock_history_daily(event, context):
# def fmp_stock_history_daily():    # FIXME: dev only
    """ Pulls daily OHLC data for the input list of stocks. """

    # Run through each coin in list
    for stock in stock_list:

        # Pull data
        print('Pulling data for ' + stock + '...')
        try:
            url = 'https://financialmodelingprep.com/api/v3/historical-price-full/' + stock + '?apikey=' + fmp_api_key
            res = requests.get(url).json()
            
        except Exception as e:
            print('Error during financial modeling prep api pull for ' + stock)
            print(e)

        try:
            # Parse data
            print('Parsing data for ' + stock + '...')
            df = pd.DataFrame(res['historical'])
            df = df.sort_values(by=['date'], ascending=True)
            df = df.reset_index(drop=True)
            print(df)

            # Save data to cloud
            print('Saving data for ' + stock + '...')
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            file_name = base_file_name + stock + '.csv'
            temp_file = '/tmp/' + file_name
            df.to_csv(temp_file, index=False)
            blob = bucket.blob(os.path.join(output_cloud_directory, file_name))
            blob.upload_from_filename(temp_file)

        except Exception as e:
            print('Error during financial modeling prep parsing of:  ' + stock)
            print(e)




# Local testing entry point
if __name__ == '__main__':
    fmp_stock_history_daily()

# TODO
# 

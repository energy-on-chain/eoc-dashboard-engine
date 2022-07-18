###############################################################################
# FILENAME: compare_time_history.py
# CPROJECT: EOC-Dashboard-Engine
# AUTHOR: Matt Hartigan
# DATE CREATED: 14-July-2022
# DESCRIPTION: Pulls in time histories for assets from cloud, gives them a uniform
# set of dates, then outputs the result to drive and cloud for use in performance
# comparison plots, etc.
###############################################################################
import os
import shutil
import itertools
import datetime
import pandas as pd 
import numpy as np
import json

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.cloud import storage
from google.cloud import secretmanager
from oauth2client.service_account import ServiceAccountCredentials
import google.auth


# AUTHENTICATE
SCOPES = ['https://www.googleapis.com/auth/drive']
gauth = GoogleAuth()    # pydrive library helper class for authenticating
client = secretmanager.SecretManagerServiceClient()
secret_name = "EOC_DASHBOARD_SERVICE_ACCT_KEY_JSON"
project_id = "eoc-dashboard-352623"
request = {"name": f"projects/{project_id}/secrets/{secret_name}/versions/latest"}
response = client.access_secret_version(request)
credentials_json = json.loads(response.payload.data.decode("UTF-8"))
gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_json, SCOPES)   
drive = GoogleDrive(gauth)    # this creates the google drive API instance... correct creds must already be contained in gauth


# CONFIG
bucket_name = 'eoc-dashboard-bucket'
cloud_file_path = 'pages'
file_name = 'eoc-dashboard-time-history-comparison.csv'
file_name_excel = 'eoc-dashboard-time-history-comparison.xlsx'
DRIVE_FOLDER_ID = '1fjVF41cZvQcIkArLcdzvJgLLKpC_PbCT'   
REFERENCE_FILE_ID = '12ZO87d-zXi4t0rK3cz7HTLHP1cjiclpBdL0AtsKt6lQ'   
REFERENCE_FILENAME = 'eoc-dashboard-time-history-comparison-reference'    
crypto_list = [
    'bitcoin',
    'ethereum',
    'binancecoin',
    'ripple',
    'cardano',
    'solana',
    'dogecoin',
    'polkadot',
    'kusama',
    'avalanche-2',
    'matic-network',
    'fantom',
    'aave',
    'uniswap',
    'sushi',
    'hex',
    'litecoin',
]


# FUNCTIONS
def output_results(df):
    """ Outputs the list of percentage ath drawdowns to 
    google drive sheets file and google cloud csv file. """

    # Output to google cloud storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    local_file = '/tmp/' + file_name
    cloud_file = cloud_file_path + '/' + file_name
    df.to_csv(local_file, header=True, index=True)
    blob = bucket.blob(cloud_file)
    blob.upload_from_filename(local_file) 
    print('updated google cloud file!')

    # Output to google sheets
    local_file_excel = '/tmp/' + file_name_excel    # name file path
    writer = pd.ExcelWriter(local_file_excel, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='ath_drawdown')
    writer.save()

    csv = drive.CreateFile({'id': REFERENCE_FILE_ID, 'parents': [{'id': DRIVE_FOLDER_ID}], 'title': REFERENCE_FILENAME, 'mimeType': 'application/vnd.ms-excel'})
    csv.SetContentFile(local_file_excel)
    csv.Upload({'convert': True})
    print('updated google drive file!')


def format_time_history(coin_time_history_dict):
    """ Takes in dictionary of coin time histories. Uses bitcoin (aka the one with the longest
    running time history) to create a data frame that holds all coin 1 day time histories where
    a null value is used for all rows where no data exists. Returns that data frame. """

    df = pd.DataFrame(coin_time_history_dict['bitcoin'])
    df = df.rename(columns={'price(usd)': 'bitcoin', 'date': 'date'})

    for coin, time_history in coin_time_history_dict.items():
        if coin != 'bitcoin':
            time_history_for_df = time_history.rename(columns={'price(usd)': coin, 'date': 'date'})
            df = pd.merge(df, time_history_for_df, on='date', how='left')
   
    return df


def generate_time_history_comparison_files(event, context):    # FIXME: for google cloud function deployment
# def generate_time_history_comparison_files():
    """ Main run function that is called to pull in asset time histories, format, and output them to 
    cloud and sheets for plotting, etc.. """

    # Get coin data
    coin_dict = {}
    for crypto in crypto_list:
        
        crypto_df = pd.read_csv('gs://eoc-dashboard-bucket/data/coin_histories/coingecko_coin_history_24h_' + crypto + '.csv')    # download file FIXME: filename
        
        crypto_df = crypto_df[['utc', 'price(usd)']]    # only need price and time
        crypto_df['date'] = pd.to_datetime(crypto_df['utc']).dt.date
        crypto_df = crypto_df.drop_duplicates(subset=['date'], keep="first")

        crypto_df.drop('utc', axis=1, inplace=True)   # eliminate duplicate columns

        coin_dict[crypto] = crypto_df

    # Format time histories
    formatted_time_history_df = format_time_history(coin_dict)

    # Output results
    output_results(formatted_time_history_df)


if __name__ == '__main__':
    generate_time_history_comparison_files()

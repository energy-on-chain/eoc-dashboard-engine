###############################################################################
# FILENAME: stablecoins.py
# CPROJECT: EOC-Dashboard-Engine
# AUTHOR: Matt Hartigan
# DATE CREATED: 26-July-2022
# DESCRIPTION: Pull in data for stable coins from cloud and output a table of
# metrics, as well as formatted time histories for easy plotting on a front
# end.
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
local_file_path = '/tmp'
cloud_file_path = 'pages'
DRIVE_FOLDER_ID = '1eKN2U172WEghWQWeWGfx7LKNiQk3eEBr'   
REFERENCE_FILE_ID = '1MCtIa4w9FrTJ9p2FAkUFmygAXZh3YCt95RnFcjIlWg4'   
REFERENCE_FILENAME = 'eoc-dashboard-stablecoin-24h-history' 
crypto_path = 'data/coin_histories/coingecko_coin_history_24h_'
crypto_list = [
    'bitcoin',
    'binance-usd',    # stablecoins
    'tether',
    'usd-coin',
    'dai',
    'frax',    
    'true-usd',    
    'paxos-standard',    
    'neutrino',    
    'usdd',    
    'tether-gold',    
]


# FUNCTIONS
def _output_to_cloud(df):
    """ Outputs the input data frame to google cloud. """

    storage_client = storage.Client()    # initialize storage client
    bucket = storage_client.bucket(bucket_name)

    file_name = 'eoc-dashboard-stablecoin-24h-history.csv'    # prep local file
    local_file = '/tmp/' + file_name
    cloud_file = cloud_file_path + '/' + file_name
    df.to_csv(local_file, header=True, index=True)
    
    blob = bucket.blob(cloud_file)    # upload to cloud
    blob.upload_from_filename(local_file) 


def _output_to_drive(df):
    """ Outputs the input data frame to google sheets on google drive. """

    file_name = 'eoc-dashboard-stablecoin-24h-history.xlsx'    # prep local file
    local_file = '/tmp/' + file_name
    writer = pd.ExcelWriter(local_file, engine='xlsxwriter')

    df.to_excel(writer, sheet_name='stablecoins')    # add stablecoins sheet

    last_updated_df = pd.DataFrame({'Last Updated': [datetime.datetime.utcnow()]})
    last_updated_df.to_excel(writer, sheet_name='last_updated')    # add last updated sheet

    writer.save()


    # csv = drive.CreateFile({'id': REFERENCE_FILE_ID, 'parents': [{'id': DRIVE_FOLDER_ID}], 'title': REFERENCE_FILENAME, 'mimeType': 'application/vnd.ms-excel'})
    # csv.SetContentFile(local_file)
    # csv.Upload({'convert': True})
    # df.to_csv(local_file, header=True, index=True)
    # file_name_excel = 'eoc-dashboard-correlation-matrix.xlsx'
    # local_file_excel = local_file_path + '/' + file_name_excel
    # writer = pd.ExcelWriter(local_file_excel, engine='xlsxwriter')
    # for sheet in list(google_sheets_matrix.keys()):
    #     google_sheets_matrix[sheet].to_excel(writer, sheet_name=sheet)
    # last_updated_df = pd.DataFrame({'Last Updated': [datetime.datetime.utcnow()]})
    # last_updated_df.to_excel(writer, sheet_name='last_updated')    # add last updated sheet
    # csv = drive.CreateFile({'id': REFERENCE_FILE_ID, 'parents': [{'id': DRIVE_FOLDER_ID}], 'title': REFERENCE_FILENAME, 'mimeType': 'application/vnd.ms-excel'})
    # csv.SetContentFile(local_file_excel)
    # csv.Upload({'convert': True})
    # print('updated google drive file!')


def _calculate_ssr(df):
    """ Takes full coin time history df in. Calculates the stablecoin supply ratio (SSR)
    based on BTC MC / Total Stablecoin MC, then returns the original df with this time history
    added as a new column. """

    result_df = df.copy()
    result_df['total-stablecoin-mc'] = 0

    for header in result_df.columns:    # sum up stablecoin market caps
        if ("mc" in header) and ("bitcoin" not in header):
            print('added: ', header)
            result_df['total-stablecoin-mc'] = result_df['total-stablecoin-mc'] + result_df[header]
    
    result_df['ssr'] = result_df['bitcoin-mc'] / result_df['total-stablecoin-mc']

    return result_df


def _format_time_history(coin_time_history_dict):
    """ Takes in dictionary of coin time histories. Uses bitcoin (aka the one with the longest
    running time history) to create a data frame that holds all coin 1 day time histories where
    a null value is used for all rows where no data exists. Returns that data frame. """

    df = pd.DataFrame(coin_time_history_dict['bitcoin'])
    # df = df.rename(columns={'price(usd)': 'bitcoin', 'date': 'date'})

    for coin, time_history in coin_time_history_dict.items():
        if coin != 'bitcoin':
            # time_history_for_df = time_history.rename(columns={'price(usd)': coin, 'date': 'date'})
            df = pd.merge(df, time_history, on='date', how='left')
   
    return df


# def generate_stablecoin_page(event, context):    # FIXME: for google cloud function deployment
def generate_stablecoin_page():
    """ Main run function that is called to pull stablecoin histories from clouds, compute useful metrics,
    then output those metrics as tables and coin time histories to the cloud and drive for front end use. """

    # GET DATA
    history_dict = {}

    # Load cryptos
    for crypto in crypto_list:
        
        try:
            crypto_df = pd.read_csv('gs://eoc-dashboard-bucket/data/coin_histories/coingecko_coin_history_24h_' + crypto + '.csv')    

            crypto_df.columns = ['unix', crypto + '-price', crypto + '-mc', crypto + '-vol', 'utc']    # make column names coin-specific
            crypto_df['date'] = pd.to_datetime(crypto_df['utc']).dt.date    # type cast to a datetime column

            crypto_df[crypto + '-supply'] = crypto_df[crypto + '-mc'] / crypto_df[crypto + '-price']    # add supply time history (mc / price)
            crypto_df[crypto + '-supply_shift'] = crypto_df[crypto + '-supply'].shift(periods=1)   
            crypto_df[crypto + '-supply-24h-change'] = (crypto_df[crypto + '-supply'] - crypto_df[crypto + '-supply_shift']) / crypto_df[crypto + '-supply_shift']    # calc 24h supply change

            crypto_df[crypto + '-vol_shift'] = crypto_df[crypto + '-vol'].shift(periods=1)   
            crypto_df[crypto + '-vol-24h-change'] = (crypto_df[crypto + '-vol'] - crypto_df[crypto + '-vol_shift']) / crypto_df[crypto + '-vol_shift']    # calc 24h volume change

            crypto_df = crypto_df.drop_duplicates(subset=['date'], keep="first")    # eliminate unnecessary columns
            crypto_df.drop(crypto + '-supply_shift', axis=1, inplace=True)    
            crypto_df.drop(crypto + '-vol_shift', axis=1, inplace=True)    
            crypto_df.drop('unix', axis=1, inplace=True)   
            crypto_df.drop('utc', axis=1, inplace=True)   

            history_dict[crypto] = crypto_df            

        except Exception as e:
            print('Error while loading and creating time hitories for for:  ' + crypto)
            print(e)

    # Combine into single time history df
    combined_df = _format_time_history(history_dict)
    print(combined_df)

    # Calculate and add total MC, SSR FIXM: add SSR oscillator?
    # full_df = _calculate_ssr(combined_df)    # FIXME

    # Output results
    # _output_to_cloud(full_df)    # FIXME
    # _output_to_drive(full_df)    # FIXME


# ENTRY POINT
if __name__ == '__main__':
    generate_stablecoin_page()

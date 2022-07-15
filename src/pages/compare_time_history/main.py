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

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import google.auth


# AUTHENTICATE
SCOPES = ['https://www.googleapis.com/auth/drive']
JSON_FILE = 'credentials.json'
gauth = GoogleAuth()
gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, SCOPES)    # dev only
# credentials, project_id = google.auth.default(scopes=SCOPES)    # production only
# gauth.credentials = credentials    # production only
drive = GoogleDrive(gauth)


# CONFIG
bucket_name = 'eoc-dashboard-bucket'
cloud_file_path = 'pages'
file_name = 'eoc-dashboard-crypto-ath-percent-drawdown.csv'
file_name_excel = 'eoc-dashboard-crypto-ath-percent-drawdown.xlsx'
DRIVE_FOLDER_ID = '1w8d5rb2khorGtsUOvQDQmDTx-p-NtGPp'   
REFERENCE_FILE_ID = '1a19zS8RWsURrXv81MdanRmNg21KS1aiKyux3VSrPVcQ'   
REFERENCE_FILENAME = 'eoc-dashboard-crypto-ath-percent-drawdown-reference'    
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

    # Create dir for temp files if it doesn't already exist
    # if not os.path.exists(os.path.join(os.getcwd(), 'tmp')):      # FIXME: production only    
        # os.mkdir(os.path.join(os.getcwd(), 'tmp'))

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

    # # Tear down temp directory
    shutil.rmtree(os.path.join(os.getcwd(), 'tmp'))    #FIXME: production only


def format_time_history(coin_time_history_dict):
    """ Takes in dictionary of coin time histories. Uses bitcoin (aka the one with the longest
    running time history) to create a data frame that holds all coin 1 day time histories where
    a null value is used for all rows where no data exists. Returns that data frame. """

    df = pd.DataFrame(coin_time_history_dict['bitcoin'])
    df = df.rename(columns={'price(usd)': 'bitcoin', 'date': 'date'})

    for coin, time_history in coin_time_history_dict.items():
        # df[coin] = time_history['price(usd)']
        df[coin] = np.where(df['date'] == time_history['date'], time_history['price(usd)'], None)
   
    # max_time_history_length = 0
    # max_time_history_coin = ''
    # for coin, time_history in coin_time_history_dict.items():
    #     if len(time_history) > max_time_history_length:
    #         max_time_history_coin = coin
    
    # print(max_time_history_coin)
    # print(coin_time_history_dict[max_time_history_coin]['price(usd)'])
    # print(coin_time_history_dict[max_time_history_coin])
    # print(len(coin_time_history_dict[max_time_history_coin]))
    # Find longest time history
    # Extract dates and use this as base
    # Add each coin, where data doesn't exist for price input a None
    print(df)

    return df


# def generate_time_history_comparison_files(event, context):    # FIXME: for google cloud function deployment
def generate_time_history_comparison_files():
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
    # output_results(formatted_time_history_df)


if __name__ == '__main__':
    generate_time_history_comparison_files()

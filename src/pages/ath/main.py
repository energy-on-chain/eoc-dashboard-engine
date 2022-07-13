###############################################################################
# FILENAME: ath.py
# CPROJECT: EOC-Dashboard-Engine
# AUTHOR: Matt Hartigan
# DATE CREATED: 13-July-2022
# DESCRIPTION: Pull in data from cloud and generate a list of the percentage 
# down from all time highs that each coin is (on daily time scale).
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
local_file_path = 'tmp'
cloud_file_path = 'pages'
DRIVE_FOLDER_ID = '14LAPdLKJYVI1TS0pUL0D_UFShCoXLt4P'    #FIXME
REFERENCE_FILE_ID = '1bPH7CLEOHmDQDHcnhSkSdQyekqrtUsxnvFCxvN_TtlM'    #FIXME
REFERENCE_FILENAME = 'eoc-dashboard-correlation-matrix-references'    #FIXME
crypto_path = 'data/coin_histories/coingecko_dailiy_coin_history_'
coin_list = [
    'bitcoin',
    'ethereum',
    'tether',
    'usd-coin',
    'binancecoin',
    'binance-usd',
    'ripple',
    'cardano',
    'solana',
    'dogecoin',
    'dai',
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
def output_results(asset_list, correlation_matrix):
    """ Outputs the correlation matrices specified by the user in 
    the input 'correlation_matrix' variable to google cloud and
    google sheets for final beautification on the front end. """

    # Create dir for temp files if it doesn't already exist
    if not os.path.exists(os.path.join(os.getcwd(), 'tmp')):    
        os.mkdir(os.path.join(os.getcwd(), 'tmp'))

    # Create correlation matrix for each lookback period
    google_sheets_matrix = {}
    for lookback in correlation_matrix.keys():

        print('Correlation matrix for: {} day lookback'.format(lookback))
        df = create_matrix(asset_list, correlation_matrix[lookback])
        print(df)

        # Output to google cloud storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        file_name = 'eoc-dashboard-correlation-matrix-' + str(lookback) + 'day.csv'
        local_file = local_file_path + '/' + file_name
        cloud_file = cloud_file_path + '/' + file_name
        df.to_csv(local_file, header=True, index=True)
        blob = bucket.blob(cloud_file)
        blob.upload_from_filename(local_file) 
        print('updated google cloud file!')

        # Prep for output to google sheets
        google_sheets_matrix[str(lookback)] = df

    # Output to google sheets
    file_name_excel = 'eoc-dashboard-correlation-matrix.xlsx'
    local_file_excel = local_file_path + '/' + file_name_excel

    writer = pd.ExcelWriter(local_file_excel, engine='xlsxwriter')
    for sheet in list(google_sheets_matrix.keys()):
        google_sheets_matrix[sheet].to_excel(writer, sheet_name=sheet)
    writer.save()

    csv = drive.CreateFile({'id': REFERENCE_FILE_ID, 'parents': [{'id': DRIVE_FOLDER_ID}], 'title': REFERENCE_FILENAME, 'mimeType': 'application/vnd.ms-excel'})
    csv.SetContentFile(local_file_excel)
    csv.Upload({'convert': True})
    print('updated google drive file!')

    # Tear down temp directory
    shutil.rmtree(os.path.join(os.getcwd(), 'tmp'))


def _calculate_percentage_drawdown(df, price_column_label):
    """ Calculates how far down (in terms of percentage) a coin is down given the
    input time history. """

    current_price = df[price_column_label].iloc[-1]
    ath_price = df[price_column_label].max()

    return round(current_price / ath_price, 2)


# def generate_ath_page(event, context):    # FIXME: for google cloud function deployment
def generate_ath_page():
    """ Main run function that is called to calculate and output ath drawdown for each
    coind of interest to a google sheet. """

    # Get coin data
    coin_dict = {}
    for crypto in crypto_list:
        
        crypto_df = pd.read_csv('gs://eoc-dashboard-bucket/data/coin_histories/coingecko_coin_history_24h_' + crypto + '.csv')    # download file FIXME: filename
        
        crypto_df = crypto_df[['utc', 'price(usd)']]    # only need price and time
        crypto_df['date'] = pd.to_datetime(crypto_df['utc']).dt.date
        crypto_df = crypto_df.drop_duplicates(subset=['date'], keep="first")

        crypto_df.drop('utc', axis=1, inplace=True)   # eliminate duplicate columns

        coin_dict[crypto] = crypto_df

    # Calculate aths
    ath_dict = {}
    for coin, history in coin_dict.items():
        ath_dict[coin] = _calculate_percentage_drawdown(history, 'price(usd)')
    print(ath_dict)

    # Output results
    output_results(list(history_dict.keys()), big_correlation_matrix)


if __name__ == '__main__':
    generate_ath_page()

###############################################################################
# FILENAME: correlation.py
# CPROJECT: EOC-Dashboard-Engine
# AUTHOR: Matt Hartigan
# DATE CREATED: 10-Jun-2022
# DESCRIPTION: Pull in data from cloud and generate a correlation matrix for 
# all of the included assets. Outputs results to cloud and google sheet on 
# google drive. The correlation method used is Pearson. 
# Source: https://algotrading101.com/learn/python-correlation-guide/
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
# gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, SCOPES)    # dev only
credentials, project_id = google.auth.default(scopes=SCOPES)
gauth.credentials = credentials
drive = GoogleDrive(gauth)


# CONFIG
bucket_name = 'eoc-dashboard-bucket'
local_file_path = 'tmp'
cloud_file_path = 'pages'
DRIVE_FOLDER_ID = '14LAPdLKJYVI1TS0pUL0D_UFShCoXLt4P'
REFERENCE_FILE_ID = '1bPH7CLEOHmDQDHcnhSkSdQyekqrtUsxnvFCxvN_TtlM'
REFERENCE_FILENAME = 'eoc-dashboard-correlation-matrix-references'
lookback_period_list = [7, 30, 90, 365]
crypto_path = 'data/coin_histories/coingecko_coin_history_24h_'
crypto_list = [
    'bitcoin',
    'ethereum',
]
stock_path = 'data/stock_histories/fmp_stock_history_24h_'
stock_list = [
    'AAPL',
    'AMZN',
    'GOOG',
    'META',
    '^GSPC',
    '^DJI',
    '^IXIC',
    '^TYX',
    '^TNX',
    'ZGUSD',
    'CLUSD',
    'NGUSD'
]


# FUNCTIONS
def create_matrix(asset_list, permutation_dict):
    """ Takes in list of assets that are of interest for the correlation matrix,
    finds their previously calculated correlation value in the input dict, then
    returns a dataframe (matrix) that can be displayed on an excel sheet. """

    df = pd.DataFrame(1.0, index=asset_list, columns=asset_list)    # instantiate df
    
    for permutation in list(permutation_dict.keys()):    # find the correlation val that corresponds to each pair of interest
        df.loc[permutation[0]][permutation[1]] = permutation_dict[permutation]

    return df


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


def calculate_correlation(input_df, returns_header1, returns_header2, lookback):
    """ Calculates the Pearson correlation coefficient of the two input data frames for 
    the input lookback period and returns the result as a float. Assumes that the two input
    data frames are already properly formatted (i.e. they have dates that match). """

    # Format data
    df = pd.DataFrame()
    df['x'] = input_df[returns_header1]
    df['y'] = input_df[returns_header2]
    df = df.iloc[-lookback:]    # slice the df for the specified lookback period

    # Perform Pearson correlation coeff calcs
    df['step1'] = df.x - df.x.mean()
    df['step2'] = df.y - df.y.mean()
    df['step3'] = df.step1 * df.step2
    step4 = df.step3.sum()
    df['step5'] = df.step1 ** 2
    df['step6'] = df.step2 ** 2
    step7 = df.step5.sum() * df.step6.sum()
    step8 = np.sqrt(step7)
    correlation_coeff = step4 / step8

    # print(correlation_coeff)    # can validate agains the python library function using: df.x.corr(df.y)
    return correlation_coeff


def generate_correlation_page(event, context):    # FIXME: for google cloud function deployment
# def generate_correlation_page():
    """ Main run function that is called to compute correlations between all possible combinations of the specified stocks and cryptos
    for the input list of lookback periods. It then outputs the resulting correlation matrix to google cloud and google sheets. """

    # GET DATA
    history_dict = {}
    big_correlation_matrix = {}

    # Load cryptos
    for crypto in crypto_list:
        
        # crypto_df = pd.read_csv('gs://eoc-dashboard-bucket/data/coin_histories/coingecko_daily_coin_history_' + crypto + '.csv')    # original file name
        crypto_df = pd.read_csv('gs://eoc-dashboard-bucket/data/coin_histories/coingecko_coin_history_24h_' + crypto + '.csv')    
        
        crypto_df = crypto_df[['utc', 'price(usd)']]    # only need price and time
        crypto_df['date'] = pd.to_datetime(crypto_df['utc']).dt.date
        crypto_df = crypto_df.drop_duplicates(subset=['date'], keep="first")
        crypto_df['previous_close'] = crypto_df['price(usd)'].shift(periods=1)
        crypto_df[crypto + '_rate_of_return'] = (crypto_df['price(usd)'] - crypto_df['previous_close']) / crypto_df['previous_close']    # calc rate of return

        crypto_df.drop('price(usd)', axis=1, inplace=True)    # eliminate unnecessary columns
        crypto_df.drop('previous_close', axis=1, inplace=True)    
        crypto_df.drop('utc', axis=1, inplace=True)   

        history_dict[crypto] = crypto_df

    # Load stocks
    for stock in stock_list:

        try:
            # stock_df = pd.read_csv('gs://eoc-dashboard-bucket/data/stock_histories/fmp_daily_stock_history_' + stock + '.csv')    # original file name
            stock_df = pd.read_csv('gs://eoc-dashboard-bucket/data/stock_histories/fmp_stock_history_24h_' + stock + '.csv')   

            stock_df = stock_df[['date', 'close']]    # eliminate unnecessary columns
            stock_df['date'] = pd.to_datetime(stock_df['date']).dt.date
            stock_df = stock_df.drop_duplicates(subset=['date'], keep="first")
            stock_df['previous_close'] = stock_df['close'].shift(periods=1)
            stock_df[stock + '_rate_of_return'] = (stock_df['close'] - stock_df['previous_close']) / stock_df['previous_close']    # calc rate of return

            stock_df.drop('close', axis=1, inplace=True)    # eliminate unnecessary columns
            stock_df.drop('previous_close', axis=1, inplace=True)    

            history_dict[stock] = stock_df
        
        except Exception as e:
            print('Error during correlation function pulling of data for:  ' + stock)
            print(e)

    # Define permutations to be run
    permutation_list = list(itertools.permutations(history_dict.keys(), r=2))    # generate all possible market pairs

    # Run all permutations for all lookbacks
    for lookback_period in lookback_period_list:

        small_correlation_matrix = {}    # only covers correlations for the current lookback period being run

        for permutation in permutation_list:

            print('Computing correlation between {} and {} for {} lookback period...'.format(permutation[0], permutation[1], lookback_period))

            df0 = history_dict[permutation[0]]
            df1 = history_dict[permutation[1]]
            merged_df = df0.merge(df1, on=['date'])    # ensure that only dates that occur in both histories are used for crypto / stock pairs
            merged_df = merged_df.dropna()

            correlation_coeff = calculate_correlation(merged_df, permutation[0] + '_rate_of_return', permutation[1] + '_rate_of_return', lookback_period)

            small_correlation_matrix[permutation] = correlation_coeff

        big_correlation_matrix[lookback_period] = small_correlation_matrix
    
    # Output results
    output_results(list(history_dict.keys()), big_correlation_matrix)


# if __name__ == '__main__':
#     generate_correlation_page()

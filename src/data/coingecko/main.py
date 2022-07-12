###############################################################################
# FILENAME: main.py
# CPROJECT: EOC-Dashboard-Engine
# AUTHOR: Matt Hartigan
# DATE CREATED: 7-Jun-2022
# DESCRIPTION: Pull API data from coingecko.com. Designed to be used as a google
# cloud function.
# COPYRIGHT: Powered by CoinGecko API (https://www.coingecko.com/)
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


# CONFIG
bucket_name = 'eoc-dashboard-bucket'
output_cloud_directory = 'data/coin_histories'
base_file_name = 'coingecko_coin_history_24h_'
vs_currency = 'usd'
days = 'max'
interval = 'daily'
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
def coingecko_coin_history_daily(event, context):
# def coingecko_coin_history_daily():    # FIXME: dev only
    """ Pulls daily OHLC data for the input list of coins. """

    # Run through each coin in list
    for coin in coin_list:

        # Pull data
        print('Pulling data for ' + coin + '...')
        try:
            url = 'https://api.coingecko.com/api/v3/coins/' + coin + '/market_chart?vs_currency=' + vs_currency + '&days=' + days + '&interval=' + interval
            res = requests.get(url).json()
        except Exception as e:
            print('Error during coingecko api pull for ' + coin)
            print(e)

        # Parse data
        print('Parsing data for ' + coin + '...')
        price_df = pd.DataFrame.from_records(res['prices'], columns=['unix', 'price(usd)'])
        mc_df = pd.DataFrame.from_records(res['market_caps'], columns=['unix', 'market_cap(usd)'])
        vol_df = pd.DataFrame.from_records(res['total_volumes'], columns=['unix', 'volume(usd)'])
        merged_df = pd.concat([price_df.set_index('unix'), mc_df.set_index('unix'), vol_df.set_index('unix')], axis=1, join='inner').reset_index()
        merged_df['utc'] = merged_df['unix'].apply(lambda x: datetime.datetime.utcfromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))    # utc time

        # Save data to cloud
        print('Saving data for ' + coin + '...')
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        file_name = base_file_name + coin + '.csv'
        temp_file = '/tmp/' + file_name
        merged_df.to_csv(temp_file, index=False)
        blob = bucket.blob(os.path.join(output_cloud_directory, file_name))
        blob.upload_from_filename(temp_file)

        print(merged_df)


# Local testing entry point
if __name__ == '__main__':
    coingecko_coin_history_daily()


# TODO
# 

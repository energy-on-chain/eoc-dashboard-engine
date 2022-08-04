###############################################################################
# PROJECT: EOC-Dashboard-Engine
# AUTHOR: Matt Hartigan
# DATE CREATED: 3-August-2022
# DESCRIPTION: Pull in the relevant data feeds, assess where they are in terms
# of anomaly levels, and output a dashboard page summarizing status. Also send
# automatic emails when certain levels are reached.
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

from anomaly_config import config_params


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
DRIVE_FOLDER_ID = '1r9WyWELm414GzWlO7ruRgYmgFdN7vouG'   
REFERENCE_FILE_ID = '1TY9mvBtw5S8G0v1iEoGIpk7c98MVed2tnlVCGSD4XJU'   
REFERENCE_FILENAME = 'anomaly-sheet' 


# FUNCTIONS
def _output_to_cloud(input_dict):
    """ Outputs the input data frame to google cloud. """

    storage_client = storage.Client()    # initialize storage client
    bucket = storage_client.bucket(bucket_name)

    for sheet in input_dict.keys():

        # Output to google cloud storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        file_name = 'eoc-dashboard-' + str(sheet) + '.csv'
        local_file = '/tmp/' + file_name
        cloud_file = cloud_file_path + '/' + file_name
        input_dict[sheet].to_csv(local_file, header=True, index=False)
        blob = bucket.blob(cloud_file)
        blob.upload_from_filename(local_file) 
        print('updated google cloud file!')


def _output_to_drive(input_dict):
    """ Outputs the input data frame to google sheets on google drive. """

    file_name = 'eoc-dashboard-stablecoin-24h-history.xlsx'    # prep local file
    local_file = '/tmp/' + file_name
    writer = pd.ExcelWriter(local_file, engine='xlsxwriter')
    for sheet in list(input_dict.keys()):
        input_dict[sheet].to_excel(writer, sheet_name=sheet)

    last_updated_df = pd.DataFrame({'Last Updated': [datetime.datetime.utcnow()]})
    last_updated_df.to_excel(writer, sheet_name='last_updated')    # add last updated sheet

    writer.save()

    csv = drive.CreateFile({'id': REFERENCE_FILE_ID, 'parents': [{'id': DRIVE_FOLDER_ID}], 'title': REFERENCE_FILENAME, 'mimeType': 'application/vnd.ms-excel'})
    csv.SetContentFile(local_file)
    csv.Upload({'convert': True})
    print('updated google drive file!')


def _send_email_alert(input_dict):
    pass


def generate_anomaly_page(event, context):    # FIXME: for cloud deployment only
# def generate_anomaly_page():
    """
    FIXME: description goes here
    """

    anomaly_cols = ['Metric', 'Threshold', 'Current Level', 'Description']
    anomaly_df = pd.DataFrame(columns=anomaly_cols)    # holds the calculated anomaly status data frame to be output to dash
    counter = 0

    # Compute anomaly stats for each metric
    for metric in config_params.keys():

        print('Evaluating anomaly status for: ', metric)

        # Get raw data
        if not config_params[metric]['is_column']: 
            df = pd.read_csv(config_params[metric]['input_time_history_file_path']).transpose(copy=False)    # transpose if necessary
            header = df.iloc[0]
            df = df[1:]
            df.columns = header
        else:
            df = pd.read_csv(config_params[metric]['input_time_history_file_path'])   

        # Get current level
        if not config_params[metric]['is_standard_threshold']:
            if config_params[metric]['index'] is not None:
                current_level = df[config_params[metric]['series_label']].iloc[config_params[metric]['index']]
            else:
                current_level = df[config_params[metric]['series_label']].iloc[len(df)-1]
            threshold = config_params[metric]['threshold']
        else:
            df['sd_history'] = df[config_params[metric]['series_label']].std() 
            current_level = df['sd_history'].iloc[len(df)-1]
            threshold = 2 * df[config_params[metric]['series_label']].std() 

        # Build anomaly df
        row = [metric, threshold, current_level, config_params[metric]['description']]
        anomaly_df.loc[counter] = row

        # Take action(s) based on current level
        print('Threshold: {} Current: {}'.format(threshold, current_level))
        if config_params[metric]['is_standard_threshold'] and (abs(current_level) > abs(threshold)):
            print('FIXME: send email, this is an anomaly')
        # Output status to dashboard
        elif current_level > threshold:
            print('FIXME: send email, this is an anomaly')
        else:
            print('Not above threshold, take no action.')

        # Save result and increment
        counter = counter + 1

    # Output results
    _output_to_cloud({'anomaly-sheet': anomaly_df})
    _output_to_drive({'anomaly_df': anomaly_df})


# DEV ENTRY POINT
if __name__ == '__main__':
    generate_anomaly_page()


# TODO:
# handle simple auto emails
# handle formatted emails with logo, etc.
# handle sending formatted pdfs with logo, etc.


###############################################################################
# PROJECT: EOC-Dashboard-Engine
# AUTHOR: Matt Hartigan
# DATE: 3-August-2022
# FILENAME: anomaly_config.py
# DESCRIPTION: Defines the key parameters for this trading bot.
###############################################################################


email_config = {
    'sender_email': 'matthew@energyonchain.net',
    'receiver_email_list': ['matthew.t.hartigan@gmail.com'],
    'subject': 'CVC Anomaly Status Summary',
    'body': 'The following anomalies have been identified: ',
    'footer': 'For more details, see the CVC Dashboard here: https://docs.google.com/spreadsheets/d/1ZncxtsmBCm31MSKqwDMfxtdvG-OTosrpyhO09p1wPSY/edit#gid=1851173820',
    'tagline': 'Happy hunting!',
}


config_params = {
    'ath_drawdown (btc)': {
        'input_time_history_file_path': 'gs://eoc-dashboard-bucket/pages/eoc-dashboard-crypto-ath-percent-drawdown.csv',
        'series_label': 'percent_drawdown',
        'is_column': False,
        'is_standard_threshold': False,
        'threshold': 0.8,
        'index': 0,
        'description': 'triggers when btc > 50% drawdown'
    },
    'ath_drawdown (eth)': {
        'input_time_history_file_path': 'gs://eoc-dashboard-bucket/pages/eoc-dashboard-crypto-ath-percent-drawdown.csv',
        'series_label': 'percent_drawdown',
        'is_column': False,
        'is_standard_threshold': False,
        'threshold': 0.8,
        'index': 1,
        'description': 'triggers when eth > 50% drawdown'
    },
}


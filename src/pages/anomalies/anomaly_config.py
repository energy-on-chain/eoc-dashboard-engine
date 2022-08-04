###############################################################################
# PROJECT: EOC-Dashboard-Engine
# AUTHOR: Matt Hartigan
# DATE: 3-August-2022
# FILENAME: anomaly_config.py
# DESCRIPTION: Defines the key parameters for this trading bot.
###############################################################################


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


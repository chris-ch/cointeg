import os
import json
import logging
from urllib import quote_plus
import pandas
import Quandl

_CACHE_LOCATION = '.quandl_cache'


def load_quandl(datasets):
    dataset = None
    sensitive_file = os.path.abspath('sensitive.json')
    with open(sensitive_file) as sensitive_data:
        json_data = json.load(sensitive_data)
        logging.info('loaded sensitive data: %s', json_data)
        quandl_token = json_data['quandl_token']
        dataset = Quandl.get(datasets, authtoken=quandl_token).dropna()

    return dataset


def load_prices(datasets, start_date=None, end_date=None, field_selector='CLOSE'):
    if not os.path.isdir(os.path.abspath(_CACHE_LOCATION)):
        os.makedirs(os.path.abspath(_CACHE_LOCATION))
    
    datasets_encoded = quote_plus('@'.join(datasets))
    cache_path = os.path.abspath(os.sep.join([_CACHE_LOCATION, datasets_encoded]))
    if os.path.isfile(cache_path):
        logging.info('reading datasets from local cache')
        quandl_df = pandas.read_pickle(cache_path)

    else:
        quandl_df = load_quandl(datasets)
        quandl_df.to_pickle(cache_path)
    
    filtered = quandl_df
    if start_date is not None and end_date is not None:
        filtered = quandl_df.ix[start_date:end_date]
        
    elif start_date is not None and end_date is None:
        filtered = quandl_df.ix[start_date::]
        
    elif start_date is None and end_date is not None:
        filtered = quandl_df.ix[::end_date]

    selected = filtered
    if field_selector is not None:
        pattern = ' - ' + field_selector.upper()
        selected = filtered.loc[:, filtered.columns.str.upper().str.endswith(pattern)]

    return selected

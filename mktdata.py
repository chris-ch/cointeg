
_QUANDL_TOKEN = 'UvpzeSNabDxs3DKxggsi'  # Christophe private
_CACHE_LOCATION = '.quandl_cache'


import os
import logging
from urllib import quote_plus
import pandas
import Quandl


def load_quandl(datasets, trim_start=None):
    if trim_start:
        return Quandl.get(datasets, trim_start=trim_start, authtoken=_QUANDL_TOKEN).dropna()

    else:
        return Quandl.get(datasets, authtoken=_QUANDL_TOKEN).dropna()


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

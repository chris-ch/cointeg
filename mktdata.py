import os
import json
import logging
import argparse
from urllib import quote_plus
import httplib2
import oauth2client
from apiclient import discovery
import pandas
import Quandl


_SENSITIVE_FILE = 'sensitive.json'
_CACHE_LOCATION = '.quandl_cache'

_GOOGLE_CLIENT_SECRET = 'client_secret.json'
_GOOGLE_SCOPES = 'https://www.googleapis.com/auth/drive.readonly.metadata'


def sensitive(key):
    sensitive_file = os.path.abspath(_SENSITIVE_FILE)
    with open(sensitive_file) as sensitive_data:
        sensitive_value = json.load(sensitive_data)
        logging.info('loaded sensitive value %s = %s', key, sensitive_value[key])
        return sensitive_value[key]


def load_prices_quandl(codes, start_date=None, end_date=None, field_selector='CLOSE'):
    if not os.path.isdir(os.path.abspath(_CACHE_LOCATION)):
        os.makedirs(os.path.abspath(_CACHE_LOCATION))

    datasets_encoded = quote_plus('@'.join(codes))
    cache_path = os.path.abspath(os.sep.join([_CACHE_LOCATION, datasets_encoded]))
    if os.path.isfile(cache_path):
        logging.info('reading datasets from local cache')
        quandl_df = pandas.read_pickle(cache_path)

    else:
        quandl_df = Quandl.get(codes, authtoken=sensitive('quandl_token')).dropna()
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


def load_prices_eoddata(codes):
    if not os.path.isdir(os.path.abspath(_CACHE_LOCATION)):
        os.makedirs(os.path.abspath(_CACHE_LOCATION))

    store = oauth2client.file.Storage('credential_storage.json')
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = oauth2client.client.flow_from_clientsecrets(_GOOGLE_CLIENT_SECRET, _GOOGLE_SCOPES)
        flags = argparse.ArgumentParser(parents=[oauth2client.tools.argparser]).parse_args()
        credentials = oauth2client.tools.run_flow(flow, store, flags=flags)

    drive = discovery.build('drive', 'v2', http=credentials.authorize(httplib2.Http()))
    files = drive.files().list().execute().get('items', [])
    for f in files:
        print f['title'], f['mimeType']


if __name__ == '__main__':
    load_prices_eoddata(['IBM'])
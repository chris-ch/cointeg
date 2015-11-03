import logging

import numpy
import pandas
from matplotlib import pyplot
from pandas.stats.api import ols
from datetime import datetime
from mktdatadb import list_tickers, load_book_states, LoaderARCA, get_date_range

from statsext import cointeg
from mktdata import load_prices_quandl


__author__ = 'Christophe'

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(name)s - %(message)s', level=logging.DEBUG)
    for ticker in list_tickers('equities'):
        logging.info('available data for %s, %s', ticker, get_date_range(ticker, 'equities'))

    nyse_arca = LoaderARCA()
    start_date = datetime(2015, 3, 1)
    end_date = datetime(2015, 3, 31)
    book_states = nyse_arca.load_book_states('HYG US Equity', start_date, end_date)
    book_states.to_pickle('HYG.pkl')
    book_states = nyse_arca.load_book_states('JNK US Equity', start_date, end_date)
    book_states.to_pickle('JNK.pkl')
    #for book_state in ticks_book_states('HYG US Equity', start_time, end_time):
    #    print book_state
import logging
import os
import pandas
from datetime import datetime
from matplotlib import pyplot
import sys
from mktdatadb import list_tickers, LoaderARCA, get_date_range

from statsext import cointeg


__author__ = 'Christophe'


def save_sample():
    for ticker in list_tickers('equities'):
        logging.info('available data for %s, %s', ticker, get_date_range(ticker, 'equities'))

    nyse_arca = LoaderARCA()
    start_date = datetime(2015, 4, 1)
    end_date = datetime(2015, 5, 31)
    book_states = nyse_arca.load_book_states('HYG US Equity', start_date, end_date)
    book_states.to_pickle('HYG.pkl')
    book_states = nyse_arca.load_book_states('JNK US Equity', start_date, end_date)
    book_states.to_pickle('JNK.pkl')


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(name)s - %(message)s', level=logging.DEBUG)
    df1 = pandas.read_pickle(os.sep.join(['data', 'HYG.pkl'])).astype(float)
    df2 = pandas.read_pickle(os.sep.join(['data', 'JNK.pkl'])).astype(float)
    df = pandas.concat([(df1['bid'] + df1['ask']) / 2, (df2['bid'] + df2['ask']) / 2], axis=1).ffill()
    df.columns = ['HYG', 'JNK']
    vectors = cointeg.get_johansen(df[df.index <= '2015-04-30'], lag=1)
    print vectors
    calibration = df[df.index <= '2015-04-30'].dot(vectors[0])
    print calibration.describe()
    signal = pandas.DataFrame(df[df.index >= '2015-05-01'].dot(vectors[0]), columns=['signal'])
    print '----'
    print signal
    print '----'
    # dividend paid at this time
    avg = pandas.ewma(signal['signal'], halflife=5000)
    threshold = calibration.std() * 0.5
    signal['threshold1'] = avg - 2 * threshold
    signal['threshold2'] = avg - threshold
    signal['average'] = avg
    signal['threshold3'] = avg + threshold
    signal['threshold4'] = avg + 2 * threshold
    print signal
    plot1 = signal[signal.index.normalize() == '2015-05-01']
    plot2 = signal[signal.index.normalize() == '2015-05-05']
    plot3 = signal[signal.index.normalize() == '2015-05-06']
    plot1.index = plot1.index.time
    plot2.index = plot2.index.time
    plot3.index = plot3.index.time
    plot1.plot()
    plot2.plot()
    plot3.plot()
    pyplot.show()

    #df1[['bid', 'ask']][(df1.index >= '2015-03-17') & (df1.index < '2015-03-21')].plot()
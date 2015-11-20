import logging
import os
import numpy
import pandas
from datetime import datetime
from matplotlib import pyplot
from matplotlib import ticker
from statsmodels.formula.api import ols
import math
from bollinger import get_position_scaling
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


class IrregularDatetimeFormatter(ticker.Formatter):
    def __init__(self, dates, format='%Y-%m-%d %H:%M:%S'):
        self.dates = dates
        self._format = format

    def __call__(self, x, pos=0):
        """ Label for time x at position pos. """
        ind = int(round(x))
        if ind >= len(self.dates) or ind < 0:
            return ''

        as_timestamp = pandas.to_datetime(str(self.dates[ind]))
        return as_timestamp.strftime(self._format)


class CoIntegration(object):
    def __init__(self, training_set):
        cointeg_vectors = cointeg.get_johansen(training_set, lag=1)
        if len(cointeg_vectors) > 0:
            self._vector = cointeg_vectors[0]
            self._calibration = pandas.DataFrame(training_set.dot(self._vector))
            self._calibration.columns = ['signal']
            delta_calibration = self._calibration - self._calibration.shift(periods=1)
            delta_calibration.columns = ['dy']
            delta_calibration['y'] = self._calibration.shift(1)
            regress = ols(data=delta_calibration, formula='dy ~ y').fit()
            logging.info('regression results: %s', regress.summary())
            self._half_life = -int(math.log(2) / regress.params.y)

    @property
    def calibration(self):
        return self._calibration

    @property
    def half_life(self):
        return self._half_life

    @property
    def vector(self):
        return self._vector


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(name)s - %(message)s', level=logging.DEBUG)
    df1 = pandas.read_pickle(os.sep.join(['data', 'HYG.pkl'])).astype(float)
    df2 = pandas.read_pickle(os.sep.join(['data', 'JNK.pkl'])).astype(float)
    df = pandas.concat([(df1['bid'] + df1['ask']) / 2, (df2['bid'] + df2['ask']) / 2], axis=1).ffill()
    df.columns = ['HYG', 'JNK']
    cointegration = CoIntegration(df[df.index <= '2015-04-30'])
    print('half-life', cointegration.half_life)
    print()
    signal = pandas.DataFrame(df[(df.index >= '2015-05-01') & (df.index <= '2015-05-20')].dot(cointegration.vector),
                              columns=['signal'])

    formatter = IrregularDatetimeFormatter(signal.index.values)

    fig, x_prices = pyplot.subplots()
    x_prices.xaxis.set_major_formatter(formatter)
    df.plot(ax=x_prices, x=numpy.arange(len(df)), subplots=True)

    #avg = pandas.ewma(signal['signal'], halflife=cointegration.half_life)
    avg = cointegration.calibration['signal'].mean()
    threshold = 0.95 * cointegration.calibration['signal'].std()
    logging.info('size of threshold: %.2f', threshold)
    cumul = {'current_scaling': 0.}

    def scale(row, cumul=cumul):
        current_scaling = cumul['current_scaling']
        price = row['signal']
        new_position_scaling = get_position_scaling(price, current_scaling, avg, threshold)
        # updating for next step
        cumul['current_scaling'] = new_position_scaling
        result = {
            'band_inf': avg + ((new_position_scaling + 1) * threshold),
            'band_mid': avg + (new_position_scaling * threshold),
            'band_sup': avg + ((new_position_scaling - 1) * threshold)
        }
        return pandas.Series(result)

    signal = pandas.concat([signal, signal.apply(scale, axis=1)], axis=1)
    logging.info('finding crossing levels')

    # compute threshold for going long and threshold for going short

    logging.info('finding current crossing levels')

    logging.info('writing results to output file')
    writer = pandas.ExcelWriter('signal.test.xlsx', engine='xlsxwriter')
    signal.to_excel(writer, 'Sheet1')
    writer.save()
    fig, ax_signal = pyplot.subplots()
    ax_signal.xaxis.set_major_formatter(formatter)
    signal.plot(ax=ax_signal, x=numpy.arange(len(signal)))

    # ref_level = pandas.DataFrame(((signal['signal'] - signal['average']) / threshold).astype('int'))
    #fig, ax_ref_level = pyplot.subplots()
    #ax_ref_level.xaxis.set_major_formatter(formatter)
    #ref_level.plot(ax=ax_ref_level, x=numpy.arange(len(signal)))

    pyplot.show()

import logging
import os
import numpy
import pandas
from statsext import cointeg
from matplotlib import pyplot
from pandas.stats.api import ols

from mktdata import load_prices

__author__ = 'Christophe'

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(name)s - %(message)s', level=logging.DEBUG)
    codes = ['GOOG/NYSE_EWA', 'GOOG/NYSE_EWC']
    quandl_data = load_prices(codes, start_date='2006-04-26', end_date='2012-04-09')
    results = cointeg.get_johansen(quandl_data, lag=1, significance='90%')
    print
    print 'critical_values_trace', results['critical_values_trace']
    print 'trace_statistic', results['trace_statistic']
    print 'critical_values_max_eigenvalue', results['critical_values_max_eigenvalue']
    print 'eigenvalue_statistics', results['eigenvalue_statistics']
    cointeg_vector = results['cointegration_vectors']
    print
    print '------', results
    print '------', cointeg_vector
    signal = numpy.dot(quandl_data.as_matrix(), cointeg_vector)[:, 0]
    signal_df = pandas.DataFrame({'signal': signal}, index=quandl_data.index)
    signal_df.plot()
    print '--- non-stationarity test', cointeg.is_not_stationary(signal, significance='1%')
    quandl_data.plot(kind='scatter', x='GOOG.NYSE_EWA - Close', y='GOOG.NYSE_EWC - Close')
    regression = ols(y=quandl_data['GOOG.NYSE_EWA - Close'], x=quandl_data[['GOOG.NYSE_EWC - Close']])
    print regression
    hedge_ratio = regression.beta[0]
    portfolio = quandl_data['GOOG.NYSE_EWA - Close'] - hedge_ratio * quandl_data['GOOG.NYSE_EWC - Close']
    print portfolio
    pandas.DataFrame(portfolio, columns=['signal2']).plot()
    pyplot.show()
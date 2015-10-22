import logging
import os
import numpy
import pandas
from statsext import cointeg
from matplotlib import pyplot

from mktdata import load_prices

__author__ = 'Christophe'

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(name)s - %(message)s', level=logging.DEBUG)
    codes = ['GOOG/KO', 'GOOG/PEP']
    quandl_data = load_prices(codes, start_date='2000-01-01', end_date='2015-12-31')
    print quandl_data
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
    print cointeg.is_not_stationary(signal)
    pyplot.show()
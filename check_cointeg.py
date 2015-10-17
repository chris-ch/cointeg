from urllib import quote_plus
import logging
import os
import numpy
import pandas
import Quandl
from statsext import cointeg
from matplotlib import pyplot

__author__ = 'Christophe'

_QUANDL_TOKEN = 'UvpzeSNabDxs3DKxggsi'  # Christophe private
_CACHE_LOCATION = '.quandl_cache'


def load_quandl(datasets, trim_start=None):
    if trim_start:
        return Quandl.get(datasets, trim_start=trim_start, authtoken=_QUANDL_TOKEN).dropna()

    else:
        return Quandl.get(datasets, authtoken=_QUANDL_TOKEN).dropna()


def load_cache(datasets, trim_start=None):
    if not os.path.isdir(os.path.abspath(_CACHE_LOCATION)):
        os.makedirs(os.path.abspath(_CACHE_LOCATION))

    SUFFIX_CLOSE_ADJ = '11'
    datasets = ['.'.join([dataset, SUFFIX_CLOSE_ADJ]) for dataset in datasets]
    datasets_encoded = quote_plus('@'.join(datasets + ['trim_start=%s' % trim_start]))
    cache_path = os.path.abspath(os.sep.join([_CACHE_LOCATION, datasets_encoded]))
    if os.path.isfile(cache_path):
        logging.info('reading datasets from local cache')
        quandl_df = pandas.read_pickle(cache_path)

    else:
        quandl_df = load_quandl(datasets, trim_start=trim_start)
        quandl_df.to_pickle(cache_path)

    return quandl_df

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(name)s - %(message)s', level=logging.DEBUG)
    codes = ['WIKI/KO', 'WIKI/PEP']
    quandl_data = load_cache(codes, trim_start='2000-01-01')
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
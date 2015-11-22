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
from pnl import AverageCostProfitAndLoss
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
    """

    """

    def __init__(self, training_set):
        training_set = training_set.ffill()
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

    def compute_signal(self, input_ts, start_date=None, end_date=None, name='signal'):
        """

        :param input_ts:
        :param start_date: starting time range of input, included
        :param end_date: ending time range of input, excluded
        :param name:
        :return:
        """
        input_ts_filter = input_ts.index >= '1980-01-01'  # hack
        if start_date is not None:
            input_ts_filter &= input_ts.index >= start_date

        if end_date is not None:
            input_ts_filter &= input_ts.index < end_date

        by_day = input_ts[input_ts_filter].groupby([(pandas.TimeGrouper('D'))]).ffill()
        return pandas.DataFrame(by_day.dot(self.vector), columns=[name]).dropna()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(name)s - %(message)s', level=logging.DEBUG)
    logging.info('loading datasets...')

    SECURITIES = ['HYG', 'JNK']
    TRADE_SCALE = 100.

    prices_bid_ask_list = list()
    prices_mid_list = list()
    for security in SECURITIES:
        quote = pandas.read_pickle(os.sep.join(['data', '%s.pkl' % security])).astype(float)
        prices_bid_ask_list.append(quote)
        quote_mid = 0.5 * (quote['bid'] + quote['ask'])
        prices_mid_list.append(quote_mid)

    logging.info('loaded datasets')
    prices_mid = pandas.concat(prices_mid_list, axis=1)
    prices_mid.columns = SECURITIES
    logging.info('computing cointegration statistics')
    cointegration = CoIntegration((prices_mid[(prices_mid.index >= '2015-04-01') & (prices_mid.index <= '2015-04-30')]))
    logging.info('half-life according to warm-up period: %d', cointegration.half_life)
    signal = cointegration.compute_signal(prices_mid, start_date='2015-05-13', end_date='2015-05-15', name='signal')

    # fig, x_prices = pyplot.subplots()
    # x_prices.xaxis.set_major_formatter(formatter)
    # df.plot(ax=x_prices, x=numpy.arange(len(df)), subplots=True)
    logging.info('computing ewma')
    signal['ewma'] = pandas.ewma(signal['signal'], halflife=cointegration.half_life)

    threshold = 0.8 * cointegration.calibration['signal'].std()
    logging.info('size of threshold: %.2f', threshold)
    cumul = {'current_scaling': 0.}

    def compute_scale(row, cumul=cumul):
        current_scaling = cumul['current_scaling']
        signal_level = row['signal']
        ewma = row['ewma']
        new_position_scaling = get_position_scaling(signal_level, current_scaling, ewma, threshold)
        # updating for next step
        cumul['current_scaling'] = new_position_scaling
        result = {
            'band_inf': ewma + ((new_position_scaling - 1) * threshold),
            'band_mid': ewma + (new_position_scaling * threshold),
            'band_sup': ewma + ((new_position_scaling + 1) * threshold),
            'scaling': new_position_scaling
        }
        return pandas.Series(result)

    scales = signal.apply(compute_scale, axis=1)['scaling']
    shares = (scales.values * cointegration.vector[:, None] * TRADE_SCALE).astype(int)
    shares_df = pandas.DataFrame(shares.transpose(), index=[scales.index], columns=SECURITIES)
    components = list()
    for count, security in enumerate(SECURITIES):
        prices = pandas.concat([prices_bid_ask_list[count]['bid'], prices_bid_ask_list[count]['ask']], axis=1)
        component = pandas.concat([prices, shares_df[security]], axis=1, join='inner')
        component.columns = ['bid', 'ask', 'shares']
        components.append(component)

    components_trades = list()
    for count, component in enumerate(components):
        trades = component[['shares']].diff()
        trades.ix[0] = component['shares'].ix[0]
        trades['cost'] = component['bid'].where(component['shares'] < 0, component['ask'])
        trades = trades[trades['shares'] != 0]

        pnl_calc = AverageCostProfitAndLoss()

        def update_pnl(row, pnl_calc=pnl_calc):
            pnl_calc.add_fill(fill_qty=row['shares'], fill_price=row['cost'])
            results = {'realized': pnl_calc.realized_pnl,
                       'unrealized': pnl_calc.get_unrealized_pnl(current_price=row['cost'])}
            return pandas.Series(results)

        trades = pandas.concat([trades, trades.apply(update_pnl, axis=1)], axis=1)
        components_trades.append(trades)

    for count, trades in enumerate(components_trades):
        print(SECURITIES[count])
        print(trades.round(2))

        # logging.info('writing results to output file')
        # writer = pandas.ExcelWriter('signal.test.xlsx', engine='xlsxwriter')
        # signal.to_excel(writer, 'Sheet1')
        # writer.save()
        # fig, ax_signal = pyplot.subplots()
        # formatter = IrregularDatetimeFormatter(signal.index.values)
        # ax_signal.xaxis.set_major_formatter(formatter)
        # signal.plot(ax=ax_signal, x=numpy.arange(len(signal)))
        # pyplot.show()

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


def save_sample(ticker):
    ticker = ticker.upper()
    for available_ticker in list_tickers('equities'):
        logging.info('available data for %s, %s', available_ticker, get_date_range(available_ticker, 'equities'))

    nyse_arca = LoaderARCA()
    start_date = datetime(2015, 4, 1)
    end_date = datetime(2015, 5, 31)
    book_states = nyse_arca.load_book_states('%s US Equity' % ticker, start_date, end_date)
    book_states.to_pickle('%s.pkl' % ticker)


def save_samples(ticker1, ticker2):
    ticker1 = ticker1.upper()
    ticker2 = ticker2.upper()
    for ticker in list_tickers('equities'):
        logging.info('available data for %s, %s', ticker, get_date_range(ticker, 'equities'))

    nyse_arca = LoaderARCA()
    start_date = datetime(2015, 4, 1)
    end_date = datetime(2015, 5, 31)
    book_states = nyse_arca.load_book_states('%s US Equity' % ticker1, start_date, end_date)
    book_states.to_pickle('%s.pkl' % ticker1)
    book_states = nyse_arca.load_book_states('%s US Equity' % ticker2, start_date, end_date)
    book_states.to_pickle('%s.pkl' % ticker2)


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
    Generates a cointegrated signal.
    """

    def __init__(self, prices, calibration_start, calibration_end, backtest_end):
        calibration_period = (prices.index >= calibration_start) & (prices.index < calibration_end)
        backtest_period = (prices.index >= calibration_end) & (prices.index < backtest_end)
        calibration_set = prices[calibration_period].groupby([(pandas.TimeGrouper('D'))]).ffill().dropna(axis=0)
        self._backtest_set = prices[backtest_period]
        self._signal = None
        cointeg_vectors = cointeg.get_johansen(calibration_set, lag=1)
        if len(cointeg_vectors) > 0:
            self._vector = cointeg_vectors[0]
            self._calibration = pandas.DataFrame(calibration_set.dot(self._vector))
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

    @property
    def signal(self):
        if self._signal is None:
            self._signal = self._compute_signal(self._backtest_set)

        return self._signal

    def _compute_signal(self, input_ts):
        """

        :param input_ts:
        :return:
        """
        by_day = input_ts.groupby([(pandas.TimeGrouper('D'))]).ffill()
        return by_day.dot(self.vector).dropna()


def compute_trades(component):
    trades = component[['shares']].diff()
    trades.ix[0] = component['shares'].ix[0]
    trades['cost'] = component['bid'].where(component['shares'] < 0, component['ask'])
    #trades = trades[trades['shares'] != 0]
    update_pnl_globals = {'pnl_calc': AverageCostProfitAndLoss()}

    def update_pnl(row):
        pnl_calc = update_pnl_globals['pnl_calc']
        realized_pnl = 0.
        if row['shares'] != 0:
            pnl_calc.add_fill(fill_qty=row['shares'], fill_price=row['cost'])
            realized_pnl = pnl_calc.realized_pnl

        unrealized_pnl = pnl_calc.get_unrealized_pnl(current_price=row['cost'])
        return pandas.Series({'trade_realized': realized_pnl, 'unrealized': unrealized_pnl})

    result = list()
    for row in trades.itertuples(index=False):
        result.append(update_pnl(dict(zip(trades.columns, row))))

    trades = pandas.concat([trades, pandas.DataFrame(result, index=trades.index)], axis=1)
    trades['realized'] = trades['trade_realized'].cumsum()
    return trades[['realized', 'unrealized']]


def backtest(prices_mid_securities, calibration_start, calibration_end, backtest_end):
    securities = prices_mid_securities.keys()
    prices_mid_list = [prices_mid_securities[security] for security in securities]
    prices_mid = pandas.concat(prices_mid_list, axis=1)
    prices_mid.columns = securities
    logging.info('computing cointegration statistics')
    cointegration = CoIntegration(prices_mid, calibration_start, calibration_end, backtest_end)
    logging.info('half-life according to warm-up period: %d', cointegration.half_life)
    return cointegration


def bollinger(signal, threshold, half_life=None, ref_value=0.):
    """

    :param signal:
    :param threshold:
    :param half_life:
    :param ref_value:
    :return:
    """
    logging.info('computing ewma')
    if half_life:
        signal_ref = pandas.ewma(signal, halflife=half_life)

    else:
        ref = numpy.empty(len(signal))
        ref.fill(ref_value)
        signal_ref = pandas.Series(ref, index=signal.index)

    compute_scale_globals = {'current_scaling': 0.}

    def compute_scale(signal_level, ewma_level):
        current_scaling = compute_scale_globals['current_scaling']
        new_position_scaling = get_position_scaling(signal_level, current_scaling, ewma_level, threshold)
        # updating for next step
        compute_scale_globals['current_scaling'] = new_position_scaling
        result = {
            'band_inf': ewma_level + ((new_position_scaling - 1) * threshold),
            'band_mid': ewma_level + (new_position_scaling * threshold),
            'band_sup': ewma_level + ((new_position_scaling + 1) * threshold),
            'scaling': -new_position_scaling
        }
        return result

    logging.info('computing scaling')

    result = list()
    for signal_level, ewma_level in pandas.concat([signal, signal_ref], axis=1).itertuples(index=False):
        result.append(compute_scale(signal_level, ewma_level))

    bands = pandas.DataFrame(result, index=signal.index)
    return bands[['band_inf', 'band_mid', 'band_sup']], bands['scaling']


def main():
    logging.info('loading datasets...')

    SECURITIES = ['EWA', 'EWC', 'GDX']
    TRADE_SCALE = 100.  # how many spreads to trades at a time
    STEP_SIZE = 2.  # variation that triggers a trade in terms of std dev
    EWMA_PERIOD = 2.  # length of EWMA in terms of cointegration half-life

    CALIBRATION_START = '2015-04-01'  # included
    CALIBRATION_END = '2015-05-01'  # excluded
    BACKTEST_END = '2015-06-01'  # excluded

    prices_bid_ask_securities = dict()
    prices_mid_securities = dict()
    for security in SECURITIES:
        quote = pandas.read_pickle(os.sep.join(['data', '%s.pkl' % security])).astype(float)
        prices_bid_ask_securities[security] = quote
        quote_mid = 0.5 * (quote['bid'] + quote['ask'])
        prices_mid_securities[security] = quote_mid

    logging.info('loaded datasets')
    cointegration = backtest(prices_mid_securities, CALIBRATION_START, CALIBRATION_END, BACKTEST_END)

    #signal.resample('10T', how='last')

    threshold = STEP_SIZE * cointegration.calibration['signal'].std()
    logging.info('size of threshold: %.2f', threshold)

    #bands, scaling = bollinger(cointegration.signal, threshold, half_life=EWMA_PERIOD * cointegration.half_life)
    bands, scaling = bollinger(cointegration.signal, threshold, ref_value=cointegration.calibration['signal'].mean())

    shares = (scaling.values * cointegration.vector[:, None] * TRADE_SCALE).astype(int)
    shares_df = pandas.DataFrame(shares.transpose(), index=[scaling.index], columns=SECURITIES)

    pnl_securities = dict()
    for security in SECURITIES:
        logging.info('backtesting component: %s', security)
        prices = pandas.concat([prices_bid_ask_securities[security]['bid'], prices_bid_ask_securities[security]['ask']], axis=1)
        component = pandas.concat([prices, shares_df[security]], axis=1, join='inner')
        component.columns = ['bid', 'ask', 'shares']
        logging.info('analyzing trades for component: %s', security)
        trades = compute_trades(component)
        logging.info('displaying results for component %s', security)
        logging.info('trades:\n%s', trades)
        pnl_securities[security] = trades['realized'] + trades['unrealized']

    fig, ax_pnls = pyplot.subplots()
    pnls = pandas.DataFrame(pandas.concat([trades for trades in pnl_securities.values()], axis=1, join='inner').sum(axis=1))
    formatter = IrregularDatetimeFormatter(pnls.index.values)
    ax_pnls.xaxis.set_major_formatter(formatter)
    pnls.plot(ax=ax_pnls, x=numpy.arange(len(pnls)))

    # logging.info('writing results to output file')
    # writer = pandas.ExcelWriter('signal.test.xlsx', engine='xlsxwriter')
    # signal.to_excel(writer, 'Sheet1')
    # writer.save()
    fig, ax_signal = pyplot.subplots()
    formatter = IrregularDatetimeFormatter(cointegration.signal.index.values)
    ax_signal.xaxis.set_major_formatter(formatter)
    pandas.concat([cointegration.signal, bands], axis=1, join='inner').plot(ax=ax_signal, x=numpy.arange(len(cointegration.signal)))
    pyplot.show()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(name)s - %(message)s', level=logging.DEBUG)
    main()
    #save_sample('USO')

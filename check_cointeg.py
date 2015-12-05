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


def compute_trades(components, securities):
    components_trades = list()
    for count, component in enumerate(components):
        logging.info('analyzing trades for component: %s', securities[count])
        trades = component[['shares']].diff()
        trades.ix[0] = component['shares'].ix[0]
        trades['cost'] = component['bid'].where(component['shares'] < 0, component['ask'])
        trades = trades[trades['shares'] != 0]
        update_pnl_globals = {'pnl_calc': AverageCostProfitAndLoss()}

        def update_pnl(row):
            pnl_calc = update_pnl_globals['pnl_calc']
            pnl_calc.add_fill(fill_qty=row['shares'], fill_price=row['cost'])
            results = {'realized': pnl_calc.realized_pnl,
                       'unrealized': pnl_calc.get_unrealized_pnl(current_price=row['cost'])}
            return pandas.Series(results)

        result = list()
        for row in trades.itertuples(index=False):
            result.append(update_pnl(dict(zip(trades.columns, row))))

        trades = pandas.concat([trades, pandas.DataFrame(result, index=trades.index)], axis=1)
        components_trades.append(trades)

    return components_trades


def backtest(prices_mid_securities, calibration_start, calibration_end, backtest_end):
    securities = prices_mid_securities.keys()
    prices_mid_list = [prices_mid_securities[security] for security in securities]
    prices_mid = pandas.concat(prices_mid_list, axis=1)
    prices_mid.columns = securities
    logging.info('computing cointegration statistics')
    cointegration = CoIntegration(prices_mid, calibration_start, calibration_end, backtest_end)
    logging.info('half-life according to warm-up period: %d', cointegration.half_life)
    return cointegration


def main():
    logging.info('loading datasets...')

    SECURITIES = ['EWA', 'EWC']
    TRADE_SCALE = 100.  # how many spreads to trades at a time
    STEP_SIZE = 0.95  # variation that triggers a trade in terms of std dev
    EWMA_PERIOD = 2.  # length of EWMA in terms of cointegration half-life

    CALIBRATION_START = '2015-04-01'  # included
    CALIBRATION_END = '2015-05-01'  # excluded
    BACKTEST_END = '2015-06-01'  # excluded

    prices_bid_ask_list = list()
    prices_mid_securities = dict()
    for security in SECURITIES:
        quote = pandas.read_pickle(os.sep.join(['data', '%s.pkl' % security])).astype(float)
        prices_bid_ask_list.append(quote)
        quote_mid = 0.5 * (quote['bid'] + quote['ask'])
        prices_mid_securities[security] = quote_mid

    logging.info('loaded datasets')
    cointegration = backtest(prices_mid_securities, CALIBRATION_START, CALIBRATION_END, BACKTEST_END)

    #signal.resample('10T', how='last')

    logging.info('computing ewma')
    signal_ewma = pandas.ewma(cointegration.signal, halflife=EWMA_PERIOD * cointegration.half_life)

    threshold = STEP_SIZE * cointegration.calibration['signal'].std()
    logging.info('size of threshold: %.2f', threshold)
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
            'scaling': new_position_scaling
        }
        return result

    logging.info('computing scaling')

    result = list()

    for signal_level, ewma_level in pandas.concat([cointegration.signal, signal_ewma], axis=1).itertuples(index=False):
        result.append(compute_scale(signal_level, ewma_level))

    bands = pandas.DataFrame(result, index=cointegration.signal.index)
    scales = bands['scaling']
    shares = (scales.values * cointegration.vector[:, None] * TRADE_SCALE).astype(int)
    shares_df = pandas.DataFrame(shares.transpose(), index=[scales.index], columns=SECURITIES)

    components = list()
    for count, security in enumerate(SECURITIES):
        logging.info('backtesting component: %s', security)
        prices = pandas.concat([prices_bid_ask_list[count]['bid'], prices_bid_ask_list[count]['ask']], axis=1)
        component = pandas.concat([prices, shares_df[security]], axis=1, join='inner')
        component.columns = ['bid', 'ask', 'shares']
        components.append(component)

    components_trades = compute_trades(components, SECURITIES)

    for count, trades in enumerate(components_trades):
        logging.info('displaying results for component %s', SECURITIES[count])
        # join with prices and cumulate
        logging.info('trades:\n%s', trades)

    # logging.info('writing results to output file')
    # writer = pandas.ExcelWriter('signal.test.xlsx', engine='xlsxwriter')
    # signal.to_excel(writer, 'Sheet1')
    # writer.save()
    fig, ax_signal = pyplot.subplots()
    formatter = IrregularDatetimeFormatter(cointegration.signal.index.values)
    ax_signal.xaxis.set_major_formatter(formatter)
    pandas.concat([cointegration.signal, bands.drop(['scaling'], axis=1)], axis=1, join='inner').plot(ax=ax_signal, x=numpy.arange(len(cointegration.signal)))
    pyplot.show()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-15s %(levelname)s %(name)s - %(message)s', level=logging.DEBUG)
    main()
    #save_samples('EWA', 'EWC')

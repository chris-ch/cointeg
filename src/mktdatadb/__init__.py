from collections import OrderedDict
from decimal import Decimal
import glob
import logging
import os
from urllib.parse import quote, unquote
from zipfile import ZipFile
from datetime import timedelta, datetime
import itertools
import pandas
import pytz

__author__ = 'Christophe'

ON_TIME_NYSEARCA = '093000'
OFF_TIME_NYSEARCA = '160000'
TZ_NYSEARCA = 'US/Eastern'


def _date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def _get_db_path(db_name):
    return os.sep.join(['G:', 'mktdata', db_name])


def _get_file_path(ticker, db_name):
    encoded_ticker = quote(ticker)
    file_path = os.sep.join([_get_db_path(db_name), encoded_ticker + '.zip'])
    return file_path


def _ticks_from_zip(ticker, start_time, end_time, db_name, pattern='BEST'):
    """

    :param ticker:
    :param start_time: start time (UTC)
    :param end_time: end time (UTC)
    :param pattern: 'BEST' for bid-ask, 'TRADE' for trades
    :return:
    """
    file_path = _get_file_path(ticker, db_name)
    start_date = start_time.date()
    end_date = end_time.date()
    with ZipFile(file_path, 'r') as zip_ticks:
        logging.info('loading data from zip file %s', file_path)
        files_list = zip_ticks.namelist()
        for current_date in _date_range(start_date, end_date):
            current_file = current_date.strftime('%Y%m%d') + '.csv'
            if current_file not in files_list:
                logging.warning('source file %s not found: ignoring', current_file)
                continue

            with zip_ticks.open(current_file, mode='r') as ticks_file:
                for line in ticks_file:
                    line = line.decode('UTF-8')
                    if pattern in line:
                        parsed = line.strip().split(',')
                        yield parsed


def ticks_trades(ticker, start_time, end_time, db_name='equities'):
    trades = _ticks_from_zip(ticker, start_time, end_time, db_name, pattern='TRADE')
    for trade in trades:
        yield trade[0], Decimal(trade[2]), int(Decimal(trade[3])), trade[4]


def _pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def _ticks_quotes(ticker, start_time, end_time, db_name='equities'):
    """

    :param ticker:
    :param start_time:
    :param end_time:
    :return: tuple (timestamp, type_bid_ask, price, quantity)
    """
    quotes = _ticks_from_zip(ticker, start_time, end_time, db_name, pattern='BEST')
    current_bid_second = None
    current_ask_second = None
    for mkt_quote, mkt_quote_next in _pairwise(quotes):
        if mkt_quote[1] == 'BEST_BID':
            current_bid_second = mkt_quote[0], mkt_quote[1], Decimal(mkt_quote[2]), int(Decimal(mkt_quote[3]))

        if mkt_quote[1] == 'BEST_ASK':
            current_ask_second = mkt_quote[0], mkt_quote[1], Decimal(mkt_quote[2]), int(Decimal(mkt_quote[3]))

        if mkt_quote_next is None or mkt_quote[0] != mkt_quote_next[0]:
            # current entry is the last for current second
            if current_bid_second is not None:
                yield current_bid_second

            if current_ask_second is not None:
                yield current_ask_second

            current_bid_second = None
            current_ask_second = None


def _time_filter(ticks_data, start_time_local_str, end_time_local_str, timezone_local):
    """

    :param ticks_data:
    :param start_time_local_str: start time as a string ('%H%M%S')
    :param end_time_local_str: end time as a string ('%H%M%S')
    :param timezone_local:
    :return:
    """
    for tick_data in ticks_data:
        tick_year_utc, tick_month_utc, tick_day_utc = [int(value) for value in tick_data[0][:10].split('-')]
        tick_hour_utc, tick_minute_utc, tick_second_utc = [int(value) for value in tick_data[0][11:19].split(':')]
        tick_datetime_utc = datetime(tick_year_utc, tick_month_utc, tick_day_utc, tick_hour_utc, tick_minute_utc,
                                     tick_second_utc, tzinfo=pytz.UTC)
        tick_datetime_local = tick_datetime_utc.astimezone(timezone_local)
        tick_datetime_local_str = tick_datetime_local.strftime('%H%M%S')
        if start_time_local_str <= tick_datetime_local_str < end_time_local_str:
            yield tick_data


def load_tick_data(ticker, start_datetime, end_datetime, market_on_time, market_off_time, market_timezone):
    """

    :param ticker:
    :param start_datetime:
    :param end_datetime:
    :param market_on_time: string representing trading start time ('HHMMSS')
    :param market_off_time: string representing trading end time ('HHMMSS')
    :param market_timezone:
    :return:
    """
    ticks_data = _ticks_quotes(ticker, start_datetime, end_datetime)
    return _time_filter(ticks_data, market_on_time, market_off_time, pytz.timezone(market_timezone))


def load_book_states(ticker, start_datetime, end_datetime, market_on_time, market_off_time, market_timezone):
    book_state = OrderedDict()
    book_state['ts'] = None
    book_state['v_bid'] = None
    book_state['bid'] = None
    book_state['ask'] = None
    book_state['v_ask'] = None
    ticks_data = load_tick_data(ticker, start_datetime, end_datetime, market_on_time, market_off_time, market_timezone)
    for tick_quote in ticks_data:
        if tick_quote[1] == 'BEST_BID':
            book_state['ts'] = tick_quote[0]
            book_state['bid'] = tick_quote[2]
            book_state['v_bid'] = tick_quote[3]

        else:
            book_state['ts'] = tick_quote[0]
            book_state['ask'] = tick_quote[2]
            book_state['v_ask'] = tick_quote[3]

        yield book_state.copy()


def list_tickers(db_name):
    tickers = list()
    for filename in glob.glob(os.sep.join([_get_db_path(db_name), '*.zip'])):
        ticker = unquote(os.path.basename(filename).split('.')[0])
        tickers.append(ticker)

    return sorted(tickers)


def get_date_range(ticker, db_name):
    file_path = _get_file_path(ticker, db_name)
    with ZipFile(file_path, 'r') as zip_ticks:
        logging.debug('loading data from zip file %s', file_path)
        files_list = sorted(zip_ticks.namelist())

    start_date = files_list[0][:-4]
    end_date = files_list[-1][:-4]
    start_date_split = '%s-%s-%s' % (start_date[:4], start_date[4:6], start_date[6:8])
    end_date_split = '%s-%s-%s' % (end_date[:4], end_date[4:6], end_date[6:8])
    return start_date_split, end_date_split


class LoaderARCA(object):
    """
    Loading equities from NYSE ARCA.
    """

    def __init__(self):
        self._on_time = ON_TIME_NYSEARCA
        self._off_time = OFF_TIME_NYSEARCA
        self._timezone = TZ_NYSEARCA
        self._db_name = 'equities'

    def list_tickers(self):
        return list_tickers(self._db_name)

    def load_book_states(self, ticker, start_date=None, end_date=None):
        full_start_date, full_end_date = get_date_range(ticker, self._db_name)
        if start_date is None:
            start_date = datetime.strptime(full_start_date, '%Y-%m-%d')

        if end_date is None:
            end_date = datetime.strptime(full_end_date, '%Y-%m-%d')

        logging.info('loading %s for date range: %s through %s', ticker, start_date, end_date)
        book_states = load_book_states(ticker, start_date, end_date, self._on_time, self._off_time, self._timezone)
        df = pandas.DataFrame.from_dict(list(book_states))
        df['ts'] = pandas.to_datetime(df['ts'])
        df.drop_duplicates(subset='ts', keep='last', inplace=True)
        df.set_index('ts', inplace=True)
        return df

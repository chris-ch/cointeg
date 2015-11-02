from collections import OrderedDict
from decimal import Decimal
import logging
import os
from urllib import quote
from zipfile import ZipFile
from datetime import timedelta, datetime
import itertools
import pytz

__author__ = 'Christophe'


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def ticks_from_zip(ticker, start_time, end_time, pattern='BEST'):
    """

    :param ticker:
    :param start_time: start time (UTC)
    :param end_time: end time (UTC)
    :param pattern: 'BEST' for bid-ask, 'TRADE' for trades
    :return:
    """
    encoded_ticker = quote(ticker)
    file_path = os.sep.join(['G:', 'mktdata', 'equities', encoded_ticker + '.zip'])
    start_date = start_time.date()
    end_date = end_time.date()
    with ZipFile(file_path, 'r') as zip_ticks:
        logging.info('loading data from zip file %s', file_path)
        files_list = zip_ticks.namelist()
        for current_date in date_range(start_date, end_date):
            current_file = current_date.strftime('%Y%m%d') + '.csv'
            if current_file not in files_list:
                logging.warn('source file %s not found: ignoring', current_file)
                continue

            with zip_ticks.open(current_file, 'r') as ticks_file:
                for line in ticks_file:
                    if pattern in line:
                        parsed = line.strip().split(',')
                        yield parsed


def ticks_trades(ticker, start_time, end_time, ticks_loader=None):
    if ticks_loader is None:
        ticks_loader = ticks_from_zip
        
    trades = ticks_loader(ticker, start_time, end_time, pattern='TRADE')
    for trade in trades:
        yield trade[0], Decimal(trade[2]), int(Decimal(trade[3])), trade[4]
        

def pairwise(itr):
   first, second = itertools.tee(itr)
   second.next() # remove first element of second
   second = itertools.chain(second, [None]) # add final None
   return itertools.izip(first, second)


def ticks_quotes(ticker, start_time, end_time):
    quotes = ticks_from_zip(ticker, start_time, end_time, pattern='BEST')
    current_bid_second = None
    current_ask_second = None
    for mkt_quote, mkt_quote_next in pairwise(quotes):
        if  mkt_quote[1] == 'BEST_BID':
            current_bid_second = mkt_quote[0], mkt_quote[1], Decimal(mkt_quote[2]), int(Decimal(mkt_quote[3]))
        
        if  mkt_quote[1] == 'BEST_ASK':
            current_ask_second = mkt_quote[0], mkt_quote[1], Decimal(mkt_quote[2]), int(Decimal(mkt_quote[3]))
        
        if mkt_quote_next is None or mkt_quote[0] != mkt_quote_next[0]:
            # current entry is the last for current second
            if current_bid_second is not None:
                yield current_bid_second
                
            if current_ask_second is not None:
                yield current_ask_second
                
            current_bid_second = None
            current_ask_second = None


def ticks_book_states(ticker, start_time, end_time):
    book_state = OrderedDict()
    book_state['ts'] = None
    book_state['v_bid'] = None
    book_state['bid'] = None
    book_state['ask'] = None
    book_state['v_ask'] = None
    for tick_quote in ticks_quotes(ticker, start_time, end_time):
        if tick_quote[1] == 'BEST_BID':
            book_state['ts'] = tick_quote[0]
            book_state['bid'] = tick_quote[2]
            book_state['v_bid'] = tick_quote[3]

        else:
            book_state['ts'] = tick_quote[0]
            book_state['ask'] = tick_quote[2]
            book_state['v_ask'] = tick_quote[3]

        yield book_state


def time_filter(ticks_data, start_time_local_str, end_time_local_str, timezone_local):
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
        if tick_datetime_local_str >= start_time_local_str and tick_datetime_local_str < end_time_local_str:
            yield tick_data

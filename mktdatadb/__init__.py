from decimal import Decimal
import logging
import os
from urllib import quote
from zipfile import ZipFile
from datetime import timedelta, datetime
from itertools import islice, izip
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


def ticks_quotes(ticker, start_time, end_time, ticks_loader=None):
    if ticks_loader is None:
        ticks_loader = ticks_from_zip
        
    quotes = ticks_loader(ticker, start_time, end_time, pattern='BEST')
    for mkt_quote, mkt_quote_next in izip(quotes, islice(quotes, 1, None)):
        if mkt_quote[0] != mkt_quote_next[0]:
            # TODO: returning last from second, test this
            yield mkt_quote[0], Decimal(mkt_quote[2]), int(Decimal(mkt_quote[3]))


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
        if tick_datetime_local_str >= start_time_local_str and tick_datetime_local_str <= end_time_local_str:
            yield tick_data

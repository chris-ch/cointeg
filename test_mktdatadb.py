from datetime import datetime

import pandas
import pytz

from src.mktdatadb import ticks_trades, time_filter


if __name__ == '__main__':
    start_time = datetime(2015, 7, 1, 17, 0)
    end_time = datetime(2015, 7, 2, 21, 0)
    ticks_data = ticks_trades('HYG US Equity', start_time, end_time)
    filtered_ticks_data = time_filter(ticks_data, '093000', '160000', pytz.timezone('US/Eastern'))
    df = pandas.DataFrame(filtered_ticks_data, columns=['date', 'price', 'qty', 'type'])
    df.set_index('date', inplace=True)
    print df.head()
    print df.tail()

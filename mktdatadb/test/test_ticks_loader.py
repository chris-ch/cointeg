import unittest
import logging
import os
import pytz
from datetime import datetime
from decimal import Decimal

from mktdatadb import ticks_quotes, time_filter

class TestTicksLoader(unittest.TestCase):

  def test_quotes(self):
      start_time = datetime(2015, 3, 2, 0, 0)
      end_time = datetime(2015, 3, 2, 23, 59)
      
      def load_test(ticker, start_time, end_time, pattern):
          path_to_test_data = os.path.dirname(os.path.realpath(__file__))
          test_data_filename = os.sep.join([path_to_test_data, 'testdata', 'HYG-20150302.csv'])
          logging.info('loading test data file %s', test_data_filename)
          with open(test_data_filename, 'r') as test_data:
              for line in test_data.xreadlines():
                if pattern in line:
                  parsed = line.strip().split(',')
                  yield parsed
      
      expected = [
        ('2015-03-02 14:30:00.000000', 'BEST_BID', Decimal('89.8952'), 1),
        ('2015-03-02 14:30:00.000000', 'BEST_ASK', Decimal('89.9934'), 14),
        ('2015-03-02 14:30:01.000000', 'BEST_ASK', Decimal('89.9934'), 16),
        ('2015-03-02 14:30:02.000000', 'BEST_BID', Decimal('89.9246'), 2),
        ('2015-03-02 14:30:02.000000', 'BEST_ASK', Decimal('89.9737'), 12),
        ('2015-03-02 14:30:03.000000', 'BEST_BID', Decimal('89.9246'), 3),
        ('2015-03-02 14:30:03.000000', 'BEST_ASK', Decimal('89.9737'), 11),
        ('2015-03-02 14:30:04.000000', 'BEST_BID', Decimal('89.9246'), 3),
        ('2015-03-02 14:30:04.000000', 'BEST_ASK', Decimal('89.9737'), 111),
        ('2015-03-02 14:30:05.000000', 'BEST_ASK', Decimal('89.9737'), 11),
        ('2015-03-02 14:30:08.000000', 'BEST_BID', Decimal('89.9246'), 2),
        ('2015-03-02 14:30:08.000000', 'BEST_ASK', Decimal('89.9639'), 12),
        ('2015-03-02 14:30:09.000000', 'BEST_BID', Decimal('89.8853'), 4),
        ('2015-03-02 14:30:09.000000', 'BEST_ASK', Decimal('89.9737'), 11),
        ('2015-03-02 14:30:11.000000', 'BEST_ASK', Decimal('89.9737'), 11),
        ('2015-03-02 14:30:13.000000', 'BEST_ASK', Decimal('89.9737'), 23),
        ('2015-03-02 14:30:16.000000', 'BEST_BID', Decimal('89.8853'), 3),
        ('2015-03-02 14:30:32.000000', 'BEST_ASK', Decimal('89.9737'), 25),
        ('2015-03-02 14:30:34.000000', 'BEST_BID', Decimal('89.905'), 1),
        ('2015-03-02 14:30:39.000000', 'BEST_BID', Decimal('89.905'), 3),
        ('2015-03-02 14:30:40.000000', 'BEST_BID', Decimal('89.905'), 2),
        ('2015-03-02 14:30:42.000000', 'BEST_BID', Decimal('89.905'), 3),
        ('2015-03-02 14:30:43.000000', 'BEST_ASK', Decimal('89.9737'), 28),
        ('2015-03-02 14:30:50.000000', 'BEST_BID', Decimal('89.8853'), 14),
        ('2015-03-02 14:30:50.000000', 'BEST_ASK', Decimal('89.9345'), 9),
        ('2015-03-02 14:30:54.000000', 'BEST_ASK', Decimal('89.9345'), 14),
        ('2015-03-02 14:30:59.000000', 'BEST_ASK', Decimal('89.9345'), 16),
        ]
      
      ticks_data = ticks_quotes('HYG US Equity', start_time, end_time, ticks_loader=load_test)
      filtered_ticks_data = time_filter(ticks_data, '093000', '093100', pytz.timezone('US/Eastern'))
      self.maxDiff = None
      self.assertEqual(expected, list(filtered_ticks_data))
      
if __name__ == '__main__':
    unittest.main()
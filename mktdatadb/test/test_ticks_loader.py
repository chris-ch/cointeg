import unittest
import logging
import os
import pytz
from datetime import datetime
from decimal import Decimal
import gzip

from mktdatadb import ticks_quotes, time_filter

class TestTicksLoader(unittest.TestCase):

  def test_quotes(self):
      start_time = datetime(2015, 3, 2, 0, 0)
      end_time = datetime(2015, 3, 2, 23, 59)
      
      def load_test(ticker, start_time, end_time, pattern):
          path_to_test_data = os.path.dirname(os.path.realpath(__file__))
          test_data_filename = os.sep.join([path_to_test_data, 'testdata', 'HYG-20150302.csv.gz'])
          logging.info('loading test data file %s', test_data_filename)
          with gzip.open(test_data_filename, 'r') as test_data:
              for line in test_data.readlines():
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
      
  def test_quotes_dst(self):
      start_time = datetime(2015, 4, 2, 0, 0)
      end_time = datetime(2015, 4, 2, 23, 59)
      
      def load_test(ticker, start_time, end_time, pattern):
          path_to_test_data = os.path.dirname(os.path.realpath(__file__))
          test_data_filename = os.sep.join([path_to_test_data, 'testdata', 'HYG-20150402.csv.gz'])
          logging.info('loading test data file %s', test_data_filename)
          with gzip.open(test_data_filename, 'r') as test_data:
              for line in test_data.readlines():
                if pattern in line:
                  parsed = line.strip().split(',')
                  yield parsed
      
      expected =     [
        ('2015-04-02 13:30:00.000000', 'BEST_BID', Decimal('90.38'), 2),
       ('2015-04-02 13:30:00.000000', 'BEST_ASK', Decimal('90.42'), 38),
       ('2015-04-02 13:30:01.000000', 'BEST_ASK', Decimal('90.42'), 31),
       ('2015-04-02 13:30:05.000000', 'BEST_ASK', Decimal('90.42'), 2),
       ('2015-04-02 13:30:06.000000', 'BEST_BID', Decimal('90.35'), 8),
       ('2015-04-02 13:30:06.000000', 'BEST_ASK', Decimal('90.42'), 2),
       ('2015-04-02 13:30:09.000000', 'BEST_BID', Decimal('90.35'), 7),
       ('2015-04-02 13:30:10.000000', 'BEST_BID', Decimal('90.35'), 2),
       ('2015-04-02 13:30:15.000000', 'BEST_ASK', Decimal('90.42'), 3),
       ('2015-04-02 13:30:18.000000', 'BEST_ASK', Decimal('90.39'), 2),
       ('2015-04-02 13:30:19.000000', 'BEST_ASK', Decimal('90.39'), 1),
       ('2015-04-02 13:30:22.000000', 'BEST_BID', Decimal('90.35'), 2),
       ('2015-04-02 13:30:22.000000', 'BEST_ASK', Decimal('90.39'), 4),
       ('2015-04-02 13:30:24.000000', 'BEST_BID', Decimal('90.35'), 3),
       ('2015-04-02 13:30:28.000000', 'BEST_BID', Decimal('90.35'), 1),
       ('2015-04-02 13:30:28.000000', 'BEST_ASK', Decimal('90.36'), 1),
       ('2015-04-02 13:30:30.000000', 'BEST_BID', Decimal('90.35'), 1),
       ('2015-04-02 13:30:30.000000', 'BEST_ASK', Decimal('90.37'), 1),
       ('2015-04-02 13:30:32.000000', 'BEST_ASK', Decimal('90.37'), 7),
       ('2015-04-02 13:30:42.000000', 'BEST_BID', Decimal('90.35'), 2),
       ('2015-04-02 13:30:42.000000', 'BEST_ASK', Decimal('90.37'), 5),
       ('2015-04-02 13:30:43.000000', 'BEST_BID', Decimal('90.35'), 3),
       ('2015-04-02 13:30:45.000000', 'BEST_BID', Decimal('90.35'), 5),
       ('2015-04-02 13:30:46.000000', 'BEST_BID', Decimal('90.35'), 15),
       ('2015-04-02 13:30:49.000000', 'BEST_BID', Decimal('90.35'), 5),
       ('2015-04-02 13:30:59.000000', 'BEST_BID', Decimal('90.35'), 10),
       ('2015-04-02 13:30:59.000000', 'BEST_ASK', Decimal('90.36'), 1)]
      
      ticks_data = ticks_quotes('HYG US Equity', start_time, end_time, ticks_loader=load_test)
      filtered_ticks_data = time_filter(ticks_data, '093000', '093100', pytz.timezone('US/Eastern'))
      self.maxDiff = None
      self.assertEqual(expected, list(filtered_ticks_data))
      
if __name__ == '__main__':
    unittest.main()
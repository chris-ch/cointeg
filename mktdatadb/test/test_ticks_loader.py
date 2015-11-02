import unittest
import logging
import os
import pytz
from datetime import datetime

from mktdatadb import ticks_trades, time_filter

class TestTicksLoader(unittest.TestCase):

  def test_quotes(self):
      start_time = datetime(2015, 2, 28, 17, 0)
      end_time = datetime(2015, 3, 2, 21, 0)
      
      def load_test(ticker, start_time, end_time, pattern):
          path_to_test_data = os.path.dirname(os.path.realpath(__file__))
          test_data_filename = os.sep.join([path_to_test_data, 'HYG-20150302.csv'])
          logging.info('loading test data file %s', test_data_filename)
          with open(test_data_filename, 'r') as test_data:
              line = test_data.read()
              yield line
      
      ticks_data = ticks_trades('HYG US Equity', start_time, end_time, ticks_loader=load_test)
      self.assertEqual(0, len(ticks_data))
      filtered_ticks_data = time_filter(ticks_data, '093000', '160000', pytz.timezone('US/Eastern'))
      
import os
import pickle
import unittest
import numpy
import pandas
import sys
from pandas.tslib import Timestamp
from pandas.util import testing

from bollinger import scaled_step, get_position_scaling


def geometric_brownian_motion(date_start, date_end, mu=0.1, sigma=0.01, scaling=20.):
    dates = pandas.date_range(date_start, date_end)
    period_in_years = (dates.max() - dates.min()).days / 365
    count = dates.size
    step = float(period_in_years) / count
    times = numpy.linspace(start=0, stop=period_in_years, num=count)
    walk = numpy.random.standard_normal(size=count)
    walk = numpy.cumsum(walk) * numpy.sqrt(step)  # standard brownian motion
    values = (mu - 0.5 * sigma ** 2) * times + sigma * walk
    return pandas.Series(scaling * numpy.exp(values), index=dates)  # geometric brownian motion


class TestBollinger(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        current_module = sys.modules[__name__]
        cls._resources_path = os.sep.join(
            [os.path.dirname(current_module.__file__), 'resources'])

    def load_resource(self, relative_path, loader=None):
        if loader is None:
            loader = pickle.load

        resource_path = os.sep.join([self._resources_path, relative_path])
        resource_path_norm = os.path.abspath(resource_path)
        with open(resource_path_norm, 'r') as resource_stream:
            resource = loader(resource_stream)
            return resource

    def load_pandas(self, relative_path):
        resource_path = os.sep.join([self._resources_path, relative_path])
        resource_path_norm = os.path.abspath(resource_path)
        resource = pandas.read_pickle(resource_path_norm)
        return resource

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_step_function_unscaled(self):
        expected = {
            -1.25: -2,
            -1.: -1,
            -0.75: -1,
            -0.5: -1,
            -0.25: -1,
            0.: 0,
            0.25: 0,
            0.5: 0,
            0.75: 0,
            1.: 1,
            1.25: 1
        }
        for value in sorted(expected.keys()):
            self.assertEqual(expected[value], scaled_step(value))

    def test_step_function_scaled(self):
        expected = {
            -2.25: -2,
            -2.: -1,
            -1.: -1,
            -0.75: -1,
            -0.5: -1,
            -0.25: -1,
            0.: 0,
            0.25: 0,
            0.5: 0,
            0.75: 0,
            1.: 0,
            1.25: 0,
            2.: 1,
        }
        for value in sorted(expected.keys()):
            self.assertEqual(expected[value], scaled_step(value, step_length=2.))

    def test_bollinger(self):
        prices = self.load_pandas('test_bollinger.pkl')
        df = pandas.concat([prices, prices.shift()], axis=1)
        df.columns = ['price', 'price_prev']
        df['sigma'] = prices.std()
        df['mu'] = prices.mean()
        cumul = {'current_scaling': 0.}

        def scale(row, cumul=cumul):
            current_scaling = cumul['current_scaling']
            price = row['price']
            mu = row['mu']
            sigma = 0.8 * row['sigma']
            new_position_scaling = get_position_scaling(price, current_scaling, mu, sigma)
            # updating for next step
            cumul['current_scaling'] = new_position_scaling
            result = {
                'position_scaling': new_position_scaling,
                'band_inf': mu + ((new_position_scaling + 1) * sigma),
                'band_mid': mu + (new_position_scaling * sigma),
                'band_sup': mu + ((new_position_scaling - 1) * sigma)
            }
            return pandas.Series(result)

        df = pandas.concat([df, df.apply(scale, axis=1)], axis=1)
        df_diff = df['position_scaling'] - df['position_scaling'].shift().fillna(0.)
        expected = {Timestamp('2013-02-20 00:00:00'): -2.0, Timestamp('2012-12-06 00:00:00'): 0.0,
                    Timestamp('2012-05-29 00:00:00'): 0.0, Timestamp('2012-01-01 00:00:00'): -3.0,
                    Timestamp('2012-07-07 00:00:00'): 1.0, Timestamp('2012-02-10 00:00:00'): -2.0,
                    Timestamp('2013-01-05 00:00:00'): -1.0, Timestamp('2012-03-03 00:00:00'): -1.0,
                    Timestamp('2013-01-27 00:00:00'): -1.0, Timestamp('2012-04-04 00:00:00'): 0.0,
                    Timestamp('2012-04-18 00:00:00'): 1.0, Timestamp('2013-01-12 00:00:00'): 0.0}
        variations = df_diff[df_diff != 0.].cumsum().to_dict()
        self.assertEqual(expected, variations)

    def test_bollinger_limit(self):
        prices = self.load_pandas('test_bollinger.pkl')
        df = pandas.concat([prices, prices.shift()], axis=1)
        df.columns = ['price', 'price_prev']
        df['sigma'] = prices.std()
        df['mu'] = prices.mean()
        cumul = {'current_scaling': 0.}

        def scale(row, limit=1, cumul=cumul):
            current_scaling = cumul['current_scaling']
            price = row['price']
            mu = row['mu']
            sigma = 0.8 * row['sigma']
            new_position_scaling = get_position_scaling(price, current_scaling, mu, sigma, limit=limit)
            # updating for next step
            cumul['current_scaling'] = new_position_scaling
            result = {
                'position_scaling': new_position_scaling,
                'band_inf': mu + ((new_position_scaling + 1) * sigma),
                'band_mid': mu + (new_position_scaling * sigma),
                'band_sup': mu + ((new_position_scaling - 1) * sigma)
            }
            return pandas.Series(result)

        df = pandas.concat([df, df.apply(scale, axis=1)], axis=1)
        df_diff = df['position_scaling'] - df['position_scaling'].shift().fillna(0.)
        expected = {Timestamp('2012-12-06 00:00:00'): 0.0, Timestamp('2012-05-29 00:00:00'): 0.0,
                    Timestamp('2012-07-07 00:00:00'): 1.0, Timestamp('2012-02-10 00:00:00'): -1.0,
                    Timestamp('2013-01-05 00:00:00'): -1.0, Timestamp('2013-01-27 00:00:00'): -1.0,
                    Timestamp('2012-04-18 00:00:00'): 1.0, Timestamp('2012-04-04 00:00:00'): 0.0,
                    Timestamp('2013-01-12 00:00:00'): 0.0}
        variations = df_diff[df_diff != 0.].cumsum()
        testing.assert_series_equal(pandas.Series(expected, name='position_scaling'), variations)

import unittest
import os
import sys
import pickle
from datetime import datetime, timedelta

import numpy
import pandas

from statsext import cointeg


class TestCointegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        current_module = sys.modules[__name__]
        cls._resources_path = os.sep.join(
            [os.path.dirname(current_module.__file__), 'resources'])

    def load_resource(self, relative_path, loader=None):
        resource = None
        if loader is None:
            loader = pickle.load

        resource_path = os.sep.join([self._resources_path, relative_path])
        resource_path_norm = os.path.abspath(resource_path)
        with open(resource_path_norm, 'r') as resource_file:
            resource = loader(resource_file)

        return resource

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_adf(self):
        s1 = self.load_resource('s1.pickle')
        s2 = self.load_resource('s2.pickle')
        s3 = self.load_resource('s3.pickle')
        a = 0.5
        x_1t = numpy.cumsum(s1) + s2
        x_2t = a * numpy.cumsum(s1) + s3
        x_3t = 100. * s3
        self.assertTrue(cointeg.is_not_stationary(x_1t))
        self.assertTrue(cointeg.is_not_stationary(x_2t))
        self.assertFalse(cointeg.is_not_stationary(x_3t))

    def test_johansen(self):
        s1 = self.load_resource('s1.pickle')
        s2 = self.load_resource('s2.pickle')
        s3 = self.load_resource('s3.pickle')
        a = 0.5
        x_1t = numpy.cumsum(s1) + s2
        x_2t = a * numpy.cumsum(s1) + s3
        x_3t = s3
        test_date = datetime(2015, 10, 1)
        n = len(s1)
        index = pandas.date_range(test_date - timedelta(10), periods=n, freq='D')
        y = pandas.DataFrame(index=index, data={'col1': x_1t, 'col2': x_2t, 'col3': x_3t})
        vectors = cointeg.get_johansen(y, lag=1)
        v1 = vectors[0]
        v2 = vectors[1]
        expected_v1 = numpy.array([1., -1.9999231, 2.6499922])
        numpy.testing.assert_almost_equal(v1, expected_v1)
        expected_v2 = numpy.array([-2.3183438, 4.6361944, -1.])
        numpy.testing.assert_almost_equal(v2, expected_v2)
        self.assertFalse(cointeg.is_not_stationary(numpy.dot(y.as_matrix(), v1), significance='10%'))
        self.assertFalse(cointeg.is_not_stationary(numpy.dot(y.as_matrix(), v2), significance='10%'))


if __name__ == '__main__':
    unittest.main()
    
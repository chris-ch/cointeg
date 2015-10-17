import unittest
import os
import sys
import logging
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
        resource_path_norm =  os.path.abspath(resource_path)
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
        self.assertTrue(cointeg.is_cointegrated(x_1t))
        self.assertTrue(cointeg.is_cointegrated(x_2t))
        self.assertFalse(cointeg.is_cointegrated(x_3t))
        
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
        jres = cointeg.get_johansen(y, lag=1)
        self.assertEquals(2, jres['count_cointegration_vectors'], 'number of cointegration vectors does not match')
        v1 = jres['cointegration_vectors'][:, 0]
        v2 = jres['cointegration_vectors'][:, 1]
        v3 = jres['eigen_vectors'][:, 2]  # v3 is not a cointegration vector
        expected_v1 = numpy.array([1.18712515, -2.37415904, 3.14587243])
        numpy.testing.assert_almost_equal(v1, expected_v1)
        expected_v2 = numpy.array([-0.76082907, 1.52149628, -0.32817785])
        numpy.testing.assert_almost_equal(v2, expected_v2)
        expected_v3 = numpy.array([0.00019993, 0.04721915, -0.04629564])
        numpy.testing.assert_almost_equal(v3, expected_v3)
    
if __name__ == '__main__':
    unittest.main()
    
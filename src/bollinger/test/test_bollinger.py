import unittest

from bollinger import scaled_step


class TestBollinger(unittest.TestCase):

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

if __name__ == '__main__':
    unittest.main()

import datetime

import numpy as np
import pandas as pd

from statsext import cointeg


__author__ = 'Christophe'

if __name__ == '__main__':
    mu, sigma = 0, 1  # mean and standard deviation
    n = 10000
    s1 = np.random.normal(mu, sigma, n)
    s2 = np.random.normal(mu, sigma, n)
    s3 = np.random.normal(mu, sigma, n)
    
    a = 0.5
    x_1t = np.cumsum(s1) + s2
    x_2t = a * np.cumsum(s1) + s3
    x_3t = s3
    todays_date = datetime.datetime.now().date()
    index = pd.date_range(todays_date - datetime.timedelta(10), periods=n, freq='D')
    y = pd.DataFrame(index=index, data={'col1': x_1t, 'col2': x_2t, 'col3': x_3t})
    print(cointeg.is_not_stationary(x_1t))
    print(cointeg.is_not_stationary(np.diff(x_1t)))
    print(cointeg.is_not_stationary(x_2t))
    print(cointeg.is_not_stationary(np.diff(x_2t)))
    print(cointeg.is_not_stationary(x_3t))
    jres = cointeg.get_johansen(y, lag=1)
    print("There are ", jres['count_cointegration_vectors'], "cointegration vectors")
    v1 = jres['cointegration_vectors'][:, 0]
    v2 = jres['cointegration_vectors'][:, 1]
    print(v1)
    print(v2)
    v3 = jres['eigenvectors'][:, 2]  # v3 is not a cointegration vector
    print(v1 / -v1[1])
    print(v2 / -v2[1])

import math

__author__ = 'Christophe'


def scaled_step(value, step_length=1.):
    return math.floor(value / float(step_length))


def get_position_scaling(signal_value, current_scaling, mu, sigma, limit=None):
    """

    :param signal_value: input value
    :param current_scaling: current position size
    :param mu: reference value
    :param sigma: step size
    :param limit: limits absolute position to the indicated value
    :return:
    """
    current_band = scaled_step(signal_value - mu, step_length=sigma)
    # in case of crossings
    new_scaling = current_scaling
    if current_band > current_scaling:
        if limit is None or abs(current_band) <= abs(limit):
            new_scaling = current_band

    elif current_band < current_scaling - 1:
        if limit is None or abs(current_band + 1) <= abs(limit):
            new_scaling = current_band + 1

    return new_scaling
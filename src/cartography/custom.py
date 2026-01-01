"""Module custom.py"""
import numpy as np


class Custom:
    """
    Drawing functions
    """

    def __init__(self):
        pass

    @staticmethod
    def f_radius(value: float, lower: float, upper: float) -> float:
        """

        arctan: minimum + (maximum - minimum)*2*np.arctan(0.5*value*np.pi)/np.pi
        tanh: minimum + (maximum - minimum)*np.tanh(value)

        :param value:
        :param lower:
        :param upper:
        :return:
        """

        minimum = 3.5
        maximum = 33.5

        # factor = value/(1 + np.abs(value))
        # est = minimum + factor*(maximum - minimum)

        factor = (value - lower)/(1 + upper - lower)
        est = minimum + factor*(maximum - minimum)

        return est

    @staticmethod
    def f_opacity(value: float, lower: float, upper: float) -> float:
        """
        (value + 1)/np.sqrt(1 + np.power(value, 2))

        :param value:
        :param lower:
        :param upper:
        :return:
        """

        minimum = 0.25
        maximum = 0.95

        est = minimum + (maximum - minimum)*(value - lower)/np.sqrt(1 + np.power(upper - lower, 2))

        return est

    @staticmethod
    def f_stroke(value: float) -> bool:

        return value < 0

    @staticmethod
    def f_fill(value: float) -> bool:

        return value >= 0

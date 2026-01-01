"""Module custom.py"""
import numpy as np


class Custom:
    """
    Drawing functions
    """

    def __init__(self, lower: float, upper: float):
        """

        :param lower:
        :param upper:
        """

        self.__lower = lower
        self.__upper = upper

    @staticmethod
    def f_radius(value: float) -> float:
        """
        arctan: minimum + (maximum - minimum)*2*np.arctan(0.5*value*np.pi)/np.pi
        tanh: minimum + (maximum - minimum)*np.tanh(value)

        :param value:
        :param lower:
        :param upper:
        :return:
        """

        minimum = 8.5
        maximum = 22.5

        factor = np.abs(value)/(1 + np.abs(value))
        est = minimum + factor*(maximum - minimum)

        return est

    def f_opacity(self, value: float) -> float:
        """
        factor = (value + 1)/np.sqrt(1 + np.power(value, 2))

        :param value:
        :return:
        """

        minimum = 0.35
        maximum = 0.95

        factor = (value - self.__lower)/np.sqrt(1 + np.power(self.__upper - self.__lower, 2))
        est = minimum + factor*(maximum - minimum)

        return est

    @staticmethod
    def f_stroke(value: float) -> bool:
        """

        :param value:
        :return:
        """

        return value < 0

    @staticmethod
    def f_fill(value: float) -> bool:
        """

        :param value:
        :return:
        """

        return value >= 0

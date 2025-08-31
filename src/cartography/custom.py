"""Module custom.py"""
import numpy as np


class Custom:
    """
    Drawing functions
    """

    def __init__(self):
        pass

    @staticmethod
    def f_radius(value: float) -> float:
        """

        arctan: minimum + (maximum - minimum)*2*np.arctan(0.5*value*np.pi)/np.pi
        tanh: minimum + (maximum - minimum)*np.tanh(value)

        :param value:
        :return:
        """

        minimum = 950
        maximum = 2500

        return minimum + (maximum - minimum)*value/(1 + np.abs(value))

    @staticmethod
    def f_opacity(value: float) -> float:
        """

        :param value:
        :return:
        """

        minimum = 0.25
        maximum = 0.90

        return minimum + (maximum - minimum)*value/np.sqrt(1 + np.power(value, 2))

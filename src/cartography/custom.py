"""Module custom.py"""
import numpy as np


class Custom:
    """
    Drawing functions
    """

    def __init__(self):
        pass

    @staticmethod
    def f_radius(value: float):
        """

        arctan: minimum + (maximum - minimum)*2*np.arctan(0.5*value*np.pi)/np.pi
        tanh: minimum + (maximum - minimum)*np.tanh(value)

        :param value:
        :return:
        """

        minimum = 1250
        maximum = 2000
        minimum + (maximum - minimum)*value/(1 + np.abs(value))

    @staticmethod
    def f_opacity(value: float):
        """

        :param value:
        :return:
        """

        minimum = 0.25
        maximum = 0.65

        minimum + (maximum - minimum)*value/np.sqrt(1 + np.power(value, 2))

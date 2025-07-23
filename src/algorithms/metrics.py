"""Module metrics.py"""
import json
import os

import pandas as pd

import config
import src.functions.directories
import src.functions.objects


class Metrics:
    """
    Persist
    """

    def __init__(self):
        """

        Constructor
        """

        # The storage area
        self.__configurations = config.Config()

        # For creating JSON files
        self.__objects = src.functions.objects.Objects()

    @staticmethod
    def __get_nodes(section: pd.DataFrame, points: int) -> dict:
        """

        :param section: The section of a gauge
        :param points:
        :return:
        """

        frame: pd.DataFrame = section.copy().loc[section['points'] == points, :]
        frame.drop(columns='points', inplace=True)

        string = frame.copy().to_json(orient='split')
        _data = json.loads(string)

        return {points: _data}

    def exc(self, section: pd.DataFrame, ending: int) -> str:
        """

        :param section: The weighted rates of change of river levels with respect to one or more time spans.
        :param ending: The period ending timestamp; epoch seconds
        :return:
        """

        nodes = [self.__get_nodes(section=section, points=int(points)) for points in section['points'].unique()]

        message = self.__objects.write(
            nodes=nodes, path=os.path.join(self.__configurations.points_, f'{ending}.json'))

        return message

"""Module persist.py"""
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

    def __init__(self, instances: pd.DataFrame):
        """

        :param instances: The weighted rates of change of river levels with respect to one or more time spans.
        """

        self.__instances = instances

        # The storage area
        self.__configurations = config.Config()

        # For creating JSON files
        self.__objects = src.functions.objects.Objects()

        # Fields
        self.__fields = ['maximum', 'minimum', 'latest', 'median', 'points', 'hours', 'catchment_id', 'ts_id',
                         'station_name', 'catchment_name', 'latitude', 'longitude', 'river_name', 'ending']

    def __get_nodes(self, points: int) -> dict:
        """

        :param points:
        :return:
        """

        frame: pd.DataFrame = self.__instances.copy().loc[self.__instances['points'] == points, self.__fields]
        string = frame.copy().to_json(orient='split')

        return json.loads(string)

    def __persist(self, nodes, points):
        """

        :param nodes:
        :param points:
        :return:
        """

        return self.__objects.write(
            nodes=nodes, path=os.path.join(self.__configurations.points_, f'{points:04d}.json'))

    def exc(self):
        """


        :return:
        """

        self.__instances.info()

        computations = []
        for points in self.__instances['points'].unique():
            nodes = self.__get_nodes(points=int(points))
            message = self.__persist(nodes=nodes, points=points)
            computations.append(message)

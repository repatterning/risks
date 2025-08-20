"""Module persist.py"""
import logging
import json
import os

import pandas as pd
import numpy as np

import config
import src.functions.directories
import src.functions.objects
import src.algorithms.disaggregates


class Persist:
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

    def __get_nodes(self, points: int) -> dict | list[dict]:
        """
        string = frame.copy().to_json(orient='split')
        json.loads(string)

        :param points: The number of points across which rate calculations are made, e.g., 1 -> 0.25 hours,
                       4 -> 1 hour, etc.
        :return:
        """

        frame: pd.DataFrame = self.__instances.copy().loc[self.__instances['points'] == points, :]
        nodes = src.algorithms.disaggregates.Disaggregates(frame=frame).__call__()

        return nodes

    def __persist(self, nodes, points):
        """

        :param nodes:
        :param points:
        :return:
        """

        return self.__objects.write(
            nodes=nodes, path=os.path.join(self.__configurations.points_, f'{points:04d}.json'))

    def exc(self, points_: np.ndarray):
        """

        :param points_: Each array value denotes the number of points across which rate calculations
                        are made, e.g., 1 -> 0.25 hours, 4 -> 1 hour, etc.
        :return:
        """

        computations = []
        for points in points_:
            nodes = self.__get_nodes(points=int(points))
            message = self.__persist(nodes=nodes, points=points)
            computations.append(message)
        logging.info(computations)

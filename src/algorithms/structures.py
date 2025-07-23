"""Module structures.py"""

import pandas as pd


class Structures:
    """
    Structures
    """

    def __init__(self, instances: pd.DataFrame):
        """

        :param instances:
        """

        self.__instances = instances

        # Fields
        self.__fields = ['maximum', 'minimum', 'latest', 'median', 'points', 'catchment_id', 'ts_id',
                         'station_name', 'catchment_name', 'latitude', 'longitude', 'river_name']


    def exc(self):
        """

        :return:
        """

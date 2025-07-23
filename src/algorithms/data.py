"""Module data.py"""
import datetime
import time

import dask.dataframe as ddf
import numpy as np
import pandas as pd


class Data:
    """
    Data
    """

    def __init__(self, arguments: dict):
        """

        :param arguments: A set of arguments vis-à-vis calculation & storage objectives.
        """

        self.__arguments = arguments

        # Focus
        self.__dtype = {'timestamp': np.float64, 'ts_id': np.float64, 'measure': float}

    def __get_data(self, keys: list[str]):
        """

        :param keys: A list of S3 (Simple Storage Service) uniform resource locators
        :return:
        """

        try:
            block: pd.DataFrame = ddf.read_csv(
                keys, header=0, usecols=list(self.__dtype.keys()), dtype=self.__dtype).compute()
        except ImportError as err:
            raise err from err

        block.reset_index(drop=True, inplace=True)
        block.sort_values(by='timestamp', ascending=True, inplace=True)
        block.drop_duplicates(subset='timestamp', keep='first', inplace=True)

        return block

    def exc(self, keys: list[str]) -> pd.DataFrame:
        """

        :param keys: A list of S3 (Simple Storage Service) uniform resource locators
        :return:
        """

        block = self.__get_data(keys=keys)
        block = block.copy()[['timestamp', 'measure']]

        # The calculations starting point
        as_from = datetime.datetime.now() - datetime.timedelta(days=round(self.__arguments.get('spanning')*365))
        starting = 1000 * time.mktime(as_from.timetuple())

        return block.copy().loc[block['timestamp'] >= starting, :]

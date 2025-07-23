import logging

import dask
import pandas as pd

import src.algorithms.persist


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

    @dask.delayed
    def __get_section(self, ending: int) -> pd.DataFrame:
        """

        :param ending: The weighted rates of change are with respect to an ending timestamp
        :return:
        """

        return self.__instances.copy().loc[
            self.__instances['ending'] == ending, self.__fields]

    def exc(self):
        """

        :return:
        """

        endings = self.__instances['ending'].unique()

        # Delayed Tasks
        __persist = dask.delayed(src.algorithms.persist.Persist().exc)

        computations = []
        for ending in endings:
            section = self.__get_section(ending=int(ending))
            message = __persist(section=section, ending=int(ending))
            computations.append(message)
        messages = dask.compute(computations, scheduler='threads')[0]

        logging.info(messages)

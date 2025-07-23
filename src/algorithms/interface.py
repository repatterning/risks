"""Module interface.py"""

import os

import dask
import pandas as pd

import config
import src.algorithms.data
import src.algorithms.metrics
import src.algorithms.structures
import src.elements.partitions as pr
import src.functions.streams


class Interface:
    """
    The interface to quantiles calculations.
    """

    def __init__(self, listings: pd.DataFrame, arguments: dict):
        """

        :param listings: date / uri / catchment_id / ts_id
        :param arguments: A set of arguments vis-à-vis calculation & storage objectives.
        """

        self.__listings = listings
        self.__arguments = arguments

        # Configurations
        self.__configurations = config.Config()

    @dask.delayed
    def __get_keys(self, ts_id: int) -> list:
        """

        :param ts_id: The identification code of a gauge's time series
        :return:
        """

        keys: pd.Series = self.__listings.loc[self.__listings['ts_id'] == ts_id, 'uri']

        return keys.to_list()

    def exc(self, partitions: list[pr.Partitions], reference: pd.DataFrame):
        """

        :param partitions: The time series partitions.
        :param reference: The reference sheet of gauges.  Each instance encodes the attributes of a gauge.
        :return:
        """

        reference.info()

        # Delayed tasks
        __data = dask.delayed(src.algorithms.data.Data(arguments=self.__arguments).exc)
        __metrics = dask.delayed(src.algorithms.metrics.Metrics(arguments=self.__arguments).exc)

        # Compute
        computations = []
        for partition in partitions:
            keys = self.__get_keys(ts_id=partition.ts_id)
            data = __data(keys=keys)
            metrics = __metrics(data=data, partition=partition)
            computations.append(metrics)
        calculations = dask.compute(computations, scheduler='threads')[0]

        # Merge each instance with its descriptive attributes
        instances_ = pd.concat(calculations, ignore_index=True, axis=0)
        instances: pd.DataFrame = instances_.merge(reference, how='left', on=['catchment_id', 'ts_id'])

        # Tableau
        src.functions.streams.Streams().write(
            blob=instances, path=os.path.join(self.__configurations.data_, 'instances.csv'))

        # Structure & Persist
        src.algorithms.structures.Structures(instances=instances).exc()

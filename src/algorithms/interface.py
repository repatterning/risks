"""Module interface.py"""
import logging

import dask
import pandas as pd

import config
import src.algorithms.data
import src.algorithms.valuations
import src.elements.master as mr
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
        __valuations = dask.delayed(src.algorithms.valuations.Valuations(arguments=self.__arguments).exc)

        # Compute
        computations = []
        for partition in partitions[:8]:
            keys = self.__get_keys(ts_id=partition.ts_id)
            data = __data(keys=keys)
            master: mr.Master = __valuations(data=data, partition=partition)
            computations.append(master.metrics)
        calculations = dask.compute(computations, scheduler='threads')[0]

        # Merge each instance with its descriptive attributes
        instances_ = pd.concat(calculations, ignore_index=True, axis=0)
        instances: pd.DataFrame = instances_.merge(reference, how='left', on=['catchment_id', 'ts_id'])
        logging.info(instances)

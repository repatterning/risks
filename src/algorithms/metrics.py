"""Module metrics.py"""
import logging
import numpy as np
import pandas as pd

import src.elements.partitions as pr


class Metrics:
    """
    Metrics
    """

    def __init__(self, arguments: dict):
        """

        :param arguments:
        """

        self.__arguments = arguments

        # time intervals (hours), and the corresponding number of points that span each time interval
        self.__tau: np.ndarray = np.array(self.__arguments.get('tau'), dtype=float)
        self.__points: np.ndarray = (self.__tau / self.__arguments.get('frequency')).astype(int)

        # Back in time
        self.__limits = np.arange(-36, 0)

    def __rates(self, frame: pd.DataFrame):
        """

        :param frame:
        :return:
        """

        # differences
        differences_ = [frame.copy()['measure'].diff(int(i)).to_frame(name=i) for i in self.__points]
        differences = pd.concat(differences_, axis=1, ignore_index=False)

        # delta measure / delta time
        # rates = pd.DataFrame(data=np.true_divide(differences.to_numpy(), self.__tau), columns=self.__points)
        rates = np.true_divide(differences.to_numpy(), self.__tau)

        return rates

    def __weights(self, frame: pd.DataFrame):

        # delta measure / original measure
        weights_ = [frame.copy()['measure'].pct_change(int(i)).to_frame(name=i) for i in self.__points]
        weights = pd.concat(weights_, axis=1, ignore_index=False)

        return weights.to_numpy()

    def __get_metrics(self, gamma: pd.DataFrame, limit: int):
        """

        :param gamma:
        :param limit:
        :return:
        """

        states = gamma.copy()[:limit]
        logging.info('STATES: \n%s', states)

        metrics = pd.DataFrame(
            data={'maximum': states[self.__points].max(axis=0).values,
                  'minimum': states[self.__points].min(axis=0).values,
                  'latest': states[self.__points][-1:].squeeze().values,
                  'median': states[self.__points].median(axis=0).values})

        metrics = metrics.assign(points=self.__points)
        metrics['ending'] = states['timestamp'].max()

        return metrics

    def exc(self, data: pd.DataFrame, partition: pr.Partitions):
        """

        :param data:
        :param partition:
        :return:
        """

        frame = data.copy()
        frame.sort_values(by='timestamp', ascending=True, inplace=True)

        # Weighted rates of river level change
        gamma = pd.DataFrame(
            data=self.__rates(frame=frame) * self.__weights(frame=frame), columns=self.__points)
        gamma['timestamp'] = frame['timestamp'].values
        logging.info('gamma:\n%s', gamma)

        # Metrics
        metrics_ = [self.__get_metrics(gamma=gamma, limit=l) for l in self.__limits]
        metrics = pd.concat(metrics_)
        metrics['catchment_id'] = partition.catchment_id
        metrics['ts_id'] = partition.ts_id

        return metrics

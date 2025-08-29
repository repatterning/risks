"""Module parcels.py"""
import geopandas
import numpy as np
import pandas as pd

import src.elements.parcel as pcl


class Parcels:
    """
    Parcels
    """

    def __init__(self, data: geopandas.GeoDataFrame):
        """

        :param data: The frame of metrics per gauge station.
        """

        self.__data = data

        # Seed
        self.__seed = 5

    def __get_decimals(self, size: int):
        """

        :param size: The number of required random numbers
        :return:
        """

        rng = np.random.default_rng(seed=self.__seed)

        return rng.uniform(low=0.35, high=0.99, size=size)

    def __catchments(self) -> pd.DataFrame:
        """

        :return:
        """

        frame = self.__data[['catchment_id', 'catchment_name', 'latest']].groupby(
            by=['catchment_id', 'catchment_name']).agg(maximum=('latest', 'max'))

        # Convert 'catchment_id' & 'catchment_name' to fields; currently indices.
        frame.reset_index(drop=False, inplace=True)

        # Hence
        frame['rank'] = frame['maximum'].rank(method='first', ascending=False).astype(int)
        frame.drop(columns='maximum', inplace=True)
        frame.sort_values(by='catchment_name', inplace=True)
        frame.reset_index(drop=True, inplace=True)

        return frame

    def exc(self) -> list[pcl.Parcel]:
        """

        :return:
        """

        catchments = self.__catchments()
        catchments['decimal'] = self.__get_decimals(size=catchments.shape[0])

        # An iterable for mapping by layer
        values: list[dict] = catchments.to_dict(orient='records')
        parcels = [pcl.Parcel(**value) for value in values]

        return parcels

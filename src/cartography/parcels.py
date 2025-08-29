
import geopandas
import pandas as pd

import numpy as np

import src.elements.parcel as pcl

class Parcels:

    def __init__(self, data: geopandas.GeoDataFrame):
        """

        :param data:
        """

        self.__data = data

        # Seed
        self.__seed = 5

    def __get_decimals(self, size: int):
        """

        :param size:
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

        # The map will display the top ..., by default
        frame.sort_values(by='maximum', ascending=False, inplace=True)

        # Convert 'catchment_id' & 'catchment_name' to fields; currently indices.
        frame.reset_index(drop=False, inplace=True)

        # Create an index field for the dictionary
        frame.reset_index(drop=False, inplace=True)

        return frame

    def exc(self) -> list[pcl.Parcel]:
        """

        :return:
        """

        catchments = self.__catchments()
        catchments['decimal'] = self.__get_decimals(size=catchments.shape[0])

        # An iterble for mapping
        values: list[dict] = catchments.to_dict(orient='records')
        parcels = [pcl.Parcel(**value) for value in values]

        return parcels
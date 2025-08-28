
import geopandas

import pandas as pd

class Interface:

    def __init__(self, instances: pd.DataFrame, reference: pd.DataFrame):

        self.__instances = instances
        self.__reference = reference

    def __get_attributes(self) -> geopandas.GeoDataFrame:

        attributes = geopandas.GeoDataFrame(
            self.__reference,
            geometry=geopandas.points_from_xy(self.__reference['longitude'], self.__reference['latitude'])
        )
        attributes.crs = 'epsg:4326'

        return attributes

    def __get_data(self, points: int) -> pd.DataFrame:

        return self.__instances.copy().loc[self.__instances['points'] == points, :]

    def exc(self, ):
        """

        :return:
        """

        points_ = self.__instances['points'].unique()
        attributes = self.__get_attributes()

        for points in points_:

            data = self.__get_data(points=points)
            attributes.merge(data, how='left', on=['catchment_id', 'station_id'])

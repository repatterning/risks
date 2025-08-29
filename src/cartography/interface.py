
import geopandas

import pandas as pd



class Interface:

    def __init__(self, instances: pd.DataFrame, boundaries: geopandas.GeoDataFrame, reference: pd.DataFrame):

        self.__instances = instances
        self.__boundaries = boundaries
        self.__reference = reference



    def __get_data(self, points: int) -> geopandas.GeoDataFrame:

        values = self.__instances.copy().loc[self.__instances['points'] == points, :]

        data = geopandas.GeoDataFrame(
            values,
            geometry=geopandas.points_from_xy(values['longitude'], values['latitude'])
        )
        data.crs = 'epsg:4326'

        return data

    def exc(self, ):
        """

        :return:
        """

        points_ = self.__instances['points'].unique()

        for points in points_:

            self.__get_data(points=points)


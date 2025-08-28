
import geopandas

import pandas as pd

class Interface:

    def __init__(self, reference: pd.DataFrame):

        self.__reference = reference

    def __get_attributes(self):

        attributes = geopandas.GeoDataFrame(
            self.__reference,
            geometry=geopandas.points_from_xy(self.__reference['longitude'], self.__reference['latitude'])
        )
        attributes.crs = 'epsg:4326'

        return attributes

    def exc(self, instances: pd.DataFrame):
        """
        
        :param instances:
        :return:
        """



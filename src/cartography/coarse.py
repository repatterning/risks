"""Module coarse.py"""
import logging

import geopandas
import pandas as pd
import shapely

import src.cartography.cuttings


class Coarse:

    def __init__(self, reference: pd.DataFrame, fine: geopandas.GeoDataFrame):
        """

        :param reference: Each instance represents a distinct gauge station, alongside its details.
        :param fine: The low level, most granular, catchment-segments fine.
        """
        
        self.__reference = reference
        self.__fine = fine

    def __get_attributes(self) -> geopandas.GeoDataFrame:
        """

        :return:
        """

        attributes = geopandas.GeoDataFrame(
            self.__reference,
            geometry=geopandas.points_from_xy(self.__reference['longitude'], self.__reference['latitude'])
        )
        attributes.crs = 'epsg:4326'

        return attributes
    
    def exc(self) -> geopandas.GeoDataFrame:
        """

        :return:
        """
        
        attributes = self.__get_attributes()
        catchments = self.__reference[['catchment_id', 'catchment_name']].drop_duplicates()

        _coarse = []
        for code, name in zip(catchments.catchment_id.values, catchments.catchment_name.values):

            instances = attributes.copy().loc[attributes['catchment_id'] == code, :]

            # Which [child] polygons are associated with the catchment in focus?
            identifiers = self.__fine.geometry.map(src.cartography.cuttings.Cuttings(instances=instances).inside)
            applicable = self.__fine.copy().loc[identifiers > 0, :]

            # Convert the polygons into a single polygon.
            frame = geopandas.GeoDataFrame({'catchment_id': [code], 'catchment_name': [name]})
            _coarse.append(frame.set_geometry([shapely.unary_union(applicable.geometry)]))

        coarse: geopandas.GeoDataFrame = pd.concat(_coarse, ignore_index=True, axis=0)
        coarse.crs = self.__fine.crs.srs
        logging.info('Coördinate Reference System:\n%s', coarse.crs)

        return coarse

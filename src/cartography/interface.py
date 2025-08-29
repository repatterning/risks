import logging
import boto3

import geopandas
import pandas as pd

import src.cartography.coarse
import src.cartography.fine
import src.elements.s3_parameters as s3p

import src.cartography.illustrate


class Interface:

    def __init__(self, connector: boto3.session.Session, s3_parameters: s3p.S3Parameters, instances: pd.DataFrame, reference: pd.DataFrame):

        self.__connector = connector
        self.__s3_parameters = s3_parameters

        self.__instances = instances
        self.__reference = reference

    def __get_coarse_boundaries(self) -> geopandas.GeoDataFrame:

        fine = src.cartography.fine.Fine(
            connector=self.__connector, s3_parameters=self.__s3_parameters).exc()

        return src.cartography.coarse.Coarse(
            reference=self.__reference, fine=fine).exc()


    def __get_data(self, points: int) -> geopandas.GeoDataFrame:

        values = self.__instances.copy().loc[self.__instances['points'] == points, :]

        data = geopandas.GeoDataFrame(
            values,
            geometry=geopandas.points_from_xy(values['longitude'], values['latitude'])
        )
        data.crs = 'epsg:4326'

        return data

    def exc(self, n_catchments_visible: int):
        """

        :param n_catchments_visible:
        :return:
        """

        coarse = self.__get_coarse_boundaries()

        for points in self.__instances['points'].unique():
            data = self.__get_data(points=points)
            src.cartography.illustrate.Illustrate(data=data, coarse=coarse).exc(points=points, n_catchments_visible=n_catchments_visible)

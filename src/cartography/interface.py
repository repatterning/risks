"""Module interface.py"""
import boto3
import geopandas
import pandas as pd

import src.cartography.coarse
import src.cartography.fine
import src.cartography.illustrate
import src.elements.s3_parameters as s3p


class Interface:
    """
    Interface
    """

    def __init__(self, connector: boto3.session.Session, s3_parameters: s3p.S3Parameters,
                 instances: pd.DataFrame, reference: pd.DataFrame):
        """

        :param connector: A boto3 session instance, it retrieves the developer's <default> Amazon
                          Web Services (AWS) profile details, which allows for programmatic interaction with AWS.
        :param s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                              name, buckets, etc.
        :param instances: A frame of metrics per gauge instance, and with respect to time; in the
                          latter case, 1 time point (0.25 hours), 4 time points (1 hour), etc.
        :param reference: An inventory of gauge stations
        """

        self.__connector = connector
        self.__s3_parameters = s3_parameters

        self.__instances = instances
        self.__reference = reference

    def __get_coarse_boundaries(self) -> geopandas.GeoDataFrame:
        """

        :return:
        """

        fine = src.cartography.fine.Fine(
            connector=self.__connector, s3_parameters=self.__s3_parameters).exc()

        return src.cartography.coarse.Coarse(
            reference=self.__reference, fine=fine).exc()


    def __get_data(self, points: int) -> geopandas.GeoDataFrame:
        """

        :param points: 1 -> 0.25 hours, 4 -> 1 hour, etc.
        :return:
        """

        values = self.__instances.copy().loc[self.__instances['points'] == points, :]

        data = geopandas.GeoDataFrame(
            values,
            geometry=geopandas.points_from_xy(values['longitude'], values['latitude'])
        )
        data.crs = 'epsg:4326'

        for field in ['maximum', 'minimum', 'latest', 'median']:
            data[field] = data[field].round(decimals=4)

        return data

    def exc(self, n_catchments_visible: int):
        """

        :param n_catchments_visible: The number of catchment data layers that are visible by default.
        :return:
        """

        coarse = self.__get_coarse_boundaries()

        for points in self.__instances['points'].unique():
            data = self.__get_data(points=points)
            src.cartography.illustrate.Illustrate(data=data, coarse=coarse).exc(
                points=points, n_catchments_visible=n_catchments_visible)

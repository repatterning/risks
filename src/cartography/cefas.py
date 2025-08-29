"""Module cefas.py"""
import io

import boto3
import geopandas

import src.elements.s3_parameters as s3p
import src.s3.unload


class CEFAS:
    """
    Centre for Environment, Fisheries and Aquaculture Science (CEFAS)

    This class reads the river level catchment boundaries of the Scottish Environment Protection Agency; provided
    by CEFAS.
    """

    def __init__(self, connector: boto3.session.Session, s3_parameters: s3p.S3Parameters):
        """

        :param connector:
        :param s3_parameters:
        """

        # An instance for S3 interactions
        self.__s3_client: boto3.session.Session.client = connector.client(
            service_name='s3')

        self.__s3_parameters = s3_parameters

    def exc(self) -> geopandas.GeoDataFrame:
        """

        :return:
        """

        buffer = src.s3.unload.Unload(s3_client=self.__s3_client).exc(
            bucket_name=self.__s3_parameters.internal, key_name='cartography/SEPA.geojson')

        return geopandas.read_file(io.StringIO(buffer))

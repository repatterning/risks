import logging
import geopandas
import boto3
import io

import src.elements.s3_parameters as s3p
import src.s3.unload


class CEFAS:

    def __init__(self, connector: boto3.session.Session, s3_parameters: s3p.S3Parameters):

        # An instance for S3 interactions
        self.__s3_client: boto3.session.Session.client = connector.client(
            service_name='s3')

        self.__s3_parameters = s3_parameters

    def exc(self):

        buffer = src.s3.unload.Unload(s3_client=self.__s3_client).exc(
            bucket_name=self.__s3_parameters.internal, key_name='cartography/SEPA.geojson')

        testing = geopandas.read_file(io.StringIO(buffer))

        logging.info(testing)

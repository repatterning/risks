
import logging

import branca.colormap
import geopandas

import src.cartography.parcels
import src.elements.parcel as pcl


class Illustrate:

    def __init__(self, data: geopandas.GeoDataFrame, coarse: geopandas.GeoDataFrame):

        self.__data = data
        self.__coarse = coarse

    def exc(self):

        parcels: list[pcl.Parcel] = src.cartography.parcels.Parcels(data=self.__data).exc()
        colours: branca.colormap.StepColormap = branca.colormap.linear.YlOrBr_09.to_step(len(parcels))
        logging.info(colours)

        for parcel in parcels:
            logging.info(parcel)

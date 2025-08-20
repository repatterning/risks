"""Module disaggregates.py"""
import json

import pandas as pd


class Disaggregates:
    """
    Creates dictionary nodes by catchment.
    """

    def __init__(self, frame: pd.DataFrame):
        """

        :param frame: Per instance, millimetre per hour metrics and descriptive attributes
        """

        self.__frame = frame
        self.__catchments = self.__get_catchments()

    def __get_catchments(self) -> dict:

        data = self.__frame[['catchment_id', 'catchment_name']].drop_duplicates()
        values = data.set_index(keys='catchment_id')

        return values.to_dict()['catchment_name']
        
    def __get_disaggregate(self, catchment_id: int):
        """

        :param catchment_id: The identification doe of a catchment area.
        :return:
        """

        latest: pd.DataFrame = self.__frame.copy().loc[self.__frame['catchment_id'] == catchment_id, :]
        latest.drop(columns=['catchment_id', 'catchment_name'], inplace=True)
        string = latest.to_json(orient='split')
        values = json.loads(string)
        values['catchment_id'] = int(catchment_id)
        values['catchment_name'] = self.__catchments[catchment_id]
    
        return values

    def __call__(self) -> list[dict]:
        """

        :return:
        """

        codes = self.__frame['catchment_id'].unique()

        computation = [self.__get_disaggregate(catchment_id=code) for code in codes]

        return computation

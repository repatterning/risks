
import json

import pandas as pd


class Disaggregates:

    def __init__(self, frame: pd.DataFrame):
        """

        :param frame:
        """

        self.__frame = frame
        
    def __get_disaggregate(self, catchment_id: int):
        """

        :param catchment_id:
        :return:
        """

        latest = self.__frame.copy().loc[self.__frame['catchment_id'] == catchment_id, :]
        string = latest.to_json(orient='split')
        values = json.loads(string)
    
        return values

    def __call__(self) -> list[dict]:
        """

        :return:
        """

        codes = self.__frame['catchment_id'].unique()

        computation = [self.__get_disaggregate(catchment_id=code) for code in codes]

        return computation

import logging

import pandas as pd


class Ranking:

    def __init__(self):
        pass

    def __rankings(self, data: pd.DataFrame) -> pd.DataFrame:
        """

        :return:
        """

        frame = data.copy()[['catchment_id', 'catchment_name', 'latest']].groupby(
            by=['catchment_id', 'catchment_name']).agg(maximum=('latest', 'max'))
        logging.info(frame)

        # Convert 'catchment_id' & 'catchment_name' to fields; currently indices.
        frame.reset_index(drop=False, inplace=True)

        # Hence
        frame['rank'] = frame['maximum'].rank(method='first', ascending=False).astype(int)
        frame.drop(columns='maximum', inplace=True)
        frame.sort_values(by='catchment_name', inplace=True)
        frame.reset_index(drop=True, inplace=True)
        logging.info(frame)

        return frame

    def exc(self, instances: pd.DataFrame):
        """

        :param instances:
        :return:
        """


        __points = instances['points'].unique()
        logging.info(__points)

        for points in __points:
            data = instances.copy().loc[instances['points'] == points, :]
            rankings = self.__rankings(data=data)
            frame = data.merge(rankings, how='left', on=['catchment_id'])
            logging.info(frame)

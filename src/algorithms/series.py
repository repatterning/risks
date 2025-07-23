import json
import os

import pandas as pd

import config
import src.elements.partitions as pr
import src.functions.directories
import src.functions.objects


class Series:
    """
    For writing series
    """

    def __init__(self):
        """
        Constructor
        """

        self.__configurations = config.Config()
        self.__objects = src.functions.objects.Objects()

        # Storage path
        self.__path = os.path.join(self.__configurations.points_, 'series')

        # Ensure the storage area exists
        src.functions.directories.Directories().create(path=self.__path)

    def exc(self, gamma: pd.DataFrame, partition: pr.Partitions) -> None:
        """

        :param gamma:
        :param partition:
        :return:
        """

        string = gamma.to_json(orient='split')
        nodes = json.loads(string)
        nodes['attributes'] = partition._asdict()

        self.__objects.write(
            nodes=nodes, path=os.path.join(self.__configurations.points_, f'{partition.ts_id}.json'))

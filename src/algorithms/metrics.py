
import numpy as np
import pandas as pd


class Metrics:

    def __init__(self, arguments: dict):

        self.__arguments = arguments

        self.__tau: np.ndarray = np.array([self.__arguments.get('tau')], dtype=float)
        frequency: float = self.__arguments.get('frequency')
        self.__points: np.ndarray = (self.__tau / frequency).astype(int)

    def __rates(self, frame: pd.DataFrame) -> pd.DataFrame:

        # differences
        differences_ = [frame['measure'].diff(i).to_frame(name=i) for i in self.__points]
        differences = pd.concat(differences_, axis=1, ignore_index=False)

        # delta measure / delta time
        rates = pd.DataFrame(data=np.true_divide(differences.to_numpy(), self.__tau), columns=self.__points)

        return rates

    def __fractional_delta(self, frame: pd.DataFrame) -> pd.DataFrame:

        # delta measure / original measure
        fractions_ = [frame['measure'].pct_change(i).to_frame(name=i) for i in self.__points]
        fractions = pd.concat(fractions_, axis=1, ignore_index=False)

        return fractions

    def exc(self, frame: pd.DataFrame):
        pass

from abc import ABC, abstractmethod
import pandas as pd


class Repository(ABC):

    @abstractmethod
    def get_dataframe(self, storage_name: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def store_dataframe(self, df: pd.DataFrame, storage_name: str) -> None:
        pass

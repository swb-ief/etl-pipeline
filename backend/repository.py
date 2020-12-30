from abc import ABC, abstractmethod
import pandas as pd


class Repository(ABC):
    """ Abstract base class representing a data repository. Allowing us to swap out one repository for an other with
    little effort. For instance when we move from google sheets to say an sql cloud database, etc.."""

    @abstractmethod
    def get_dataframe(self, storage_name: str) -> pd.DataFrame:
        """ get a dataframe from the repository based on its storage name"""
        pass

    @abstractmethod
    def store_dataframe(self, df: pd.DataFrame, storage_name: str) -> None:
        """ store a dataframe in the repository with given storage name"""
        pass

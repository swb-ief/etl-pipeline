from abc import ABC, abstractmethod
import pandas as pd


class Repository(ABC):
    """ Abstract base class representing a data repository. Allowing us to swap out one repository for an other with
    little effort. For instance when we move from google sheets to say an sql cloud database, etc.."""

    @abstractmethod
    def get_dataframe(self, storage_location: str) -> pd.DataFrame:
        """ get a dataframe from the repository based on its storage location"""
        pass

    @abstractmethod
    def store_dataframe(self, df: pd.DataFrame, storage_location: str, allow_create: bool) -> None:
        """ store a dataframe in the repository with given storage location"""
        pass

    @abstractmethod
    def exists(self, storage_location: str) -> bool:
        """ check if a storage location exists """
        pass

    @abstractmethod
    def create_storage_location(self, storage_location: str) -> None:
        """ Creates the storage location for future use. This could internaly be an empty file or empty table, etc.."""
        pass

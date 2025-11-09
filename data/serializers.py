from abc import abstractmethod
from typing import Protocol, runtime_checkable

@runtime_checkable
class DataSerializer[TData](Protocol):
    @abstractmethod
    def get_file_extension(self) -> str:
        raise NotImplementedError
    @abstractmethod
    def load(self) -> TData:
        raise NotImplementedError
    @abstractmethod
    def save(self, data: TData):
        raise NotImplementedError

class DefaultSerializer:
    def get_file_extension(self) -> str:
        raise

    def load(self):
        raise
    
    def save(self, data):
        raise

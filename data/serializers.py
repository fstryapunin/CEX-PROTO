from abc import abstractmethod
from pathlib import Path
from typing import Protocol, runtime_checkable

@runtime_checkable
class DataSerializer[TData](Protocol):
    @abstractmethod
    def get_file_extension(self) -> str:
        raise NotImplementedError
    @abstractmethod
    def load(self, path: Path) -> TData:
        raise NotImplementedError
    @abstractmethod
    def save(self, path: Path, data: TData):
        raise NotImplementedError

class DefaultSerializer:
    def get_file_extension(self) -> str:
        raise

    def load(self, path):
        raise
    
    def save(self, path, data):
        raise

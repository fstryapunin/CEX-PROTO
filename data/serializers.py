from abc import abstractmethod
import json
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

class JsonSerializer:
    def get_file_extension(self) -> str:
        return ".json"
    
    def load(self, path):
        if path.is_file():
            with open(path, 'r', encoding='utf-8') as data:
                return json.load(data)
        else: raise Exception(f"File not found at {path}")
    
    def save(self, path: Path, data):
        path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)


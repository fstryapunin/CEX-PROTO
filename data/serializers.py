from abc import abstractmethod
import csv
import json
from pathlib import Path
import pickle
from typing import Protocol, runtime_checkable

import yaml

from execution.common import RuntimeException

@runtime_checkable
class DataSerializer[TData](Protocol):
    @abstractmethod
    def get_file_extension(self) -> str:
        raise NotImplementedError
    @abstractmethod
    def matches_file(self, extension: str):
        raise NotImplementedError
    @abstractmethod
    def load(self, path: Path) -> TData:
        raise NotImplementedError
    @abstractmethod
    def save(self, path: Path, data: TData):
        raise NotImplementedError

class PickleSerializer:
    
    def get_file_extension(self) -> str:
        return ".pkl"
    
    def matches_file(self, extension: str):
        return extension == ".pkl" or extension == ".pickle"
    
    def load(self, path: Path):
        if not path.is_file():
            raise Exception(f"File not found at {path}")
            
        with open(path, 'rb') as file:
            return pickle.load(file)
    
    def save(self, path: Path, data):
        path.parent.mkdir(exist_ok=True, parents=True)
        
        with open(path, 'wb') as file:
            pickle.dump(data, file)

class CsvSerializer:
    def get_file_extension(self) -> str:
        return ".csv"
    
    def matches_file(self, extension: str):
        return extension == ".csv"

    def load(self, path: Path):
        if not path.is_file():
            raise RuntimeException(f"File not found at {path}")
        
        data_list = []
        with open(path, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data_list.append(dict(row))
        return data_list
    
    def save(self, path: Path, data):
        path.parent.mkdir(exist_ok=True, parents=True)
        fieldnames = list(data[0].keys())
        
        with open(path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader() 
            writer.writerows(data)

class PlainFileSerializer:
    def get_file_extension(self) -> str:
        return ".txt"

    def matches_file(self, extension: str):
        return extension == "txt"
    
    def load(self, path: Path) -> str:
        if not path.is_file():
            raise RuntimeException(f"File not found at {path}")
            
        with open(path, 'r', encoding='utf-8') as file:
            return file.read()
    
    def save(self, path: Path, data):
        path.parent.mkdir(exist_ok=True, parents=True)
        
        if isinstance(data, str):
            with open(path, 'w', encoding='utf-8') as file:
                file.write(data)
        elif isinstance(data, bytes):
            with open(path, 'wb') as file:
                file.write(data)
        else:
            with open(path, 'w', encoding='utf-8') as file:
                file.write(str(data))


class JsonSerializer:
    def get_file_extension(self) -> str:
        return ".json"
    
    def matches_file(self, extension: str):
        return extension == "json"
    
    def load(self, path):
        if path.is_file():
            with open(path, 'r', encoding='utf-8') as data:
                return json.load(data)
        else: raise RuntimeException(f"File not found at {path}")
    
    def save(self, path: Path, data):
        path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)

class YamlSerializer:
    def get_file_extension(self) -> str:
        return ".yaml"
    
    def matches_file(self, extension: str):
        return extension == "yml" or extension == "yaml"

    def load(self, path):
        if path.is_file():
            with open(path, "r") as f:
                return yaml.safe_load(f)
        else: raise RuntimeException(f"File not found at {path}")
    
    def save(self, path: Path, data):
        with open(path, "w") as f:
            return yaml.safe_dump(data, f)


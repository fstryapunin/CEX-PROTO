from enum import Enum, IntEnum
from pathlib import Path
from typing import Any, Self
import uuid
from typing_validation import is_valid

from execution.utils import get_file_hash

class ValidationException(Exception):
    def __init__(self, messages: list[str], *args: object) -> None:
        self.messages = messages
        super().__init__(*args)

class RuntimeException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
    
    def to_validaton_exception(self) -> ValidationException:
        return ValidationException([], self.args)

class ExecutionState(Enum):
    UNINITIALIZED = 1
    VALIDATED = 2
    READY = 3
    SKIPPED = 4
    RUNNING = 5
    EXECUTED = 6
    ERROR = 7
    INVALID = 8

class DataMatch(IntEnum):
    NAME_AND_ANNOTATION = 3
    NAME = 2
    ANNOTATION = 1
    NONE = 0

DATA_MACTH_NAMES = { 
    DataMatch.NAME_AND_ANNOTATION: "name and annotation",
    DataMatch.NAME: "name",
    DataMatch.ANNOTATION: "type annotation",
    DataMatch.NONE: "none"
}

class DataInformation:
    def __init__(self, name: str, type: type | None, path: Path | None = None) -> None:
        self.id = uuid.uuid4()
        self.name = name
        self.type = type
        self.path = path
        self.hash: str | None = None
        self.value: Any = None

    def match_static(self, other: Self, name_aliases: list[str] = []):
        name_matches = other.name == self.name or other.name in name_aliases
        type_matches = other.type == self.type

        return name_matches and type_matches
    
    def get_match(self, other: Self, name_aliases: list[str] = []) -> DataMatch:
        name_matches = other.name == self.name or other.name in name_aliases
        type_matches = other.type is None or self.type is None or other.type == self.type

        if name_matches and type_matches:
            return DataMatch.NAME_AND_ANNOTATION

        if name_matches:
            return DataMatch.NAME

        if type_matches:
            return DataMatch.ANNOTATION
               
        return DataMatch.NONE
    
    def get_best_match(self, others: list[Self], duplicate_message: str, name_aliases: list[str] = []) -> Self | None:
        best_score = DataMatch.NONE
        best_value = None

        for option in others:
            match = self.get_match(option, name_aliases)
            if best_score == match and best_score > DataMatch.NONE:
                raise RuntimeException(f"{duplicate_message}: {[best_value, option]}, match type {DATA_MACTH_NAMES[best_score]}")
            
            if best_score < int(match):
                best_value = option

        return best_value
        

    def update_hash(self):
        self.hash = self.get_hash()
        return self

    def get_hash(self) -> str | None:
        if self.path is not None:
            return get_file_hash(self.path)

    def with_value(self, value: Any, type_exception_message: str):
        if self.type is not None and not is_valid(value, self.type):
            raise RuntimeException(type_exception_message)
        
        self.value = value

        return self
    
    def with_path(self, path: Path):
        self.path = path
        return self

    def __str__(self) -> str:
        return f"name: {self.name}, type: {self.type}"   

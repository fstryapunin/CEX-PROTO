from enum import Enum
from pathlib import Path
from typing import Any, Self

from execution.utils import get_file_hash
from execution.validation import ValidationException

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

class DataInformation:
    def __init__(self, name: str, type: type | None, path: Path | None) -> None:
        self.name = name
        self.type = type
        self.path = path
        self.hash: str | None = None
        self.value: Any = None

    def match_static(self, other: Self, name_aliases: list[str] = []):
        name_matches = other.name == self.name or self.name in name_aliases
        type_matches = other.type is None or self.type is None or other.type == self.type

        return name_matches and type_matches

    # "lazy loaded" as file hashing can be expensive
    def with_hash(self):
        if self.path is not None:
            self.hash = get_file_hash(self.path)

        return self
    
    def with_value(self, value: Any, type_exception_message: str | None = None):
        value_type = type(value)

        if self.type is not None and value_type != self.type:
            message = type_exception_message if type_exception_message is not None else f"Value of invalid type provied. Expected {self.type}, got {value_type}"
            raise RuntimeException(message)

        self.value = value
        return self

    def __str__(self) -> str:
        return f"name: {self.name}, type: {self.type}"   

import uuid
from typing import Callable, Self
import hashlib

from data.serializers import DataSerializer

class Node:
    
    #region Creation Methods

    def __init__(
            self, 
            function: Callable, 
            *, 
            name: str,
            input_directory_name: str | None = None,
            input_serializers: dict[str, DataSerializer] | DataSerializer | None = None,
            output_directory_name: str | None = None,
            is_cached: bool = True,
            input_aliases: dict[str, list[str] | str] | None = None,
            output_serializer: DataSerializer | None = None,
            output_name: str | None = None
        ) -> None:
        
        self.runtime_id = uuid.uuid4()
        self.name = name
        self.function = function
        self.is_cached = is_cached
        self.input_directory_name = input_directory_name
        self.input_aliases = input_aliases
        self.input_serializers = input_serializers
        self.output_name = output_name
        self.output_serializer = output_serializer
        self.output_directory = f"{output_directory_name}" if output_directory_name is not None else self.name
        self.subsequent_nodes: list[Self] = []

    def continue_with(self, arg: Self | list[Self]) -> Self:
        if isinstance(arg, list):
            self.subsequent_nodes += arg
            return self
        if isinstance(arg, Node):
            self.subsequent_nodes.append(arg)
            return self

        raise Exception(f"Wrong argument type provided to method ContinueWith: {type(arg)}")
    
    #endregion

    #region Methods

    def get_persistent_hash(self) -> str:
        def get_stable_input_aliases():
            if self.input_aliases is None:
                return None
            if isinstance(self.input_aliases, dict):
                sorted_items = sorted(self.input_aliases.items())
                return str(sorted_items)
            return str(self.input_aliases)

        func_name = f"{self.function.__module__}.{self.function.__qualname__}"

        stable_attrs = (
            self.name,
            self.is_cached,
            func_name,
            self.output_name,
            str(self.input_directory_name) if self.input_directory_name else None,
            self.output_directory,
            get_stable_input_aliases(),
        )

        stable_representation = "|".join(
            [str(attr) for attr in stable_attrs if attr is not None]
        ).encode('utf-8')

        return hashlib.sha256(stable_representation).hexdigest()


    def get_subsequent_nodes(self) -> list[Self]:
        return self.subsequent_nodes

    #endregion

    #region Overrides

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Node):
            return False
        
        return self.runtime_id == value.runtime_id
    
    def __hash__(self) -> int:
        return hash(self.runtime_id)
    
    def __str__(self) -> str:
        return self.name
    
    #endregion
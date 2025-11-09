from enum import Enum
import inspect
from pathlib import Path
import uuid
from typing import Any, Callable, Self
import hashlib

from data.serializers import DataSerializer

class NodeState(Enum):
    UNINITIALIZED = 1
    VALIDATED = 2
    READY = 3
    SKIPPED = 4
    RUNNING = 5
    EXECUTED = 6
    ERROR = 7

class Node:
    #region Creation Methods

    def __init__(
            self, 
            function: Callable, 
            *, 
            name: str,
            input_directory_name: str | None,
            input_serializers: dict[str, DataSerializer] | DataSerializer | None = None,
            output_directory_name: str | None = None,
            is_cached: bool = False,
            input_aliases: dict[str, list[str] | str] | None = None,
            output_serializers: dict[str, DataSerializer] | DataSerializer | None = None,
            outputs: str | list[str] | None = None
        ) -> None:
        
        self.runtime_id = uuid.uuid4()
        self.state = NodeState.UNINITIALIZED
        self.name = name
        self.function = function
        self.is_cached = is_cached
        self.input_directory_name = input_directory_name
        self.input_aliases = input_aliases
        self.input_serializers = input_serializers
        self.outputs = outputs
        self.output_serializers = output_serializers
        self.output_directory = f"{output_directory_name}" if output_directory_name is not None else self.name
        self.subsequent_nodes: list[Self] = []

    def continue_with(self, arg: Self | list[Self]) -> Self:
        if isinstance(arg, list):
            self.subsequent_nodes += arg
            return self
        if isinstance(arg, Self):
            self.subsequent_nodes.append(arg)
            return self

        raise Exception(f"Wrong argument type provided to method ContinueWith: {type(arg)}")
    
    #endregion

    #region Execution Methods

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
            self.outputs,
            str(self.input_directory_name) if self.input_directory_name else None,
            self.output_directory,
            get_stable_input_aliases(),
        )

        stable_representation = "|".join(
            [str(attr) for attr in stable_attrs if attr is not None]
        ).encode('utf-8')

        return hashlib.sha256(stable_representation).hexdigest()

    def __get_serializer(self, serializers: dict[str, DataSerializer] | DataSerializer | None, name: str) -> DataSerializer | None:
        if isinstance(serializers, DataSerializer) or serializers == None : 
            return serializers
        
        return serializers.get(name)      

    def get_output_serializer(self, name: str) -> DataSerializer | None:        
        return self.__get_serializer(self.output_serializers, name)

    def get_input_serializer(self, name: str) -> DataSerializer | None:        
        return self.__get_serializer(self.input_serializers, name)

    def get_required_inputs(self) -> list[tuple[str, Any]]:
        annotations = self.function.__annotations__
        return [(key, annotations[key]) for key in annotations.keys()]

    def get_inputs_aliases(self, input_name: str) -> list[str]:
        if self.input_aliases is None:
            return [input_name]
        
        aliases = self.input_aliases[input_name]

        if aliases is None:
            return [input_name]

        if isinstance(aliases, str):
            return [aliases]

        return aliases 

    def get_available_outputs(self) -> list[tuple[str, Any]] | None:
        if self.outputs is None:
            return
        
        return_annotations = inspect.signature(self.function).return_annotation

        if isinstance(self.outputs, str):
            return [(self.outputs, return_annotations)]

        return [(self.outputs[i], return_annotations[i]) for i in range(len(self.outputs))]

    def execute(self, **kwargs):
        bounded_args = inspect.signature(self.function).bind(None, kwargs)
        bounded_args.apply_defaults()
        return self.function(*bounded_args.args, **bounded_args.kwargs)
    
    #endregion

    #region Overrides

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Node):
            return False
        
        return self.runtime_id == value.runtime_id
    
    def __hash__(self) -> int:
        return hash(self.runtime_id)
    
    #endregion
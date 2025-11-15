from __future__ import annotations

import inspect
from pathlib import Path

from data.serializers import DataSerializer
from execution.common import DataInformation, ExecutionState, RuntimeException
from execution.utils import get_file_hash
from meta.meta import MetadataProvider, NodeMeta
from pipeline.node import Node
import execution.namespace

class NodeExecutor:
    def __init__(self, node: Node, namespace: execution.namespace.NamespaceExecutor, meta_provider: MetadataProvider) -> None:
        self.node = node
        self.meta_metaprovider = meta_provider
        self.state = ExecutionState.UNINITIALIZED
        self.parent = namespace
    
    @property
    def runtime_id(self):
        return self.node.runtime_id
    
    @property
    def name(self):
        return self.node.name

    @property
    def subsequent_nodes(self):
        return self.node.subsequent_nodes

    @property 
    def subsequent_node_ids(self):
        return [node.runtime_id for node in self.node.subsequent_nodes]

    @property
    def is_cached(self):
        return self.node.is_cached

    #region IO

    def get_available_file_inputs(self) -> list[DataInformation]:
        if self.node.input_directory_name is None:
            return []
        
        path = self.resolve_path(self.node.input_directory_name)

        inputs = list(path.iterdir())
        
        return [DataInformation(input.stem, None, input) for input in inputs]
    
    def get_required_inputs(self) -> list[DataInformation]:
        parameters = inspect.signature(self.node.function).parameters

        return [DataInformation(parameter[0], parameter[1].annotation, None) for parameter in parameters.items()]

    def get_input_aliases(self, input_name: str) -> list[str]:
        if self.node.input_aliases is None:
            return [input_name]
        
        aliases = self.node.input_aliases[input_name]

        if aliases is None:
            return [input_name]

        if isinstance(aliases, str):
            return [aliases]

        return aliases

    def resolve_input_serializer(self, input: DataInformation) -> DataSerializer:
        if isinstance(self.node.input_serializers, DataSerializer):
            return self.node.input_serializers

        serializer: DataSerializer | None = None
        if self.node.input_serializers is None:
            serializer = self.parent.resolve_serializer(input)
        elif input.name in self.node.input_serializers:
            serializer = self.node.input_serializers[input.name]

        if serializer is None:
            raise RuntimeException(f"Failed to resolve serializer for input: {input} of node: {self.name}")

        return serializer
        
    def resolve_output_serializer(self, info: DataInformation) -> DataSerializer:
        if self.node.output_serializer:
            return self.node.output_serializer
        
        serializer = None
        serializer = self.parent.resolve_serializer(info)

        if serializer is not None:
            return serializer
        
        raise Exception(f"Failed to resolve output serializer for node {self.node.name}")

    def resolve_path(self, directory: str) -> Path:
        return self.parent.resolve_path(directory)


    def get_output_information(self) -> DataInformation | None:
        signature = inspect.signature(self.node.function)

        if signature.return_annotation or self.node.output_name is None: return
        
        information = DataInformation(self.node.output_name, signature.return_annotation)

        path = None
        if self.node.is_cached:
            serializer = self.resolve_output_serializer(information)
            path = self.resolve_path(self.node.output_directory) / (self.node.output_name + serializer.get_file_extension())
            return information.with_path(path)
        
    #region Execution

    def resolve_value(self, info: DataInformation):
        if info.value is not None:
            return info.value
        
        if info.path is not None:
            serializer = self.resolve_input_serializer(info)
            return serializer.load(info.path)
        
        raise RuntimeException(f"Failed to resolve value for input {info.name} of node: {self.name}. Neither value or path was provided")

    def execute(self, args: dict[str, DataInformation]):
        input_values = {key: self.resolve_value(info) for key, info in args.items()}

        bounded_args = inspect.signature(self.node.function).bind(**input_values)
        bounded_args.apply_defaults()

        value = self.node.function(*bounded_args.args, **bounded_args.kwargs)

        output = self.get_output_information()
        
        if output is None or output.path is None:
            return
        
        result = output.with_value(value, f"Node {self.name} produced output of invalid type. Expected {output.type}, got {type(value)}")

        if self.is_cached:
            serializer = self.resolve_output_serializer(result)
            serializer.save(output.path, result)
            hash = get_file_hash(output.path)

            if hash is None:
                raise RuntimeException(f"Failed to hash output of node {self.name}")
            
            self.meta.update_output_hash(hash)
            self.meta_metaprovider.sync()

        return result

    #endregion

    #region Meta

    @property
    def meta(self) -> NodeMeta:
        namespace = self.meta_metaprovider.data.get_namespace(self.parent.namespace.name)

        if namespace is None:
            raise Exception("Missing namespace meta for " + self.parent.namespace.name)
        
        meta = namespace.get_node_meta(self.node.get_persistent_hash())

        if meta is None:
            raise Exception("Missing node meta for " + self.node.name)
        
        return meta

    #endregion

    #region State

    def set_state(self, state: ExecutionState):
        self.state = state

    #endregion

    #endregion
    
    #region Overrides

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, NodeExecutor):
            return False
        
        return self.node == value.node
    
    def __hash__(self) -> int:
        return hash(self.node.__hash__())
    
    #endregion
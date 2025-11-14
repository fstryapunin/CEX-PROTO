import inspect
from pathlib import Path

from data.serializers import DataSerializer
from execution.common import DataInformation
from execution.executor import NamespaceExecutor
from execution.validation import ValidationMessages
from meta.meta import MetadataProvider, NodeMeta
from pipeline.node import Node, NodeState

import itertools

class NodeExecutor:
    def __init__(self, node: Node, namespace: NamespaceExecutor, meta_provider: MetadataProvider) -> None:
        self.node = node
        self.meta_metaprovider = meta_provider
        self.state = NodeState.UNINITIALIZED
        self.parent = namespace
    
    @property
    def runtime_id(self):
        return self.node.runtime_id
    
    @property
    def subsequent_nodes(self):
        return self.node.subsequent_nodes

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

    def resolve_output_serializer(self) -> DataSerializer:
        if self.node.output_serializer:
            return self.node.output_serializer
        
        serializer = None
        output_type = inspect.signature(self.node.function).return_annotation
        serializer = self.parent.resolve_serializer(output_type)

        if serializer is not None:
            return serializer
        
        raise Exception(f"Failed to resolve output serializer for node {self.node.name}")

    def resolve_path(self, directory: str) -> Path:
        return self.parent.resolve_path(directory)


    def get_output_information(self) -> DataInformation | None:
        signature = inspect.signature(self.node.function)

        if signature.return_annotation or self.node.output_name is None: return

        path = None
        if self.node.is_cached:
            serializer = self.resolve_output_serializer()
            path = self.resolve_path(self.node.output_directory) / (self.node.output_name + serializer.get_file_extension())
        
        return DataInformation(self.node.output_name, signature.return_annotation, path)

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

    #region Validation

    def validate(self) -> tuple[bool, list[str]]:
        valdiation_messages = []

        if not isinstance(self.node.name, str):
            valdiation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("name", type(self.node.name), self.node))

        if not callable(self.node.function):
            valdiation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("function", type(self.node.function), self.node))

        if self.node.output_name is not None and not isinstance(self.node.output_name, str):
            valdiation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("output name", type(self.node.output_name), self.node))

        if self.node.input_directory_name is not None and not isinstance(self.node.input_directory_name, str):
            valdiation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("input directory name", type(self.node.input_directory_name), self.node))

        if not isinstance(self.node.output_directory, str):
            valdiation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("output directory name", type(self.node.output_directory), self.node))

        if not isinstance(self.node.is_cached, bool):
            valdiation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("is cached", type(self.node.is_cached), self.node))

        allowed_parameter_kinds = {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
        signature = inspect.signature(self.node.function).parameters.items()

        if any([param[1].kind not in allowed_parameter_kinds  for param in signature]):
            valdiation_messages.append(f"Invalid function provided to node {self.node.name}, position-only parameters are not allowed")

        if self.node.input_aliases is not None:
            flat_aliases = list(itertools.chain.from_iterable(self.node.input_aliases))

            for alias in flat_aliases:
                if not isinstance(alias, str):
                    valdiation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("input alias", type(alias), self.node))

            if len(flat_aliases) != len(set(flat_aliases)):
                valdiation_messages.append(f"Duplicate input aliases were passed to node {self.node.name}")

        if self.node.output_serializer is not None and not isinstance(self.node.output_serializer, DataSerializer) is not None:
            valdiation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("output serializer", type(self.node.output_serializer), self.node, ValidationMessages.InvalidSerializer()))

        if self.node.input_serializers is not None:
            if isinstance(self.node.input_serializers, list):
                for serializer in self.node.input_serializers:
                    if not isinstance(serializer, DataSerializer):
                        valdiation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("input serializer", type(serializer), self.node, ValidationMessages.InvalidSerializer()))

            if not isinstance(self.node.input_serializers, DataSerializer):
                valdiation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("input serializer", type(self.node.input_serializers), self.node, ValidationMessages.InvalidSerializer()))

        output_info = self.get_output_information()

        if output_info is not None:
            if output_info.type == inspect._empty:
                valdiation_messages.append(f"Function output must be annotated with type hints if function produces an output.")
        
        is_valid = len(valdiation_messages) == 0

        if not is_valid:
            self.state = NodeState.INVALID

        return is_valid, valdiation_messages

    #endregion
    
    #region Overrides

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, NodeExecutor):
            return False
        
        return self.node == value.node
    
    def __hash__(self) -> int:
        return hash(self.node.__hash__())
    
    #endregion
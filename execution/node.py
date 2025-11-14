import inspect


from data.serializers import DataSerializer
from execution.validation import ValidationMessages
from meta.meta import NodeMeta
from pipeline.node import Node, NodeState

import itertools

class OutputInformation:
    def __init__(self, name: str, type: type) -> None:
        self.name = name
        self.type = type

class NodeExecutor:
    def __init__(self, node: Node, meta: NodeMeta, serializer: DataSerializer | None) -> None:
        self.node = node
        self.meta = meta
        self.state = NodeState.UNINITIALIZED
        self.serializer = serializer
    
    def get_output_information(self) -> OutputInformation | None:
        signature = inspect.signature(self.node.function)
        if self.node.output_name is None: return
        return OutputInformation(self.node.output_name, signature.return_annotation)

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
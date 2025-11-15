import inspect
import itertools
from pathlib import Path
from data.serializers import DataSerializer
from execution.common import DataInformation
from execution.utils import dfs
from pipeline.namespace import Namespace
from pipeline.node import Node

class ValidationException(Exception):
    def __init__(self, messages: list[str], *args: object) -> None:
        self.messages = messages
        super().__init__(*args)

class ValidationMessages:
    @staticmethod
    def InvalidSerializer():
        return "Serializers must implement protocol DataSerializer"

    @staticmethod
    def InvalidArgumentTypeProvidedToNode(name: str, type: type, node: Node | str, additional: str | None = None):
        message = f"Invalid argument type: {type} provided to parameter: {name} of node: {node}"
        if additional is None:
            return message
        return message + " " + additional
    
    @staticmethod
    def InvalidArgumentTypeProvidedToNamespace(name: str, type: type, namespace: Namespace | str, additional: str | None = None):
        message = f"Invalid argument type: {type} provided to parameter: {name} of namespace: {namespace}"
        if additional is None:
            return message
        return message + " " + additional
    
    @staticmethod
    def NotAnInstanceOf(type: type, type_name: str):
        return f"{type} is not an instance of {type_name}"
    
    @staticmethod
    def CannotSatisfyInput(node: Node | str, namespace: Namespace | str, input: DataInformation):
        return f"No suitable input was found for input: {input.name} of type {input.type} of node {node} in namespace {namespace}"
    
    @staticmethod
    def AmbiguosInputs(node: Node | str, namespace: Namespace | str, input: DataInformation):
        return f"Ambiguous inputs detect for input: {input.name} of type {input.type} of node {node} in namespace {namespace}"
    
class NodeValidator:
    @staticmethod
    def validate(node: Node) -> tuple[bool, list[str]]:
        validation_messages = []

        if not isinstance(node, Node):
            validation_messages.append(ValidationMessages.NotAnInstanceOf(type(node), Node.__name__))
            return False, validation_messages

        if not isinstance(node.name, str):
            validation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("name", type(node.name), ""))

        if not callable(node.function):
            validation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("function", type(node.function), node))

        if node.output_name is not None and not isinstance(node.output_name, str):
            validation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("output name", type(node.output_name), node))

        if node.input_directory_name is not None and not isinstance(node.input_directory_name, str):
            validation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("input directory name", type(node.input_directory_name), node))

        if not isinstance(node.output_directory, str):
            validation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("output directory name", type(node.output_directory), node))

        if not isinstance(node.is_cached, bool):
            validation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("is cached", type(node.is_cached), node))

        allowed_parameter_kinds = {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
        
        signature = inspect.signature(node.function)
        parameters = signature.parameters.items()

        if any([param[1].kind not in allowed_parameter_kinds for param in parameters]):
            validation_messages.append(f"Invalid function provided to node {node.name}, position-only parameters are not allowed")

        if any([param[1].annotation == inspect._empty for param in parameters]):
            validation_messages.append(f"Function parameters must be type hinted in {node.name}")

        if node.input_aliases is not None:
            flat_aliases = list(itertools.chain.from_iterable(node.input_aliases))

            for alias in flat_aliases:
                if not isinstance(alias, str):
                    validation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("input alias", type(alias), node))

            if len(flat_aliases) != len(set(flat_aliases)):
                validation_messages.append(f"Duplicate input aliases were passed to node {node.name}")

        if node.output_serializer is not None and not isinstance(node.output_serializer, DataSerializer) is not None:
            validation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("output serializer", type(node.output_serializer), node, ValidationMessages.InvalidSerializer()))

        if node.input_serializers is not None:
            if isinstance(node.input_serializers, list):
                for serializer in node.input_serializers:
                    if not isinstance(serializer, DataSerializer):
                        validation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("input serializer", type(serializer), node, ValidationMessages.InvalidSerializer()))

            if isinstance(node.input_serializers, DataSerializer) and len(signature.parameters.items()) > 1:
                validation_messages.append(f"Node {node} has multiple inputs but only one serializer was provided. To specify serializer for a specific input, provide a dictionary.")

            if not isinstance(node.input_serializers, DataSerializer):
                validation_messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNode("input serializer", type(node.input_serializers), node, ValidationMessages.InvalidSerializer()))


        if node.output_name is not None and signature.return_annotation == inspect._empty:
            validation_messages.append(f"Function output must be annotated with type hints if function produces an output.")
        
        return len(validation_messages) == 0, validation_messages
    
class NamespaceValidator:
    @staticmethod
    def validate(namespace: Namespace):
        messages: list[str] = []

        if not isinstance(namespace, Namespace):
            messages.append(ValidationMessages.NotAnInstanceOf(type(namespace), Namespace.__name__))
            is_nodes_valid = False

            return is_nodes_valid, messages

        if not isinstance(namespace.name, str):
            messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNamespace(namespace.name, type(namespace.name), ""))

        if not isinstance(namespace.root_path, Path):
            messages.append(ValidationMessages.InvalidArgumentTypeProvidedToNamespace(namespace.root_path, type(namespace.name), namespace))

        if len(namespace.root_nodes) == 0:
            messages.append(f"No root nodes provided to namespace {namespace.name}")

        def callback(node: Node, next: list[Node]):
            nonlocal messages 
            _, node_messages = NodeValidator.validate(node)
            messages += node_messages

        dfs(namespace.root_nodes, Node.get_subsequent_nodes, callback)

        if len(messages) > 0:
            raise ValidationException(messages)
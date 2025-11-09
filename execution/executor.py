
from collections import defaultdict, deque
import hashlib
import inspect
import os
from pathlib import Path
from typing import Any, Callable
import uuid
from venv import logger


from data.serializers import DataSerializer, DefaultSerializer
from meta.meta import ExecutionMetadataHandler
from pipeline.namespace import Namespace
from pipeline.node import Node, NodeState
import networkx as nx
import logging

import itertools


def get_file_hash(file_path: Path, chunk_size=8192) -> str | None:
    if not os.path.exists(file_path):
        return None

    hash_func = hashlib.new('sha256')
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hash_func.update(chunk)
    
    return hash_func.hexdigest()

class NamespaceExecutor:
    def __init__(self, namespace: Namespace, meta_handler: ExecutionMetadataHandler, default_serializer: DataSerializer) -> None:
        self.namespace = namespace
        self.meta_handler = meta_handler
        self.node_execution_states: dict[uuid.UUID, NodeState] = dict()
        self.serializers: dict[type, DataSerializer] = dict()
        self.default_serializer: DataSerializer = default_serializer

    #region Serialization

    def get_serializer(self, node_function: Callable[[str], DataSerializer | None], name: str, type: type) -> DataSerializer | None:
        serializer = node_function(name)

        if serializer is not None:
            return serializer
        
        if self.serializers.__contains__(type):
            return self.serializers[type]

        if self.default_serializer is not None:
            return self.default_serializer
        
    def resolve_serializer(self, node, node_function: Callable[[str], DataSerializer | None], output_name: str, output_type: type) -> DataSerializer:
        serializer = self.get_serializer(node_function, output_name, output_type)

        if serializer is not None:
            return serializer
        
        logger.fatal(f"No serializer could be found for type {output_type} of {output_name} in node {node.name}")
        raise

    def resolve_output_serializer(self, node: Node, output_name: str, output_type: type):
        return self.resolve_serializer(node, node.get_output_serializer, output_name, output_type)

    def resolve_input_serializer(self, node: Node, input_name: str, input_type: type):
        return self.resolve_serializer(node, node.get_input_serializer, input_name, input_type)

    #endregion

    # needed later for proper namespacing
    #region Path handling

    def resolve_output_directory_path(self, output_dir: str) -> Path:
        return Path(output_dir)

    def resolve_input_directory_path(self, input_dir: str) -> Path:
        return Path(input_dir)
    
    def resolve_output_path(self, node: Node, output_name: str, output_type: type) -> Path:
        extension = self.resolve_output_serializer(node, output_name, output_type).get_file_extension()
        directory = self.resolve_output_directory_path(node.output_directory)

        return Path(directory) / (output_name + extension)

    def resolve_input_path(self, node: Node, input_name: str, input_type: type) -> Path:
        extension = self.resolve_input_serializer(node, input_name, input_type).get_file_extension()
        directory = self.resolve_input_directory_path(node.output_directory)

        return Path(directory) / (input_name + extension)
    
    #endregion

    def build_graph(self):
        graph = nx.DiGraph()
        queue = deque(self.namespace.root_nodes)
        visited = set()

        while queue.__len__() > 0:
            node = queue.popleft()

            for next in node.subsequent_nodes:
                graph.add_edge(node, next)
            
            if visited.__contains__(node):
                continue
            
            graph.add_node(node)

            visited.add(node)
        
        return graph

    def get_available_io_inputs(self, input_dir: str) -> list[Path]:
        path = self.resolve_input_directory_path(input_dir)
        stems: list[Path] = []
        try:
            for entry in path.iterdir():
                if entry.is_file():
                    stems.append(entry)
        
        except FileNotFoundError:
            logger.error(f"The directory '{path}' was not found.")
            return []
        except PermissionError:
            logger.error(f"Permission denied for directory '{path}'.")
            return []
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return []

        return stems
    
    #region Validation
    
    def validate_node_parameters(self):
        result = True
        for node in self.graph.nodes:
            if not isinstance(node, Node):
                logging.error(f"Invalid type error, {node} is not an instance of Node")
                result = False
                continue 
            if node.input_aliases is not None:
                flat_aliases = list(itertools.chain.from_iterable(node.input_aliases))

                for alias in flat_aliases:
                    if not isinstance(alias, str):
                        result = False
                        logging.error(f"Alias of incorrect type: {type(alias)} passed to node: {node.name}. Only string are allowed ")

                if len(flat_aliases) != len(set(flat_aliases)):
                    result = False
                    logging.error(f"Duplicate input aliases were passed to node {node.name}")

            if node.outputs is not None:
                node_outputs = [node.outputs] if isinstance(node.outputs, str) else node.outputs
                return_annotations = inspect.signature(node.function).return_annotation

                if(len(return_annotations) != len(node_outputs)):
                    result = False
                    logging.error(f"Invalid output configuration for node: {node.name}, outputs must be both named and annotated.")

                if(len(node_outputs) != len(set(node_outputs))):
                    result = False
                    logging.error(f"Invalid output configuration for node: {node.name}, duplicate output names are present.")
                if node.is_cached:
                    for index in range(len(node_outputs)):
                        if self.get_serializer(node.get_output_serializer, node.outputs[index], return_annotations[index]) is None:
                            logging.error(f"Could not find serializer for output: {node.outputs[index]}, type: {return_annotations[index]} of node: {node.name}")
                            result = False

            if node.output_serializers is not None:
                if isinstance(node.output_serializers, dict):
                    for serializer in node.output_serializers.values():
                        if not isinstance(serializer, DataSerializer):
                            logging.error(f"Invaild output serializer provided to {node.name}: {serializer}")
                            result = False
                elif not isinstance(node.output_serializers, DataSerializer):
                    logging.error(f"Invaild output serializer provided to {node.name}: {node.output_serializers}")
                    result = False
                if isinstance(node.output_serializers, DataSerializer) and isinstance(node.outputs, list) and len(node.outputs) > 1:
                    logging.error(f"Impossible to determine which output of node: {node.name} to associate with serializer: {node.output_serializers}, please pass a dictionary")
                    result = False


        return result
      
    def validate_input_dependencies(self):
        sorted_graph = nx.topological_sort(self.graph)
        available_inputs: dict[uuid.UUID, list[tuple[str, Any]]] = defaultdict()

        result = True
        for node in sorted_graph:
            if not isinstance(node, Node):
                logging.error(f"Invalid type error, {node} is not an instance of Node")
                result = False
                continue

            if node.input_directory_name is not None:
                for input in self.get_available_io_inputs(node.input_directory_name):
                    available_inputs[node.runtime_id].append((input.stem, object))
            
            node_available_inputs = available_inputs.pop(node.runtime_id)

            for name, type in node.get_required_inputs():
                aliases = node.input_aliases[name] if node.input_aliases is not None else [name]

                if isinstance(aliases, str):
                    aliases = [aliases]

                matching_inputs = list(filter(lambda input: any([alias == input[0] for alias in aliases]) and issubclass(input[1], type), node_available_inputs))
                
                if len(matching_inputs) == 0:
                    logging.error(f"No suitable input found for {aliases} in node {node.name}")
                    result = False

                if len(matching_inputs) > 1:
                    logging.error(f"Ambiguous inputs found for {aliases} in node {node.name}")
                    result = False

                if self.get_serializer(node.get_input_serializer,  name, type) is None:
                    logging.error(f"A suitable input for {name} in node: {node.name} of type: {type} was found but no suitable serializer could be determined")
                    result = False

                for subsequent_node in node.subsequent_nodes:
                    outputs = subsequent_node.get_available_outputs()
                    if outputs is not None:
                        available_inputs[subsequent_node.runtime_id].extend(outputs)

        return result

    def validate(self) -> bool:
        if not nx.is_directed_acyclic_graph(self.graph):
            logging.fatal("Pipeline is not a DAG")
            return False
        
        if not self.validate_node_parameters():
            logging.fatal("Invalid configuration of one or more nodes")
            return False
        
        if not self.validate_input_dependencies():
            logging.fatal("Invalid dependencies for one or more nodes")
            return False

        logging.info("Succesfuly validated pipeline")

        return True
    
    #endregion
    
    #region Meta
    
    def load_meta(self):
        nodes = list(self.graph)
        stored = self.meta_handler.data.get_namespace(self.namespace.name)

        if stored is None:
            self.meta_data = self.meta_handler.data.create_namespace(self.namespace, nodes)
            self.meta_handler.sync()
            return
        
        stored.update_from(nodes)
        self.meta_data = stored
        self.meta_handler.sync()


    def update_meta(self):
        self.meta_handler.sync()

    #endregion

    def prepare_node_states(self):
        sorted_graph = nx.topological_sort(self.graph)
        available_input_hashes: dict[uuid.UUID, list[tuple[str, str | None]]] = defaultdict()
        
        for node in sorted_graph:
            if not isinstance(node, Node):
                logging.error(f"Invalid type error, {node} is not an instance of Node")
                return
            
            if not node.is_cached:
                self.node_execution_states[node.runtime_id] = NodeState.READY
                continue

            meta = self.meta_data.get_node_meta(node.get_persistent_hash())

            if meta is None:
                logger.fatal(f"Metadata not found for node {node.name}")
                raise

            # resolve available inputs and generate hashes
            node_inputs = available_input_hashes.pop(node.runtime_id)
            io_inputs = self.get_available_io_inputs(node.input_directory_name) if node.input_directory_name is not None else []

            required_inputs = node.get_required_inputs()

            input_hashes: dict[str, str | None] = dict()

            for input in required_inputs:
                names = node.get_inputs_aliases(input[0])
                available_inputs: list[Path | tuple[str, str | None]] = []

                for name in names:
                    available_inputs += filter(lambda node_input: node_input[0] == name, node_inputs)
                    available_inputs += filter(lambda node_input: node_input.stem == name, io_inputs)
                
                if len(available_inputs) > 1:
                    logger.fatal(f"Ambiguous inputs for node {node.name}, function input {input}, aliases: {names}")
                    raise
                
                if len(available_inputs) == 0:
                    input_hashes[input[0]] = None
                    continue

                available_input = available_inputs[0]
                
                if isinstance(available_input, Path):
                    input_hashes[input[0]] = get_file_hash(available_input)
                else:
                    input_hashes[input[0]] = available_input[1]

            is_hashes_current = meta.is_current(input_hashes)

            if not is_hashes_current:
                self.node_execution_states[node.runtime_id] = NodeState.READY

            if node.is_cached:
                outputs = node.get_available_outputs()
                if outputs is None:
                    continue

                outputs_with_hashes = [(output[0], get_file_hash(self.resolve_output_path(node, output[0], output[1]))) for output in outputs]

                for subsequent_node in node.subsequent_nodes:
                    available_input_hashes[subsequent_node.runtime_id].extend(outputs_with_hashes)

    def prepare_execution(self):
        self.graph = self.build_graph()
        self.validate()
        self.load_meta()
        self.prepare_node_states()
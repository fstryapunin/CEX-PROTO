
from collections import defaultdict, deque
import hashlib
import os
from pathlib import Path
from typing import Any, Literal
import uuid
from venv import logger


from data.serializers import DataSerializer
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

    def get_serializer(self, type: type) -> DataSerializer | None:
        if self.serializers.__contains__(type):
            return self.serializers[type]

        if self.default_serializer is not None:
            return self.default_serializer
        
    def resolve_output_serializer(self, node: Node):
        serializer = node.get_output_serializer()

        if serializer is not None:
            return serializer
        
        output = node.get_available_output()

        if output is None:
            logger.fatal(f"Attempted to resolver output serializer for node: {node.name} that produces no output")
            raise

        serializer = self.get_serializer(output[1])

        if serializer is None:
            logger.fatal(f"No serializer could be found for output: {output[0]} type {output[1]} in node {node.name}")
            raise

        return serializer

    def resolve_input_serializer(self, node: Node, input_name: str, input_type: type):
        serializer = node.get_input_serializer(input_name)
        
        if serializer is not None:
            return serializer
        
        serializer = self.get_serializer(input_type)

        if serializer is None:
            logger.fatal(f"No serializer could be found for input: {input_name} type {input_type} in node {node.name}")
            raise

        return serializer

    #endregion

    # needed later for proper namespacing
    #region Path handling

    def resolve_output_directory_path(self, output_dir: str) -> Path:
        return Path(output_dir)

    def resolve_input_directory_path(self, input_dir: str) -> Path:
        return Path(input_dir)
    
    def resolve_output_path(self, node: Node) -> Path:
        output = node.get_available_output()

        if output is None:
            logger.fatal(f"Attempted to resolve output path for node: {node} that produces no outputs")
            raise

        extension = self.resolve_output_serializer(node).get_file_extension()
        directory = self.resolve_output_directory_path(node.output_directory)

        return Path(directory) / (output[0] + extension)

    def resolve_input_path(self, node: Node, input_name: str, input_type: type) -> Path:
        extension = self.resolve_input_serializer(node, input_name, input_type).get_file_extension()
        directory = self.resolve_input_directory_path(node.output_directory)

        return Path(directory) / (input_name + extension)
    
    #endregion

    #region Initialization

    def initialize_node_states(self):
        self.node_execution_states = dict.fromkeys([node.runtime_id for node in list(self.graph)], NodeState.UNINITIALIZED)

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

    #endregion

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

            output = node.get_available_output()
            if output is not None and node.is_cached and node.get_output_serializer() is None and self.get_serializer(output[1]) is None:
                logging.error(f"Could not find serializer for output: {output[0]}, type: {output[1]} of node: {node.name}")
                result = False

            if node.output_serializer is not None:
                if not isinstance(node.output_serializer, DataSerializer):
                    logging.error(f"Invaild output serializer provided to {node.name}: {node.output_serializer}")
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
                aliases = node.get_inputs_aliases(name)

                matching_inputs = list(filter(lambda input: any([alias == input[0] for alias in aliases]) and (input[1] == object or input[1] == type), node_available_inputs))
                
                if len(matching_inputs) == 0:
                    logging.error(f"No suitable input found for {aliases} in node {node.name}")
                    result = False

                if len(matching_inputs) > 1:
                    logging.error(f"Ambiguous inputs found for {aliases} in node {node.name}")
                    result = False

                try:
                    self.resolve_input_serializer(node, name, type)
                except:
                    logging.error(f"A suitable input for {name} in node: {node.name} of type: {type} was found but no suitable serializer could be determined")
                    result = False

                for subsequent_node in node.subsequent_nodes:
                    outputs = subsequent_node.get_available_output()
                    if outputs is not None:
                        available_inputs[subsequent_node.runtime_id].append(outputs)

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

    #region Preparation

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
                output = node.get_available_output()
                if output is None:
                    continue

                output_hash = get_file_hash(self.resolve_output_path(node))

                for subsequent_node in node.subsequent_nodes:
                    available_input_hashes[subsequent_node.runtime_id].append((output[0], output_hash))


    def prepare_execution(self):
        self.graph = self.build_graph()
        self.initialize_node_states()
        self.validate()
        self.load_meta()
        self.prepare_node_states()

    #endregion

    #region Execution

    def resolve_node_inputs(self, node: Node, available: dict[uuid.UUID, list[tuple[str, Any]]]) -> dict[str, Any] | Literal[False]:
        resolved_inputs = dict[str, Any]()
        available_inputs = available[node.runtime_id]
        available_io_inputs = []
        required_inputs = node.get_required_inputs()

        if node.input_directory_name is not None:
            available_io_inputs = self.get_available_io_inputs(node.input_directory_name)

        for name, type in required_inputs:
            aliases = node.get_inputs_aliases(name)
            inputs = []
            io_inputs: list[Path] = []

            for alias in aliases:
                inputs += list(filter(lambda available: available[0] == alias, available_inputs))
                io_inputs += list(filter(lambda available: available.stem == alias, available_io_inputs))

            if len(inputs) > 1:
                break

            if len(inputs) == 1:
                resolved_inputs[name] = inputs[0]
                continue
            
            if len(io_inputs) == 1:
                io_input = io_inputs[0]
                try:
                    serializer = self.resolve_input_serializer(node, name, type)
                    resolved_inputs[name] = serializer.load(io_input)
                except:
                    break
            else:
                break

        if len(required_inputs) == len(resolved_inputs.keys()):
            return resolved_inputs

        logger.fatal(f"Failed to resolve inputs for node: {node.name}")
        return False
    
    def verify_node_output(self, node: Node, output_value: Any) -> bool:
        expected_output = node.get_available_output()
        if expected_output is None:
            return True

        if expected_output[1] == type(output_value):
            return True
        
        logger.fatal(f"Unexpect output for node: {node.name}")
        return False

    # TODO replace with topological generations and pararell execution
    def execute(self):
        logger.info(f"Started execution of {self.namespace.name}")

        sorted_graph = nx.topological_generations(self.graph)

        available_inputs: dict[uuid.UUID, list[tuple[str, Any]]] = defaultdict()

        for node in sorted_graph:
            if not isinstance(node, Node):
                logging.fatal(f"Invalid type error, {node} is not an instance of Node")
                raise
                        
            if self.node_execution_states[node.runtime_id] == NodeState.SKIPPED:
                continue
            
            self.node_execution_states[node.runtime_id] = NodeState.RUNNING

            inputs = self.resolve_node_inputs(node, available_inputs)

            if inputs == False:
                logger.fatal(f"Could not resolve inputs for node: {node.name} during execution")
                self.node_execution_states[node.runtime_id] = NodeState.ERROR
                continue
            
            try:
                result = node.execute(**inputs)
            except Exception as exception:
                logger.fatal(f"Node {node.name} reported an exception during execution: {exception}")
                continue

            node_output = node.get_available_output()

            if node_output is None:
                continue

            if not self.verify_node_output(node, result):
                self.node_execution_states[node.runtime_id] = NodeState.ERROR
                continue

            for subsequent_node in node.subsequent_nodes:
                available_inputs[subsequent_node.runtime_id].append((node_output[0], result))

            if node.is_cached:
                try:
                    serializer = self.resolve_output_serializer(node)
                    path = self.resolve_output_path(node)

                    if path is None:
                        logger.fatal(f"Faile to  resolve output path ")
                        raise

                    serializer.save(path, result)
                    output_hash = get_file_hash(path)

                    meta = self.meta_data.get_node_meta(node.get_persistent_hash())

                    if meta is not None and output_hash is not None:
                        meta.update_hash(node_output[0], output_hash)
                    else:
                        logger.fatal(f"Failed to update meta for node: {node.name}, ouput: {node_output[0]}")
                        raise
                except:
                    logger.fatal(f"Failed to save node output: {node_output[0]} of node {node.name}")
                    self.node_execution_states[node.runtime_id] = NodeState.ERROR
                    continue

            self.node_execution_states[node.runtime_id] = NodeState.EXECUTED

        logger.info(f"Finished executing {self.namespace.name}")

    #endregion

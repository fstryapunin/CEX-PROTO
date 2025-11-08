
from collections import defaultdict, deque
import hashlib
import inspect
import os
from pathlib import Path
from typing import Any
import uuid
from venv import logger


from pipeline.namespace import Namespace
from pipeline.node import Node
import networkx as nx
import logging

import itertools

def get_file_hash(file_path, chunk_size=8192) -> str | None:
    if not os.path.exists(file_path):
        return None

    hash_func = hashlib.new('sha256')
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hash_func.update(chunk)
    
    return hash_func.hexdigest()

class NamespaceExecutor:
    def __init__(self, namespace: Namespace) -> None:
        self.namespace = namespace
        self.nodes_by_id: dict[uuid.UUID, Node] = dict()
        self.input_paths: dict[uuid.UUID, dict[str, str]] = dict()

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

    def get_available_io_inputs(self, path: Path) -> list[str]:
        stems = []
        try:
            for entry in path.iterdir():
                if entry.is_file():
                    stems.append(entry.stem)
        
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

            if node.input_directory is not None:
                for input in self.get_available_io_inputs(node.input_directory):
                    available_inputs[node.runtime_id].append((input, object))
            
            node_available_inputs = available_inputs.pop(node.runtime_id)

            for name, type in node.get_required_inputs():
                aliases = node.input_aliases[name] if node.input_aliases is not None else [name]
                matching_inputs = list(filter(lambda input: any([alias == input[0] for alias in aliases]) and issubclass(input[1], type), node_available_inputs))
                
                if len(matching_inputs) == 0:
                    logging.error(f"No suitable input found for {aliases} in node {node.name}")
                    result = False

                if len(matching_inputs) > 1:
                    logging.error(f"Ambiguous inputs found for {aliases} in node {node.name}")
                    result = False

                for subsequent_node in node.subsequent_nodes:
                    outputs = subsequent_node.get_available_outputs()
                    if outputs is not None:
                        available_inputs[subsequent_node.runtime_id].extend(outputs)

        return result
    
    def prepare_node_states(self):
        pass

    def load_meta(self):
        pass

    def update_meta(self):
        pass

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

    def prepare_execution(self):
        self.graph = self.build_graph()
        self.validate()
        self.load_meta()
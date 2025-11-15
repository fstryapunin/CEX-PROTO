
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import uuid
from execution.common import DataInformation, ExecutionState, RuntimeException, ValidationException
from execution.validation import NamespaceValidator, ValidationMessages
from log import logger

from data.serializers import DataSerializer
from execution.node import NodeExecutor
from execution.utils import append_multiple, dfs, pop_or_default
from meta.meta import MetadataProvider
import networkx as nx

import execution.cex as cex
import pipeline.namespace
import pipeline.node

type Namespace = pipeline.namespace.Namespace
type Node = pipeline.node.Node

class NamespaceExecutor:
    def __init__(self, parent: cex.CexExecutor, namespace: Namespace, meta_provider: MetadataProvider) -> None:
        NamespaceValidator.validate(namespace)
        self.parent = parent
        self.namespace = namespace
        self.meta_provider = meta_provider
        self.graph = self.build_graph(meta_provider)

    #region Serialization

    def resolve_serializer(self, data: DataInformation) -> DataSerializer | None:
        if data.type in self.namespace.serializers_by_type:
            return self.namespace.serializers_by_type[data.type]
        
        return self.parent.resolve_serializer(data)

    #endregion

    #region Path handling

    def resolve_path(self, path: Path | str) -> Path:
        if self.namespace.path.is_absolute():
            return self.namespace.path / path
        
        return self.parent.resolve_path(self.namespace.path / path)
    
    #endregion

    #region Initialization

    def build_graph(self, meta_provider: MetadataProvider):
        graph: nx.DiGraph[NodeExecutor] = nx.DiGraph()

        def create_executor(node: Node):
            return NodeExecutor(node, self, meta_provider)

        def callback(node: Node, next: list[Node]):
            nonlocal graph
            node_executor = create_executor(node)
            graph.add_node(node_executor)
            for node in next:
                graph.add_edge(node_executor, create_executor(node))

        dfs(self.namespace.root_nodes, pipeline.node.Node.get_subsequent_nodes, callback)

        return graph

    #endregion
    
    #region Validation
      
    def validate_input_dependencies(self):
        sorted_graph = nx.topological_sort(self.graph)
        available_inputs: dict[uuid.UUID, list[DataInformation]] = defaultdict(list)
        messages = []

        for node in sorted_graph:
            node_inputs = pop_or_default(node.runtime_id, available_inputs) + node.get_available_file_inputs()
            for data_information in node.get_required_inputs():
                aliases = node.get_input_aliases(data_information.name)
                matching_inputs = list(filter(lambda input: data_information.match_static(input, aliases), node_inputs))

                if len(matching_inputs) == 0:
                    messages.append(ValidationMessages.CannotSatisfyInput(node.node, self.namespace, data_information))

                elif len(matching_inputs) > 1:
                    messages.append(ValidationMessages.AmbiguosInputs(node.node, self.namespace, data_information))
                try:
                    node.resolve_input_serializer(data_information)
                except RuntimeException as ex:
                    raise ex.to_validaton_exception()
            
            output = node.get_output_information()
            
            if output is None: continue

            append_multiple(available_inputs, node.subsequent_node_ids, output)

        if len(messages) > 0:
            raise ValidationException(messages)

    def validate(self):
        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValidationException([f"Graph of namespace {self.namespace.name} is not a DAG"])
                
        self.validate_input_dependencies()
            
    #endregion
    
    #region Preparation

    def prepare_node_states(self):
        sorted_graph = nx.topological_sort(self.graph)
        available_inputs: dict[uuid.UUID, list[DataInformation]] = defaultdict(list)

        for node in sorted_graph:
            output = node.get_output_information()

            if output is not None and len(node.subsequent_nodes) > 0:
                append_multiple(available_inputs, node.subsequent_node_ids, output if not node.is_cached else output.with_hash())

            if not node.is_cached:
                node.set_state(ExecutionState.READY)
                continue
            
            resolved_inputs = self.resolve_node_inputs(node, pop_or_default(node.runtime_id, available_inputs))

            state = ExecutionState.SKIPPED

            for name, resolved_input in resolved_inputs.items():
                if resolved_input.hash is None or not node.meta.is_current_input(name, resolved_input.hash):
                    state = ExecutionState.READY
                    continue

            node.set_state(state)


    def prepare(self):
        self.prepare_node_states()

    #endregion

    #region Execution

    def resolve_node_inputs(self, node: NodeExecutor, available: list[DataInformation]):
        node_inputs = available + node.get_available_file_inputs()
        required_inputs = node.get_required_inputs()
        resolved_inputs: dict[str, DataInformation] = dict()
        
        for data_information in required_inputs:
            aliases = node.get_input_aliases(data_information.name)
            matching_inputs = list(filter(lambda input: data_information.match_static(input, aliases), node_inputs))
            if len(matching_inputs) != 1:
                node.set_state(ExecutionState.ERROR)
                raise RuntimeException(f"Failed to resolve input for input: {data_information} of node: {node.node}")

            resolved_inputs[data_information.name] = matching_inputs[0]
        
        return resolved_inputs
    
    # TODO replace with topological generations and pararell execution
    def execute(self):
        logger.info(f"Started execution of {self.namespace.name}")

        sorted_graph = nx.topological_sort(self.graph)

        available_inputs: dict[uuid.UUID, list[DataInformation]] = defaultdict(list)

        for node in sorted_graph:
            logger.info(f"Started execution of node {node.name}")

            if node.state == ExecutionState.SKIPPED:
                logger.info(f"Skipping node {node.name}")
                continue

            resolved_inputs = self.resolve_node_inputs(node, pop_or_default(node.runtime_id, available_inputs))
            
            try:
                result = node.execute(resolved_inputs)
            except Exception as ex:
                node.set_state(ExecutionState.ERROR)
                raise ex

            if node.is_cached:
                if result is None:
                    raise RuntimeException(f"Expected ouput from node {node.name} is missing")
                append_multiple(available_inputs, node.subsequent_node_ids, result)
            
            node.set_state(ExecutionState.EXECUTED)

        logger.info(f"Finished executing {self.namespace.name}")

    #endregion

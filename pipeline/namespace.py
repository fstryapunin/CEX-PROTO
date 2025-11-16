from pathlib import Path
from typing import Self
from data.serializers import DataSerializer
from execution.cex import CexExecutor
from pipeline.node import Node


class Namespace:
    def __init__(self, name: str, path: Path | None = None) -> None:
        self.name = name
        self.root_nodes: list[Node] = []
        self.path = path if path is not None else Path(name)
        self.serializers_by_type: dict[type, DataSerializer] = dict()

    def add_serializer_by_type(self, type: type, serializer: DataSerializer):
        self.serializers_by_type[type] = serializer        
        return self

    def add_root_node(self, node: Node):
        self.root_nodes.append(node)
        return self
    
    def run(self):
        executor = CexExecutor()
        executor.execute_pipeline(self)

    @classmethod
    def init_from(cls, namespace: Self, name: str, path: Path | None):
        copy = cls(name, path)
        copy.root_nodes = namespace.root_nodes
        copy.serializers_by_type = namespace.serializers_by_type
        return copy

    def __str__(self) -> str:
        return self.name

from pathlib import Path
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

    def __str__(self) -> str:
        return self.name

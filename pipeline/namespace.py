from pathlib import Path
from pipeline.node import Node


class Namespace:
    def __init__(self, name: str, root_path: Path) -> None:
        self.name = name
        self.root_nodes: list[Node] = []
        self.root_path = root_path

    def add_root_node(self, node: Node):
        self.root_nodes.append(node)

    def __str__(self) -> str:
        return self.name

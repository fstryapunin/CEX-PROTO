from pipeline.namespace import Namespace
from pathlib import Path
from pipeline.node import Node
import json

# TODO this can be majorly optimised if a dictionary based impl is used instead of lists

META_DIR_PATH = Path("cex")
META_FILE_PATH = META_DIR_PATH / "cex.json"

class NodeMeta:
    def __init__(self, hash: str, input_hashes: dict[str, str | None]) -> None:
        self.hash = hash
        self.input_hashes = input_hashes

    @staticmethod
    def get_input_keys(node: Node):
        return node.function.__annotations__.keys()

    def update_from(self, node: Node):
        new_keys = NodeMeta.get_input_keys(node)

        new_hashes = dict.fromkeys(NodeMeta.get_input_keys(node))

        for key in new_keys:
            if self.input_hashes.__contains__(key):
                new_hashes[key] = self.input_hashes[key]

        self.input_hashes = new_hashes                

    @classmethod
    def init_from(cls, node: Node):
        return cls(node.get_persistent_hash(), dict.fromkeys(NodeMeta.get_input_keys(node)))
    
    def set_current_hashes(self, current_hashes: dict[str, str | None]):
        self.input_hashes = current_hashes
    
    def is_current(self, current_hashes: dict[str, str | None]) -> bool:
        for key, value in current_hashes.items():
            if value is None:
                return False
            if not self.input_hashes.__contains__(key):
                return False
            if self.input_hashes[key] is None:
                return False
            if self.input_hashes[key] != value:
                return False
        
        return True

class NamespaceMeta:
    def __init__(self, name: str, nodes: list[NodeMeta]) -> None:
        self.name = name
        self.nodes = nodes
    
    def get_node_meta(self, hash: str) -> NodeMeta | None:
        nodes = [ns for ns in self.nodes if ns.hash == hash]

        if len(nodes) == 0:
            return
        
        return nodes[0] 

    def update_from(self, nodes: list[Node]):
        new_nodes = []

        for node in nodes:
            current_meta = self.get_node_meta(node.get_persistent_hash())
            
            if current_meta is None:
                new_nodes.append(NodeMeta.init_from(node))
                continue

            new_nodes.append(current_meta.update_from(node))

        self.nodes = new_nodes

    @classmethod
    def init_from(cls, name: str, nodes: list[Node]):
        return cls(name, [NodeMeta.init_from(node) for node in nodes])

class CexMeta:
    def __init__(self, namespaces: list[NamespaceMeta]):
        self.namespaces = namespaces

    def create_namespace(self, namespace: Namespace, nodes: list[Node]) -> NamespaceMeta:
        meta = NamespaceMeta.init_from(namespace.name, nodes)
        self.namespaces.append(meta)
        return meta

    # TODO handle duplicate namespaces
    def get_namespace(self, name: str) -> NamespaceMeta | None:
        namespace_metas = [ns for ns in self.namespaces if ns.name == name]

        if len(namespace_metas) == 0:
            return
        return namespace_metas[0]

    @classmethod
    def init_from(cls, namespaces: list[tuple[Namespace, list[Node]]]):
        return cls([NamespaceMeta.init_from(namespace[0].name, namespace[1]) for namespace in namespaces])

class ExecutionMetadataHandler:
    def __init__(self) -> None:
        META_DIR_PATH.mkdir(exist_ok=True)

        if META_FILE_PATH.is_file():
            with open(META_FILE_PATH, 'r', encoding='utf-8') as data:
                self.data = CexMeta([NamespaceMeta(**ns) for ns in json.load(data)])

        else:
            self.data = CexMeta.init_from([])

    # TODO Evaluate perf impact, replace with relevant decorators for ease of use
    def sync(self) -> None:
        with open(META_FILE_PATH, 'w', encoding='utf-8') as file:
            json.dump(self.data, file, indent=4)
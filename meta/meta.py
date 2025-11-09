from pipeline.namespace import Namespace
from pathlib import Path
from pipeline.node import Node
import json

# TODO this can be majorly optimised if a dictionary based impl is used instead of lists

META_DIR_PATH = Path(".cex")
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
    
    def update_hash(self, name: str, value: str):
        self.input_hashes[name] = value

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
    
    def to_serializable(self) -> dict:
        return { "hash": self.hash, "input_hashes": self.input_hashes }
    
    @classmethod
    def from_dict(cls, dict: dict) -> "NodeMeta":
        return cls(dict["hash"], dict["input_hashes"])
    
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

    def to_serializable(self) -> dict:
        return { "name": self.name, "nodes": [node.to_serializable() for node in self.nodes]}

    @classmethod
    def from_dict(cls, dict: dict) -> "NamespaceMeta":
        return cls(dict["name"], [NodeMeta.from_dict(node_dict) for node_dict in dict['nodes']])

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

    def to_serializable(self):
        return [ns.to_serializable() for ns in self.namespaces]

    @classmethod
    def from_list(cls, list: list):
        return cls([NamespaceMeta.from_dict(ns_dict) for ns_dict in list])

    @classmethod
    def init_from(cls, namespaces: list[tuple[Namespace, list[Node]]]):
        return cls([NamespaceMeta.init_from(namespace[0].name, namespace[1]) for namespace in namespaces])

class ExecutionMetadataHandler:
    def __init__(self) -> None:
        META_DIR_PATH.mkdir(exist_ok=True)

        if META_FILE_PATH.is_file():
            with open(META_FILE_PATH, 'r', encoding='utf-8') as data:
                self.data = CexMeta.from_list(json.load(data))
        else:
            self.data = CexMeta.init_from([])

    # TODO Evaluate perf impact, replace with relevant decorators for ease of use
    def sync(self) -> None:
        with open(META_FILE_PATH, 'w', encoding='utf-8') as file:
            json.dump(self.data.to_serializable(), file, indent=4)
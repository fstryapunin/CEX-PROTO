from collections import defaultdict
from typing import Self
from pathlib import Path
import json

import pipeline.node
import pipeline.namespace

type Node = pipeline.node.Node
type Namespace = pipeline.namespace.Namespace

META_DIR_PATH = Path(".cex")
META_FILE_PATH = META_DIR_PATH / "cex.json"
a = 5
class NodeMeta:
    def __init__(self, node_hash: str, input_hashes: dict[str, str] | None, output_hash: str | None) -> None:
        self.node_hash = node_hash
        self.input_hashes = input_hashes if input_hashes is not None else defaultdict()
        self.output_hash = output_hash

    @classmethod
    def init_from(cls, node: Node):
        return cls(node.get_persistent_hash(), None, None)        

    def update_input_hash(self, name: str, value: str):
        self.input_hashes[name] = value

    def update_output_hash(self, value: str):
        self.output_hash = value

    def is_current_input(self, name: str, hash: str):
        if name not in self.input_hashes:
            return None
        
        return self.input_hashes[name] == hash and hash is not None

    def is_current_output(self, value: str | None):
        if value is None:
            return False

        return value == self.output_hash
    
    def to_serializable(self) -> dict:
        return { "node_hash": self.node_hash, "input_hashes": self.input_hashes, "output_hash": self.output_hash }
    
    @classmethod
    def from_dict(cls, dict: dict) -> Self:
        return cls(dict["node_hash"], dict["input_hashes"], dict["output_hash"])
    
class NamespaceMeta:
    def __init__(self, name: str, nodes: defaultdict[str, NodeMeta]) -> None:
        self.name = name
        self.nodes = nodes 
    
    def get_node_meta(self, hash: str) -> NodeMeta | None:
        return self.nodes[hash]

    def update_from(self, nodes: list[Node]):
        new_nodes: defaultdict[str, NodeMeta] = defaultdict()

        for node in nodes:
            node_hash = node.get_persistent_hash()
            current_meta = self.get_node_meta(node_hash)
            
            if current_meta is None:
                new_nodes[node_hash] = NodeMeta.init_from(node)
                continue

            new_nodes[node_hash] = current_meta

        self.nodes = new_nodes

    def to_serializable(self) -> dict:
        return { "name": self.name, "nodes": self.nodes}

    @classmethod
    def from_dict(cls, dict: dict) -> Self:
        nodes = defaultdict()
        
        for key, value in dict["nodes"].items():
            nodes[key] = NodeMeta.from_dict(value)

        return cls(dict["name"], nodes)

    @classmethod
    def init_from(cls, name: str, nodes: list[Node]):
        meta_nodes = defaultdict()

        for node in nodes:
            meta_nodes[node.get_persistent_hash()] = NodeMeta.init_from(node)

        return cls(name, meta_nodes)

class MetaData:
    def __init__(self, namespaces: list[NamespaceMeta]):
        self.namespaces = namespaces

    def set_namespace(self, namespace: Namespace, nodes: list[Node]) -> NamespaceMeta:
        existing = self.get_namespace(namespace.name)
        
        if not existing:
            meta = NamespaceMeta.init_from(namespace.name, nodes)
            self.namespaces.append(meta)
            return meta
        
        existing.update_from(nodes)

        return existing

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

class MetadataProvider:
    def __init__(self) -> None:
        META_DIR_PATH.mkdir(exist_ok=True)

        if META_FILE_PATH.is_file():
            with open(META_FILE_PATH, 'r', encoding='utf-8') as data:
                self.data = MetaData.from_list(json.load(data))
        else:
            self.data = MetaData.init_from([])
            self.sync()

    # TODO Evaluate perf impact, replace with relevant decorators for ease of use
    def sync(self) -> None:
        with open(META_FILE_PATH, 'w', encoding='utf-8') as file:
            json.dump(self.data.to_serializable(), file, indent=4)

meta_provider = MetadataProvider()
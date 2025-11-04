
import hashlib
import os
import uuid
from pipeline.namespace import Namespace
from pipeline.node import Node

def get_file_hash(file_path, chunk_size=8192) -> str | None:
    if not os.path.exists(file_path):
        return None

    hash_func = hashlib.new('sha256')
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hash_func.update(chunk)
    
    return hash_func.hexdigest()

class Executor:
    def __init__(self, namespace: Namespace) -> None:
        self.namespace = namespace
        self.nodes_by_id: dict[uuid.UUID, Node] = dict()
        self.input_paths: dict[uuid.UUID, dict[str, str]] = dict()

    def load_meta(self):
        pass

    def build_graph(self):
        pass

    def validate_input_dependencies(self):
        pass

    def update_meta(self):
        pass

    def validate(self):
        self.meta = self.load_meta()
        self.graph = self.build_graph()
        self.validate_input_dependencies()

    def resolve_input_paths(self, ):

    def prepare(self):
        
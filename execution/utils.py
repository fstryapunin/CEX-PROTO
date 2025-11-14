import hashlib
from pathlib import Path
from typing import Callable, TypeVar

from collections import defaultdict, deque
from uuid import UUID

from pipeline.node import Node


def get_file_hash(file_path: Path, chunk_size=8192) -> str | None:
    if not Path.exists(file_path):
        return None

    hash_func = hashlib.new('sha256')
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hash_func.update(chunk)
    
    return hash_func.hexdigest()

TNode = TypeVar('TNode')

def dfs(root: list[TNode], get_next: Callable[[TNode], list[TNode]], callback: Callable[[TNode, list[TNode]]]):
    queue = deque(root)
    visited = set()

    while len(queue) > 0:
        node = queue.pop()
        next = get_next(node)
        callback(node, next)

        if node in visited: continue
        queue += next                             
        visited.add(node)

TKey = TypeVar('TKey')
TValue = TypeVar('TValue')

def pop_or_default(key: TKey, dict: defaultdict[TKey, TValue]) -> TValue:
    if not key in dict:
        return dict.default_factory() # type: ignore
    
    return dict.pop(key)

def append_multiple(dict: defaultdict[TKey, list[TValue]], keys: list[TKey], value: TValue):
    for key in keys:
        dict[key].append(value)

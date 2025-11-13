import hashlib
from pathlib import Path


def get_file_hash(file_path: Path, chunk_size=8192) -> str | None:
    if not Path.exists(file_path):
        return None

    hash_func = hashlib.new('sha256')
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            hash_func.update(chunk)
    
    return hash_func.hexdigest()
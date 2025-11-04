from typing import Generic, TypeVar


TData = TypeVar('TData')

class DataSerializer(Generic[TData]):
    def load(self) -> TData:
        raise
    def save(self, data: TData):
        pass

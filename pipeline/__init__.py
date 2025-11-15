
from pathlib import Path
from typing import Self
from data.serializers import PickleSerializer, CsvSerializer, DataSerializer, JsonSerializer, YamlSerializer, PlainFileSerializer


class Cex:
    def __init__(self) -> None:
        self.root_path = Path.cwd()
        self.serilizers_by_type: dict[type, DataSerializer] = dict()
        self.default_serializers: list[DataSerializer] = [
            JsonSerializer(), 
            YamlSerializer(), 
            CsvSerializer(), 
            PickleSerializer(), 
            PlainFileSerializer()
        ]

    def set_root_path(self, path: Path) -> Self:
        self.root_path = path
        return self

    def add_serializer(self, serializer: DataSerializer) -> Self:
        self.default_serializers.append(serializer)
        return self

    def add_serializer_for_type(self, type: type, serizalier: DataSerializer) -> Self:
        self.serilizers_by_type[type] = serizalier
        return self

cex = Cex()
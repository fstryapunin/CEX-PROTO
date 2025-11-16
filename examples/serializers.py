# To save and load node data, CEX will attempt to resolve and appropriate serializer by the given type of data.
# Serializers can be specified at different levels.
# Serializer for a type can be specified at either cex, namespace, or node level.
# Node level allows for direct association with and input or output. Useful for loading data from disk for example.
# If a serializer can not be selected at node level, an attempt will be made to select serializer at namespace and then at cex level.
# A set of default serializers is provided at cex level to load file inputs by extension, if no serializer was given.
# Provided serializers must implement DataSerializer protocol

from pathlib import Path
from data.serializers import JsonSerializer
from pipeline.namespace import Namespace
from pipeline.node import Node

def print_data(data: dict[str, int]) -> dict[str, int]:
        print("Loaded input: ")
        print(data)
        return data

if __name__ == "__main__":

    # Use default input serializer for json
    default_namespace = Namespace("DefaultJsonNamespace", Path("./examples/data"))



    json_node = Node(print_data, name="DataPrinter", input_directory="input", is_cached=False)

    default_namespace.add_root_node(json_node)
    #default_namespace.run()

    # Specify serializer for input

    specified_namespace = Namespace("DefaultJsonNamespace", Path("./examples/data"))

    specified_node = Node(print_data, name="DataPrinter", input_directory="input", is_cached=False, input_serializers=JsonSerializer())

    specified_namespace.add_root_node(json_node)
    
    # specified_namespace.run()

    # Provide a serializer for type. Same is also possible at CEX level.

    class MyData:
        def  __init__(self, name) -> None:
            self.name = name

    class MySerializer:
        DATA: MyData
        def get_file_extension(self) -> str:
            return ".pkl"
        
        def matches_file(self, extension: str):
            return extension == ".pkl" or extension == ".pickle"
        
        def load(self, path: Path):
            return MySerializer.DATA
        
        def save(self, path: Path, data):
            MySerializer.DATA = data

    def create_my_data() -> MyData:
        return MyData("My name")

    def print_my_data(data: MyData):
        print(data.name)

    my_data_namespace = Namespace("MyDataNamespace")
    my_data_namespace.add_serializer_by_type(MyData, MySerializer())
    
    create_my_data_node = Node(create_my_data, name="MyDataCreator", output_name="MyData",  is_cached=False)
    print_my_data_node = Node(print_my_data, name="MyDataPrinter", is_cached=False)

    create_my_data_node.continue_with(print_my_data_node)

    my_data_namespace.add_root_node(create_my_data_node)
    # my_data_namespace.run()
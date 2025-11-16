from pathlib import Path
from data.serializers import JsonSerializer
from pipeline import cex
from pipeline.namespace import Namespace
from pipeline.node import Node

if __name__ == "__main__":
    # Set root folder for CEX. By default is is CWD.

    # Simple cached pipeline. Notice that on the second run execution will be skipped and no data is printed.

    def print_data(data: dict[str, int]):
        print("Loaded input: ")
        print(data)
        return data

    cached_namespace = Namespace("CachedNamespace", Path("./examples/data"))

    printer_node = Node(print_data, name="One", input_directory="input")

    cached_namespace.add_root_node(printer_node)
    
    # cached_namespace.run()
    # cached_namespace.run()

    # Lets imagine we want to run the same pipeline, but with different inputs.

    # Clone the namespace:

    another_namespace = Namespace.init_from(cached_namespace, "AnotherNamespace", Path("./examples/other_data"))
    another_namespace.run()

    # We reused the pipeline with different output


from pathlib import Path
import random
from data.serializers import JsonSerializer
from pipeline.namespace import Namespace
from pipeline.node import Node
from pipeline import cex

# It is possible not to cache a node, which is sometimes desirable, for example if the output is not deterministic.

if __name__ == "__main__":
    # Set root folder for CEX. By default it is CWD.
    cex.set_root_path(Path("examples"))

    namespace = Namespace("RandomNamespace").add_serializer_by_type(int, JsonSerializer())

    def random_function() -> int:
        rand = random.randint(0, 1)
        print("Random value generated", rand)
        return random.randint(0, 1)
    
    def double_int(int: int) -> int:
        return int * 2
    
    def print_int(int: int):
        print(int)

    # Uncached node
    random_node = Node(random_function, name="Random", is_cached=False, output_name="random")
    # This node has an uncached input so will be ran everytime. This is a known issue and could be adressed in the future by hashing value inputs as a possible optimization.
    double_node = Node(double_int, name="Double", output_name="double")
    # All inputs of this node are cached so notice it wont run on the second time unless a different random value was generated in the random node.
    print_node = Node(print_int, name="Print")

    random_node.continue_with(double_node)
    double_node.continue_with(print_node)
    namespace.add_root_node(random_node)
    namespace.run()    
    
    

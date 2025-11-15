from data.serializers import JsonSerializer
from pipeline.namespace import Namespace
from pipeline.node import Node

# Sequential pipeline

namespace = Namespace("SequentialNamespace").add_serializer_by_type(int, JsonSerializer())

def one() -> int:
    return 1

def double(y: int) -> int:
    return y * 2

def square(z: int) -> int:
    return z ** 2

N1_sequential = Node(one, name="AddOne", is_cached=True, output_name="result")
N2_sequential = Node(double, name="Double", input_aliases={'y': 'result'}, is_cached=True, output_name="result")
N3_sequential = Node(square, name="Square", input_aliases={'z': 'result'}, is_cached=True, output_name="result")

N1_sequential.continue_with(N2_sequential)
N2_sequential.continue_with(N3_sequential)

namespace.add_root_node(N1_sequential)
namespace.run()

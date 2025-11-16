from pathlib import Path
from data.serializers import JsonSerializer, PlainFileSerializer
from pipeline.namespace import Namespace
from pipeline.node import Node
from pipeline import cex

if __name__ == "__main__":
    cex.set_root_path(Path("./examples/data"))

    # Sequential pipeline

    seq_namespace = Namespace("SequentialNamespace").add_serializer_by_type(int, JsonSerializer())

    def one() -> int:
        return 1

    def double(y: int) -> int:
        return y * 2

    def square(z: int) -> int:
        return z ** 2

    N1_sequential = Node(one, name="One", output_name="result")
    N2_sequential = Node(double, name="Double", input_aliases={'y': 'result'}, output_name="result")
    N3_sequential = Node(square, name="Square", input_aliases={'z': 'result'}, output_name="result")

    N1_sequential.continue_with(N2_sequential)
    N2_sequential.continue_with(N3_sequential)

    seq_namespace.add_root_node(N1_sequential)

    # Branching pipeline
    json_serializer = JsonSerializer()
    
    branch_namespace = Namespace("BranchingNamespace")
    branch_namespace.add_serializer_by_type(list[int], json_serializer)
    branch_namespace.add_serializer_by_type(int, json_serializer)

    def get_initial_data() -> list[int]:
        data = list(range(1, 10))
        return data

    def calculate_sum(a: list[int]) -> int:
        total = sum(a)
        return total

    def calculate_product(data: list[int]) -> int:
        product = 1
        for x in data:
            product *= x
        return product
    
    N1_branching = Node(get_initial_data, name="Initial", output_name="data")
    N2_branching = Node(calculate_sum, name="Sum", output_name="total")
    N3_branching = Node(calculate_product, name="Product", input_aliases={'data': 'total'}, output_name="product")    
    
    N1_branching.continue_with(N2_branching)
    N1_branching.continue_with(N3_branching)

    branch_namespace.add_root_node(N1_branching)
    branch_namespace.run()

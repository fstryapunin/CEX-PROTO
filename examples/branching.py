from pathlib import Path
from data.serializers import JsonSerializer
from pipeline.namespace import Namespace
from pipeline.node import Node
from pipeline import cex

# Different branching patterns are possible.
# Inputs are resolved by best match based on argument name and type. In case or ambiguity, CEX will raise an exception.
# Ouputs that are saved to disk must have a provided name
# Input aliases can be used to associate inputs that are saved with a different name.

if __name__ == "__main__":
    # Set root folder for CEX. By default is is CWD.
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
    # seq_namespace.run()

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
    # branch_namespace.run()

    # Join pipeline
    
    join_namespace = Namespace("JoinNamespace")
    join_namespace.add_serializer_by_type(dict, json_serializer)
    join_namespace.add_serializer_by_type(str, json_serializer)

    def fetch_user_data() -> dict:
        return {"id": 101, "name": "Alice"}

    def fetch_transaction_data() -> dict:
        return {"count": 5, "total": 1500.00}

    def generate_report(details: dict, transactions: dict) -> str:
        user = details.get("name")
        total = transactions.get("total")
        return f"Report: User {user} made {total} in transactions."
    
    N1_join = Node(fetch_user_data, name="UserData", output_name="user")
    N2_join = Node(fetch_transaction_data, name="TransactionData", output_name="transactions")
    N3_join = Node(
        generate_report, 
        name="Report", 
        input_aliases={ 
            'details': 'user', 
            'transactions': 'transactions' 
            }, 
        output_name="report")

    N1_join.continue_with(N3_join)
    N2_join.continue_with(N3_join)

    join_namespace.add_root_node(N1_join).add_root_node(N2_join)

    # join_namespace.run()

    # Diamond pipeline

    diamond_namespace = Namespace("DiamondNamespace")
    diamond_namespace.add_serializer_by_type(dict, json_serializer)
    diamond_namespace.add_serializer_by_type(str, json_serializer)

    def initial_data_load() -> dict:
        return {"source": "Data source", "value": 100}

    def transform_data_A(data: dict) -> dict:
        transformed_value = data["value"] * 2
        return {"data_id": data["source"] + "-A", "transformed_val": transformed_value}

    def transform_data_B(data: dict) -> dict:
        transformed_value = data["value"] / 2
        return {"data_id": data["source"] + "-B", "transformed_val": transformed_value}

    def combine_results(result_A: dict, result_B: dict) -> dict:
        final_combined_value = result_A["transformed_val"] + result_B["transformed_val"]
        return {
            "final_id": f"{result_A['data_id']}-{result_B['data_id']}",
            "combined_sum": final_combined_value
        }

    N1_diamond = Node(initial_data_load, name="N1_Load", output_name="raw_data")
    N2_diamond = Node(transform_data_A, name="N2_TransformA", output_name="transformed_A")
    N3_diamond = Node(transform_data_B, name="N3_TransformB", output_name="transformed_B")

    N4_diamond = Node(
        combine_results, 
        name="N4_Combine", 
        output_name="final_combined_output",
        input_aliases={
            "result_A": "transformed_A", 
            "result_B": "transformed_B"
        }
    )

    N1_diamond.continue_with(N2_diamond)
    N1_diamond.continue_with(N3_diamond)
    N2_diamond.continue_with(N4_diamond)
    N3_diamond.continue_with(N4_diamond)

    diamond_namespace.add_root_node(N1_diamond)
    # diamond_namespace.run()


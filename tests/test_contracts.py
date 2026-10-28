import os
import pytest
from pydantic import ValidationError
from dataflow_guard.contracts import DataContract, ColumnContract


def test_valid_data_contract_parsing():
    fixture_path = os.path.join(os.path.dirname(__file__), "..", "fixtures", "orders_contract.json")
    contract = DataContract.from_json(fixture_path)
    assert contract.name == "ecommerce_orders"
    assert contract.primary_key == ["order_id"]
    assert len(contract.columns) == 7

    col = contract.get_column("order_amount")
    assert col is not None
    assert col.data_type == "float"
    assert col.min_value == 0.01
    assert col.max_value == 50000.0


def test_unsupported_data_type_raises():
    with pytest.raises(ValidationError):
        ColumnContract(name="test", data_type="invalid_type")


def test_empty_primary_key_raises():
    with pytest.raises(ValidationError):
        DataContract(
            name="test_table",
            primary_key=[],
            columns=[ColumnContract(name="id", data_type="int")]
        )

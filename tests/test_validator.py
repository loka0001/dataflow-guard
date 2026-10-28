import os
import pandas as pd
from dataflow_guard.contracts import DataContract
from dataflow_guard.validator import DataFrameValidator


def test_validator_clean_batch():
    fixture_dir = os.path.join(os.path.dirname(__file__), "..", "fixtures")
    contract = DataContract.from_json(os.path.join(fixture_dir, "orders_contract.json"))
    df_clean_source = pd.read_csv(os.path.join(fixture_dir, "orders_batch_1.csv"))

    validator = DataFrameValidator(contract)
    clean_df, quarantine_df, summary = validator.validate(df_clean_source)

    assert len(clean_df) == 6
    assert len(quarantine_df) == 0
    assert summary.is_valid is True
    assert summary.quarantine_rate == 0.0
    assert len(summary.column_violations) == 0


def test_validator_missing_column_batch_failure():
    contract = DataContract.from_json(os.path.join(os.path.dirname(__file__), "..", "fixtures", "orders_contract.json"))
    df = pd.DataFrame({"random_col": [1, 2, 3]})

    validator = DataFrameValidator(contract)
    clean_df, quarantine_df, summary = validator.validate(df)

    assert len(clean_df) == 0
    assert len(quarantine_df) == 3
    assert summary.is_valid is False
    assert len(summary.schema_errors) > 0


def test_validator_messy_batch_identifies_exact_violations():
    fixture_dir = os.path.join(os.path.dirname(__file__), "..", "fixtures")
    contract = DataContract.from_json(os.path.join(fixture_dir, "orders_contract.json"))
    df_messy = pd.read_csv(os.path.join(fixture_dir, "orders_batch_2_messy.csv"))

    validator = DataFrameValidator(contract)
    clean_df, quarantine_df, summary = validator.validate(df_messy)

    # In batch 2 (6 total rows):
    # Row 0: ORD-10001 (Valid update)
    # Row 1: ORD-10007 (Valid)
    # Row 2: ORD-10008 (Missing customer_id -> Fail)
    # Row 3: ORD-10009 (Amount -45.00 < 0.01 -> Fail)
    # Row 4: ORD-10010 (Status FRAUD_ALERT not in enum -> Fail)
    # Row 5: BAD_ORDER_FORMAT (Regex failure -> Fail)
    assert len(clean_df) == 2
    assert len(quarantine_df) == 4
    assert summary.total_rows == 6
    assert summary.quarantined_rows == 4
    assert "customer_id" in summary.column_violations
    assert "order_amount" in summary.column_violations
    assert "status" in summary.column_violations
    assert "order_id" in summary.column_violations

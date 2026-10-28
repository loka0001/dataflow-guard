import os
import tempfile
import sqlite3
from dataflow_guard.contracts import DataContract
from dataflow_guard.pipeline import GuardedPipeline


def test_guarded_pipeline_idempotency_and_quarantine():
    fixtures_dir = os.path.join(os.path.dirname(__file__), "..", "fixtures")
    contract_path = os.path.join(fixtures_dir, "orders_contract.json")
    batch1_path = os.path.join(fixtures_dir, "orders_batch_1.csv")
    batch2_path = os.path.join(fixtures_dir, "orders_batch_2_messy.csv")

    contract = DataContract.from_json(contract_path)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "pipeline_test.db")
        pipeline = GuardedPipeline(contract=contract, db_path=db_path)

        # Ingest Batch 1
        clean_1, q_1, summary_1 = pipeline.ingest_batch(batch1_path)
        assert len(clean_1) == 6
        assert len(q_1) == 0
        assert pipeline.count_clean_records() == 6
        assert pipeline.count_quarantine_records() == 0

        # Ingest Batch 2 (Contains 2 valid rows: 1 new order ORD-10007, and 1 update to ORD-10001)
        clean_2, q_2, summary_2 = pipeline.ingest_batch(batch2_path)
        assert len(clean_2) == 2
        assert len(q_2) == 4
        # Clean records total should be 7 (6 from batch 1 + 1 new from batch 2)
        assert pipeline.count_clean_records() == 7
        assert pipeline.count_quarantine_records() == 4

        # Verify idempotency & update on ORD-10001
        with pipeline._connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT status FROM "ecommerce_orders" WHERE order_id = "ORD-10001"')
            row = cursor.fetchone()
            assert row[0] == "REFUNDED"

            # Check audit log entries
            cursor.execute("SELECT COUNT(*) FROM dataflow_audit_log")
            assert cursor.fetchone()[0] == 2

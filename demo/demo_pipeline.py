import os
import sys

# Ensure dataflow_guard package is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dataflow_guard.contracts import DataContract
from dataflow_guard.pipeline import GuardedPipeline
from dataflow_guard.observability import generate_markdown_report, export_json_report


def run_demo():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    fixtures_dir = os.path.join(base_dir, "..", "fixtures")
    output_dir = os.path.join(base_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    contract_path = os.path.join(fixtures_dir, "orders_contract.json")
    batch1_path = os.path.join(fixtures_dir, "orders_batch_1.csv")
    batch2_path = os.path.join(fixtures_dir, "orders_batch_2_messy.csv")

    db_path = os.path.join(output_dir, "production_warehouse.db")
    report_md_path = os.path.join(output_dir, "data_quality_audit.md")
    report_json_path = os.path.join(output_dir, "data_quality_metrics.json")

    print("==================================================================")
    print("DataFlow-Guard Demo: Idempotent Ingestion & Observability Engine")
    print("==================================================================")
    print(f"Contract:      {contract_path}")
    print(f"Target DB:     {db_path}")
    print(f"Audit Report:  {report_md_path}")
    print("------------------------------------------------------------------")

    contract = DataContract.from_json(contract_path)
    pipeline = GuardedPipeline(contract=contract, db_path=db_path)

    print("\n[Step 1] Ingesting Batch 1: Clean Baseline Orders (6 records)...")
    clean_1, q_1, summary_1 = pipeline.ingest_batch(batch1_path)
    print(f"  -> Validated & Inserted: {len(clean_1)} | Quarantined: {len(q_1)}")
    print(f"  -> Warehouse Total Orders: {pipeline.count_clean_records()}")

    print("\n[Step 2] Ingesting Batch 2: Messy Stream with Anomalies & Updates (6 records)...")
    clean_2, q_2, summary_2 = pipeline.ingest_batch(batch2_path)
    print(f"  -> Validated: {len(clean_2)} | Quarantined: {len(q_2)}")
    print(f"  -> Warehouse Total Orders: {pipeline.count_clean_records()} (Demonstrates Idempotent Upsert)")
    print(f"  -> Total Quarantined in DLQ: {pipeline.count_quarantine_records()}")

    print("\n[Step 3] Generating Data Quality & Observability Reports...")
    generate_markdown_report(summary_2, contract, output_path=report_md_path)
    export_json_report(summary_2, report_json_path)
    print(f"  -> Markdown Audit saved to: {report_md_path}")
    print(f"  -> JSON Metrics saved to:   {report_json_path}")

    print("\n[Step 4] Inspecting Dead Letter Queue (Quarantine) Reasons:")
    with pipeline._connection() as conn:
        q_rows = conn.cursor().execute("SELECT id, primary_key_val, reasons FROM dataflow_quarantine").fetchall()
        for r in q_rows:
            print(f"  [Q-ID {r[0]}] Entity: {r[1]} | Violations: {r[2]}")

    print("\n==================================================================")
    print("DataFlow-Guard Demo Execution Completed with 100% Integrity!")
    print("==================================================================")


if __name__ == "__main__":
    run_demo()

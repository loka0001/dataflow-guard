import os
import argparse
from dataflow_guard.contracts import DataContract
from dataflow_guard.pipeline import GuardedPipeline
from dataflow_guard.observability import generate_markdown_report, export_json_report


def main():
    parser = argparse.ArgumentParser(
        description="DataFlow-Guard: Idempotent ETL Pipeline with Data Quality Contracts & Observability"
    )
    parser.add_argument(
        "--contract",
        required=True,
        help="Path to Data Contract JSON definition"
    )
    parser.add_argument(
        "--data",
        required=True,
        help="Path to input dataset (CSV or JSON)"
    )
    parser.add_argument(
        "--db",
        default="output/dataflow.db",
        help="Path to destination SQLite database (default: output/dataflow.db)"
    )
    parser.add_argument(
        "--report",
        default="output/quality_report.md",
        help="Path to output Markdown Quality Report (default: output/quality_report.md)"
    )
    parser.add_argument(
        "--json-report",
        default=None,
        help="Optional path to output structured JSON metrics"
    )

    args = parser.parse_args()

    print("==================================================================")
    print("DataFlow-Guard: Enterprise Data Quality & Idempotent Ingestion")
    print("==================================================================")
    print(f"Contract Schema: {args.contract}")
    print(f"Source Data:     {args.data}")
    print(f"Target DB:       {args.db}")
    print(f"Quality Report:  {args.report}")
    print("------------------------------------------------------------------")

    contract = DataContract.from_json(args.contract)
    pipeline = GuardedPipeline(contract=contract, db_path=args.db)

    clean_df, quarantine_df, summary = pipeline.ingest_batch(args.data)

    generate_markdown_report(summary, contract, output_path=args.report)
    if args.json_report:
        export_json_report(summary, args.json_report)

    status_str = "SUCCESS [PASSED]" if summary.is_valid else "WARNING [QUARANTINE THRESHOLD EXCEEDED]"
    print(f"\nPipeline Ingestion Completed: {status_str}")
    print(f"  * Total Rows Processed:   {summary.total_rows}")
    print(f"  * Clean Rows Upserted:    {summary.valid_rows} (Total DB Records: {pipeline.count_clean_records()})")
    print(f"  * Quarantined Rows:       {summary.quarantined_rows} (Total DB Quarantine: {pipeline.count_quarantine_records()})")
    print(f"  * Quarantine Ratio:       {summary.quarantine_rate * 100:.2f}%")
    print(f"  * Quality Report Saved:   {args.report}")
    print("==================================================================")


if __name__ == "__main__":
    main()

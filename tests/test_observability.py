import os
import tempfile
import json
from dataflow_guard.contracts import DataContract
from dataflow_guard.validator import ValidationSummary
from dataflow_guard.observability import generate_markdown_report, export_json_report


def test_observability_reports_generation():
    fixtures_dir = os.path.join(os.path.dirname(__file__), "..", "fixtures")
    contract = DataContract.from_json(os.path.join(fixtures_dir, "orders_contract.json"))

    summary = ValidationSummary(
        is_valid=True,
        total_rows=100,
        valid_rows=95,
        quarantined_rows=5,
        quarantine_rate=0.05,
        column_violations={"order_amount": 5}
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        md_path = os.path.join(tmpdir, "report.md")
        json_path = os.path.join(tmpdir, "report.json")

        md_content = generate_markdown_report(summary, contract, output_path=md_path)
        assert os.path.exists(md_path)
        assert "DataFlow-Guard: Data Quality & Observability Audit" in md_content
        assert "PASSED" in md_content
        assert "order_amount" in md_content

        export_json_report(summary, json_path)
        assert os.path.exists(json_path)
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            assert data["total_rows"] == 100
            assert data["valid_rows"] == 95

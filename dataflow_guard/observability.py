import os
import json
from datetime import datetime, timezone
from typing import Optional
from dataflow_guard.contracts import DataContract
from dataflow_guard.validator import ValidationSummary


def generate_markdown_report(
    summary: ValidationSummary,
    contract: DataContract,
    output_path: Optional[str] = None
) -> str:
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    badge = "🟢 PASSED" if summary.is_valid else "🔴 QUARANTINE THRESHOLD EXCEEDED"
    pass_pct = (summary.valid_rows / summary.total_rows * 100) if summary.total_rows > 0 else 100.0

    lines = [
        f"# DataFlow-Guard: Data Quality & Observability Audit",
        f"",
        f"**Target Dataset:** `{contract.name}`  ",
        f"**Contract Version:** `{contract.version}`  ",
        f"**Audit Timestamp:** `{now_iso}`  ",
        f"**Health Status:** `{badge}`  ",
        f"",
        f"---",
        f"",
        f"## 1. Executive Summary",
        f"",
        f"| Metric | Value |",
        f"|---|---|",
        f"| **Total Ingested Rows** | `{summary.total_rows}` |",
        f"| **Accepted Valid Rows** | `{summary.valid_rows}` ({pass_pct:.1f}%) |",
        f"| **Quarantined Rows** | `{summary.quarantined_rows}` ({summary.quarantine_rate * 100:.1f}%) |",
        f"| **Allowed Max Quarantine Rate** | `{contract.max_quarantine_rate * 100:.1f}%` |",
        f"| **Pipeline Evaluation** | **{badge}** |",
        f"",
        f"---",
        f"",
        f"## 2. Column-Level Violation Matrix",
        f"",
    ]

    if summary.column_violations:
        lines.append("| Column / Assertion | Violations Count | Impact Ratio |")
        lines.append("|---|---|---|")
        for col_name, count in summary.column_violations.items():
            ratio = (count / summary.total_rows * 100) if summary.total_rows > 0 else 0
            lines.append(f"| `{col_name}` | `{count}` | `{ratio:.1f}%` |")
    else:
        lines.append("✅ **Zero column-level rule violations detected.** All values strictly conform to contract bounds.")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 3. Data Contract Rules Enforced")
    lines.append("")
    lines.append("| Column | Type | Nullable | Bounds / Enums / Regex |")
    lines.append("|---|---|---|---|")
    for col in contract.columns:
        bounds = []
        if col.min_value is not None or col.max_value is not None:
            bounds.append(f"[{col.min_value} .. {col.max_value}]")
        if col.allowed_values:
            bounds.append(f"in {col.allowed_values}")
        if col.regex_pattern:
            bounds.append(f"regex `{col.regex_pattern}`")
        bounds_str = "; ".join(bounds) if bounds else "None"
        null_str = "Yes" if col.nullable else "**No**"
        lines.append(f"| `{col.name}` | `{col.data_type}` | {null_str} | {bounds_str} |")

    report_content = "\n".join(lines) + "\n"

    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report_content)

    return report_content


def export_json_report(summary: ValidationSummary, output_path: str):
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary.model_dump(), f, indent=2)

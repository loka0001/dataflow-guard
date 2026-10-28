import os
import time
import json
import sqlite3
from contextlib import contextmanager
from typing import Optional, Union, Generator, Tuple
from datetime import datetime, timezone
import pandas as pd

from dataflow_guard.contracts import DataContract
from dataflow_guard.validator import DataFrameValidator, ValidationSummary


class GuardedPipeline:
    def __init__(self, contract: DataContract, db_path: str):
        self.contract = contract
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.validator = DataFrameValidator(contract)
        self._init_database()

    @contextmanager
    def _connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _init_database(self):
        """Initializes destination target table and quarantine/audit infrastructure."""
        # Map contract types to SQLite column types
        type_map = {
            "int": "INTEGER",
            "float": "REAL",
            "str": "TEXT",
            "datetime": "TEXT",
            "bool": "INTEGER"
        }

        col_defs = []
        for col in self.contract.columns:
            sql_type = type_map.get(col.data_type, "TEXT")
            null_clause = "NOT NULL" if not col.nullable else ""
            col_defs.append(f'"{col.name}" {sql_type} {null_clause}'.strip())

        pk_clause = f"PRIMARY KEY ({', '.join(f'\"{pk}\"' for pk in self.contract.primary_key)})"
        col_defs.append(pk_clause)

        target_sql = f"""
        CREATE TABLE IF NOT EXISTS "{self.contract.name}" (
            {', '.join(col_defs)}
        );
        """

        quarantine_sql = f"""
        CREATE TABLE IF NOT EXISTS dataflow_quarantine (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            primary_key_val TEXT,
            reasons TEXT NOT NULL,
            raw_payload TEXT NOT NULL,
            quarantined_at TEXT NOT NULL
        );
        """

        audit_sql = """
        CREATE TABLE IF NOT EXISTS dataflow_audit_log (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            total_rows INTEGER,
            valid_rows INTEGER,
            quarantined_rows INTEGER,
            quarantine_rate REAL,
            duration_seconds REAL,
            status TEXT NOT NULL,
            completed_at TEXT NOT NULL
        );
        """

        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(target_sql)
            cursor.execute(quarantine_sql)
            cursor.execute(audit_sql)
            conn.commit()

    def ingest_batch(
        self,
        data_source: Union[str, pd.DataFrame]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, ValidationSummary]:
        start_time = time.time()

        if isinstance(data_source, str):
            if data_source.endswith(".csv"):
                df = pd.read_csv(data_source)
            elif data_source.endswith(".json"):
                df = pd.read_json(data_source)
            else:
                raise ValueError(f"Unsupported file format: {data_source}")
        elif isinstance(data_source, pd.DataFrame):
            df = data_source.copy()
        else:
            raise TypeError(f"data_source must be str path or DataFrame, got {type(data_source)}")

        # Run Validation
        clean_df, quarantine_df, summary = self.validator.validate(df)

        with self._connection() as conn:
            cursor = conn.cursor()

            # 1. Idempotent Upsert for Clean Rows
            if len(clean_df) > 0:
                cols = [c.name for c in self.contract.columns]
                pk_set = set(self.contract.primary_key)
                update_cols = [c for c in cols if c not in pk_set]

                placeholders = ", ".join(["?"] * len(cols))
                col_names_quoted = ", ".join([f'"{c}"' for c in cols])
                
                if update_cols:
                    update_clause = ", ".join([f'"{c}" = excluded."{c}"' for c in update_cols])
                    upsert_sql = f"""
                    INSERT INTO "{self.contract.name}" ({col_names_quoted})
                    VALUES ({placeholders})
                    ON CONFLICT({', '.join(f'"{pk}"' for pk in self.contract.primary_key)})
                    DO UPDATE SET {update_clause};
                    """
                else:
                    upsert_sql = f"""
                    INSERT OR IGNORE INTO "{self.contract.name}" ({col_names_quoted})
                    VALUES ({placeholders});
                    """

                rows_to_insert = [
                    tuple(None if pd.isna(row[c]) else row[c] for c in cols)
                    for _, row in clean_df.iterrows()
                ]
                cursor.executemany(upsert_sql, rows_to_insert)

            # 2. Insert Quarantine Rows
            if len(quarantine_df) > 0:
                q_sql = """
                INSERT INTO dataflow_quarantine (
                    table_name, primary_key_val, reasons, raw_payload, quarantined_at
                ) VALUES (?, ?, ?, ?, ?);
                """
                now_str = datetime.now(timezone.utc).isoformat()
                q_rows = []
                for _, qrow in quarantine_df.iterrows():
                    reasons = qrow.get("_quarantine_reasons", "Unknown validation failure")
                    # Construct pk representation
                    pk_val = "-".join([str(qrow.get(pk, "null")) for pk in self.contract.primary_key])
                    raw_dict = qrow.drop(labels=["_quarantine_reasons"], errors="ignore").to_dict()
                    q_rows.append((
                        self.contract.name,
                        pk_val,
                        str(reasons),
                        json.dumps(raw_dict, default=str),
                        now_str
                    ))
                cursor.executemany(q_sql, q_rows)

            # 3. Insert Audit Run Log
            duration = time.time() - start_time
            status = "PASSED" if summary.is_valid else "ALERT_QUARANTINE_EXCEEDED"
            audit_sql = """
            INSERT INTO dataflow_audit_log (
                table_name, total_rows, valid_rows, quarantined_rows,
                quarantine_rate, duration_seconds, status, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'));
            """
            cursor.execute(audit_sql, (
                self.contract.name,
                summary.total_rows,
                summary.valid_rows,
                summary.quarantined_rows,
                summary.quarantine_rate,
                round(duration, 3),
                status
            ))
            conn.commit()

        return clean_df, quarantine_df, summary

    def count_clean_records(self) -> int:
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM "{self.contract.name}"')
            return cursor.fetchone()[0]

    def count_quarantine_records(self) -> int:
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM dataflow_quarantine WHERE table_name = ?', (self.contract.name,))
            return cursor.fetchone()[0]

import re
from typing import Tuple, Dict, List, Any
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from dataflow_guard.contracts import DataContract, ColumnContract


class ValidationSummary(BaseModel):
    is_valid: bool
    total_rows: int
    valid_rows: int
    quarantined_rows: int
    quarantine_rate: float
    column_violations: Dict[str, int] = Field(default_factory=dict)
    schema_errors: List[str] = Field(default_factory=list)


class DataFrameValidator:
    """
    High-performance vectorized data contract validation engine powered by Pandas.
    """

    def __init__(self, contract: DataContract):
        self.contract = contract

    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, ValidationSummary]:
        total_rows = len(df)
        if total_rows == 0:
            summary = ValidationSummary(
                is_valid=True,
                total_rows=0,
                valid_rows=0,
                quarantined_rows=0,
                quarantine_rate=0.0
            )
            return df.copy(), df.copy(), summary

        schema_errors = []
        # 1. Check for missing columns declared in contract
        missing_cols = [c.name for c in self.contract.columns if c.name not in df.columns]
        if missing_cols:
            schema_errors.append(f"Missing mandatory columns: {missing_cols}")

        for pk in self.contract.primary_key:
            if pk not in df.columns:
                schema_errors.append(f"Primary key column '{pk}' missing from dataframe")

        if schema_errors:
            # If critical schema errors exist, entire batch fails
            summary = ValidationSummary(
                is_valid=False,
                total_rows=total_rows,
                valid_rows=0,
                quarantined_rows=total_rows,
                quarantine_rate=1.0,
                schema_errors=schema_errors
            )
            df_quarantine = df.copy()
            df_quarantine["_quarantine_reasons"] = "; ".join(schema_errors)
            empty_clean = df.iloc[0:0].copy()
            return empty_clean, df_quarantine, summary

        # Vectorized boolean mask for failures (True means row FAILS validation)
        fail_mask = np.zeros(total_rows, dtype=bool)
        row_reasons: Dict[int, List[str]] = {i: [] for i in range(total_rows)}
        column_violations: Dict[str, int] = {}

        # 2. Check Primary Key Nullity & Uniqueness
        for pk in self.contract.primary_key:
            pk_nulls = df[pk].isna().to_numpy()
            if np.any(pk_nulls):
                fail_mask |= pk_nulls
                indices = np.where(pk_nulls)[0]
                column_violations[f"{pk}_pk_null"] = len(indices)
                for idx in indices:
                    row_reasons[idx].append(f"Primary key '{pk}' cannot be null")

        # 3. Check Each Column Assertions
        for col in self.contract.columns:
            series = df[col.name]
            col_fail = np.zeros(total_rows, dtype=bool)

            # Null check
            null_mask = series.isna().to_numpy()
            if not col.nullable and np.any(null_mask):
                col_fail |= null_mask
                for idx in np.where(null_mask)[0]:
                    row_reasons[idx].append(f"Column '{col.name}' is non-nullable but contains null")

            # Non-null values for further validations
            valid_val_mask = ~null_mask

            # Numeric & Type Checks
            if col.data_type in ("int", "float"):
                # Check coercibility
                numeric_series = pd.to_numeric(series, errors="coerce")
                type_fails = valid_val_mask & numeric_series.isna().to_numpy()
                if np.any(type_fails):
                    col_fail |= type_fails
                    for idx in np.where(type_fails)[0]:
                        row_reasons[idx].append(f"Column '{col.name}' could not coerce to {col.data_type}")

                # Range checks on coerced numeric values
                coerced = numeric_series.to_numpy()
                if col.min_value is not None:
                    min_fails = valid_val_mask & ~np.isnan(coerced) & (coerced < col.min_value)
                    if np.any(min_fails):
                        col_fail |= min_fails
                        for idx in np.where(min_fails)[0]:
                            row_reasons[idx].append(f"Column '{col.name}' value {coerced[idx]} < min {col.min_value}")

                if col.max_value is not None:
                    max_fails = valid_val_mask & ~np.isnan(coerced) & (coerced > col.max_value)
                    if np.any(max_fails):
                        col_fail |= max_fails
                        for idx in np.where(max_fails)[0]:
                            row_reasons[idx].append(f"Column '{col.name}' value {coerced[idx]} > max {col.max_value}")

            # Regex pattern check
            if col.regex_pattern and col.data_type == "str":
                pattern = re.compile(col.regex_pattern)
                for idx in np.where(valid_val_mask)[0]:
                    val_str = str(series.iloc[idx])
                    if not pattern.match(val_str):
                        col_fail[idx] = True
                        row_reasons[idx].append(f"Column '{col.name}' value '{val_str}' failed regex {col.regex_pattern}")

            # Allowed values (Enum check)
            if col.allowed_values is not None:
                allowed_set = set(col.allowed_values)
                for idx in np.where(valid_val_mask)[0]:
                    val = series.iloc[idx]
                    if val not in allowed_set:
                        col_fail[idx] = True
                        row_reasons[idx].append(f"Column '{col.name}' value '{val}' not in allowed set")

            viol_count = int(np.sum(col_fail))
            if viol_count > 0:
                column_violations[col.name] = viol_count
            fail_mask |= col_fail

        # Split DataFrames
        clean_indices = np.where(~fail_mask)[0]
        quarantine_indices = np.where(fail_mask)[0]

        df_clean = df.iloc[clean_indices].copy().reset_index(drop=True)
        df_quarantine = df.iloc[quarantine_indices].copy().reset_index(drop=True)

        if len(df_quarantine) > 0:
            df_quarantine["_quarantine_reasons"] = [
                "; ".join(row_reasons[idx]) for idx in quarantine_indices
            ]

        quarantine_rate = len(df_quarantine) / total_rows if total_rows > 0 else 0.0
        is_valid = quarantine_rate <= self.contract.max_quarantine_rate

        summary = ValidationSummary(
            is_valid=is_valid,
            total_rows=total_rows,
            valid_rows=len(df_clean),
            quarantined_rows=len(df_quarantine),
            quarantine_rate=round(quarantine_rate, 4),
            column_violations=column_violations,
            schema_errors=[]
        )

        return df_clean, df_quarantine, summary

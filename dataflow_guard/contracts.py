import json
from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, field_validator


VALID_DATA_TYPES = {"int", "float", "str", "datetime", "bool"}


class ColumnContract(BaseModel):
    name: str
    data_type: str
    nullable: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    regex_pattern: Optional[str] = None
    allowed_values: Optional[List[str]] = None
    description: Optional[str] = None

    @field_validator("data_type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        v_clean = v.strip().lower()
        if v_clean not in VALID_DATA_TYPES:
            raise ValueError(f"Unsupported data_type: {v}. Must be one of {VALID_DATA_TYPES}")
        return v_clean


class DataContract(BaseModel):
    name: str
    version: str = "1.0.0"
    primary_key: List[str] = Field(default_factory=list)
    columns: List[ColumnContract]
    max_quarantine_rate: float = Field(default=0.25, ge=0.0, le=1.0)
    description: Optional[str] = None

    @field_validator("primary_key")
    @classmethod
    def validate_pk(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("Primary key cannot be empty")
        return v

    def get_column(self, col_name: str) -> Optional[ColumnContract]:
        for col in self.columns:
            if col.name == col_name:
                return col
        return None

    @classmethod
    def from_json(cls, json_path: str) -> "DataContract":
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls(**data)

    def to_json(self, json_path: str):
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.model_dump(), f, indent=2)

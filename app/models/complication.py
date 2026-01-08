"""Complication model."""
from typing import Optional

from sqlmodel import Field, SQLModel


class Complication(SQLModel, table=True):
    """Complications and risk factors data model."""
    
    id: Optional[int] = Field(default=None, primary_key=True)
    mother_id: Optional[int] = Field(default=None, foreign_key="mother.id")
    complication_type: Optional[str] = Field(default=None, index=True)  # Diabetes, Hypertension, etc.
    severity: Optional[str] = Field(default=None)
    lhd: Optional[str] = Field(default=None, index=True)
    year: int = Field(default=2023, index=True)
    
    # Statistics fields
    total_cases: Optional[int] = Field(default=None)
    percentage: Optional[float] = Field(default=None)


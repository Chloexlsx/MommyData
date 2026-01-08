"""Antenatal care model."""
from typing import Optional

from sqlmodel import Field, SQLModel


class AntenatalCare(SQLModel, table=True):
    """Antenatal care data model."""
    
    id: Optional[int] = Field(default=None, primary_key=True)
    mother_id: Optional[int] = Field(default=None, foreign_key="mother.id")
    first_visit_week: Optional[int] = Field(default=None, index=True)
    first_visit_category: Optional[str] = Field(default=None, index=True)  # <12, 12-20, >20
    visit_count: Optional[int] = Field(default=None)
    risk_level: Optional[str] = Field(default=None)
    lhd: Optional[str] = Field(default=None, index=True)
    year: int = Field(default=2023, index=True)
    
    # Statistics fields
    total_cases: Optional[int] = Field(default=None)
    percentage: Optional[float] = Field(default=None)


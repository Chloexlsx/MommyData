"""Baby model."""
from typing import Optional

from sqlmodel import Field, SQLModel


class Baby(SQLModel, table=True):
    """Baby health indicators data model."""
    
    id: Optional[int] = Field(default=None, primary_key=True)
    birth_id: Optional[int] = Field(default=None, foreign_key="birth.id")
    birth_weight: Optional[float] = Field(default=None, index=True)
    birth_weight_category: Optional[str] = Field(default=None, index=True)  # Low, Normal, High
    apgar_score: Optional[int] = Field(default=None, index=True)
    apgar_category: Optional[str] = Field(default=None, index=True)  # Low (<7), Normal (≥7)
    nicu_admission: Optional[bool] = Field(default=None, index=True)
    scunicu_admission: Optional[bool] = Field(default=None, index=True)
    breastfeeding_initiated: Optional[bool] = Field(default=None, index=True)
    breastfeeding_at_discharge: Optional[bool] = Field(default=None, index=True)
    lhd: Optional[str] = Field(default=None, index=True)
    year: int = Field(default=2023, index=True)
    
    # Statistics fields
    total_babies: Optional[int] = Field(default=None)
    percentage: Optional[float] = Field(default=None)


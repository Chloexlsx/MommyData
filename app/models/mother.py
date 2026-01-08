"""Mother model."""
from typing import Optional

from sqlmodel import Field, SQLModel


class Mother(SQLModel, table=True):
    """Mother characteristics data model."""
    
    id: Optional[int] = Field(default=None, primary_key=True)
    age_group: Optional[str] = Field(default=None, index=True)
    smoking_status: Optional[str] = Field(default=None, index=True)
    bmi_category: Optional[str] = Field(default=None, index=True)
    diabetes_pre: Optional[bool] = Field(default=None, index=True)
    hypertension_pre: Optional[bool] = Field(default=None, index=True)
    diabetes_subgroup: Optional[str] = Field(default=None, index=True)  # Pre-existing diabetes, Gestational diabetes, etc.
    hypertension_subgroup: Optional[str] = Field(default=None, index=True)  # Pre-existing hypertension, Gestational hypertension, etc.
    cultural_background: Optional[str] = Field(default=None)
    lhd: Optional[str] = Field(default=None, index=True)
    lga: Optional[str] = Field(default=None, index=True)
    parity: Optional[int] = Field(default=None)
    year: int = Field(default=2023, index=True)
    
    # Statistics fields (aggregated data)
    total_mothers: Optional[int] = Field(default=None)
    percentage: Optional[float] = Field(default=None)


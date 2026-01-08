"""Hospital models."""
from typing import Optional

from sqlmodel import Field, SQLModel


class Hospital(SQLModel, table=True):
    """Hospital data model."""
    
    id: Optional[int] = Field(default=None, primary_key=True)
    hospital_name: str = Field(index=True)
    lhd: Optional[str] = Field(default=None, index=True)
    lga: Optional[str] = Field(default=None, index=True)
    hospital_type: Optional[str] = Field(default=None)
    hospital_level: Optional[str] = Field(default=None)
    year: int = Field(default=2023, index=True)


class HospitalStat(SQLModel, table=True):
    """Hospital statistics data model."""
    
    id: Optional[int] = Field(default=None, primary_key=True)
    hospital_id: int = Field(foreign_key="hospital.id", index=True)
    metric_name: str = Field(index=True)  # birth_type, pain_relief, stay_days, breastfeeding, etc.
    metric_value: Optional[float] = Field(default=None)
    metric_category: Optional[str] = Field(default=None, index=True)  # For categorical metrics
    total_cases: Optional[int] = Field(default=None)
    percentage: Optional[float] = Field(default=None)
    year: int = Field(default=2023, index=True)


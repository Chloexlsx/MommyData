"""Birth model."""
from typing import Optional

from sqlmodel import Field, SQLModel


class Birth(SQLModel, table=True):
    """Birth outcomes data model."""
    
    id: Optional[int] = Field(default=None, primary_key=True)
    mother_id: Optional[int] = Field(default=None, foreign_key="mother.id")
    birth_type: Optional[str] = Field(default=None, index=True)  # Spontaneous, Induced, Caesarean
    onset_labour: Optional[str] = Field(default=None, index=True)  # Spontaneous, Induced
    birth_location: Optional[str] = Field(default=None)
    hospital_name: Optional[str] = Field(default=None, index=True)
    hospital_id: Optional[int] = Field(default=None, foreign_key="hospital.id")
    pain_relief_type: Optional[str] = Field(default=None, index=True)
    hospital_stay_days: Optional[float] = Field(default=None)
    gestational_age: Optional[int] = Field(default=None, index=True)
    gestational_age_category: Optional[str] = Field(default=None, index=True)  # Preterm, Term, Post-term
    lhd: Optional[str] = Field(default=None, index=True)
    year: int = Field(default=2023, index=True)
    
    # Statistics fields
    total_births: Optional[int] = Field(default=None)
    percentage: Optional[float] = Field(default=None)


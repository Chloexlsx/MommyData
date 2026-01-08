"""Data import utilities for Excel files."""
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


def read_excel_file(file_path: Path) -> Dict[str, pd.DataFrame]:
    """Read Excel file and return all sheets as a dictionary."""
    try:
        return pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return {}


def normalize_column_name(col: str) -> str:
    """Normalize column names to snake_case."""
    if pd.isna(col):
        return "unknown"
    return str(col).lower().strip().replace(" ", "_").replace("-", "_")


def parse_age_group(age_str: Optional[str]) -> Optional[str]:
    """Parse age group string to standardized format."""
    if not age_str or pd.isna(age_str):
        return None
    age_str = str(age_str).strip()
    
    # Skip if it's clearly not an age group
    if any(x in age_str.lower() for x in ["maternal", "age", "year", "total", "all", "not stated"]):
        return None
    
    # Handle various formats like "15-19", "15 to 19", "15–19" (en dash), etc.
    if "to" in age_str.lower():
        age_str = age_str.replace("to", "-")
    # Replace en dash and em dash with hyphen
    age_str = age_str.replace("–", "-").replace("—", "-")
    
    # Validate it looks like an age group (contains numbers and dash)
    if "-" in age_str and any(c.isdigit() for c in age_str):
        # Extract just the age range part
        parts = age_str.split()
        for part in parts:
            if "-" in part and any(c.isdigit() for c in part):
                return part
    
    # If it's just a number (like "40+"), return as is
    if age_str.endswith("+") and age_str[:-1].isdigit():
        return age_str
    
    return None


def parse_bmi_category(bmi_str: Optional[str]) -> Optional[str]:
    """Parse BMI category string."""
    if not bmi_str or pd.isna(bmi_str):
        return None
    bmi_str = str(bmi_str).strip().lower()
    # Map to standard categories
    if "underweight" in bmi_str or "<18.5" in bmi_str:
        return "underweight"
    elif "normal" in bmi_str or "18.5-24.9" in bmi_str:
        return "normal"
    elif "overweight" in bmi_str or "25-29.9" in bmi_str:
        return "overweight"
    elif "obese" in bmi_str or "≥30" in bmi_str or ">=30" in bmi_str:
        return "obese"
    return bmi_str


def parse_smoking_status(smoking_str: Optional[str]) -> Optional[str]:
    """Parse smoking status."""
    if not smoking_str or pd.isna(smoking_str):
        return None
    smoking_str = str(smoking_str).strip().lower()
    if "yes" in smoking_str or "smoker" in smoking_str:
        return "yes"
    elif "no" in smoking_str or "non-smoker" in smoking_str:
        return "no"
    return smoking_str


def parse_first_visit_category(week: Optional[Any]) -> Optional[str]:
    """Parse first visit week to category."""
    if pd.isna(week) or week is None:
        return None
    try:
        week_num = float(week)
        if week_num < 12:
            return "<12"
        elif week_num <= 20:
            return "12-20"
        else:
            return ">20"
    except (ValueError, TypeError):
        return None


def parse_gestational_age_category(week: Optional[Any]) -> Optional[str]:
    """Parse gestational age to category."""
    if pd.isna(week) or week is None:
        return None
    try:
        week_num = float(week)
        if week_num < 37:
            return "preterm"
        elif week_num < 42:
            return "term"
        else:
            return "post-term"
    except (ValueError, TypeError):
        return None


def parse_birth_weight_category(weight: Optional[Any]) -> Optional[str]:
    """Parse birth weight to category."""
    if pd.isna(weight) or weight is None:
        return None
    try:
        weight_num = float(weight)
        if weight_num < 2500:
            return "low"
        elif weight_num <= 4000:
            return "normal"
        else:
            return "high"
    except (ValueError, TypeError):
        return None


def parse_apgar_category(apgar: Optional[Any]) -> Optional[str]:
    """Parse Apgar score to category."""
    if pd.isna(apgar) or apgar is None:
        return None
    try:
        apgar_num = int(float(apgar))
        if apgar_num < 7:
            return "low"
        else:
            return "normal"
    except (ValueError, TypeError):
        return None


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize dataframe."""
    if df.empty:
        return df
    
    # Normalize column names
    df.columns = [normalize_column_name(col) for col in df.columns]
    
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    return df


def extract_statistics_from_table(df: pd.DataFrame, value_columns: List[str]) -> List[Dict[str, Any]]:
    """Extract statistics from a data table."""
    records = []
    
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            if col in value_columns:
                try:
                    val = row[col]
                    if pd.notna(val):
                        record[col] = float(val) if isinstance(val, (int, float)) else val
                except (ValueError, TypeError):
                    pass
            else:
                val = row[col]
                if pd.notna(val):
                    record[col] = str(val).strip()
        if record:
            records.append(record)
    
    return records


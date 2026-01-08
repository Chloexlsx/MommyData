"""Data import script for NSW Mothers and Babies 2023 data."""
import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.database import async_session, init_db
from app.models import (
    Mother,
    AntenatalCare,
    Birth,
    Baby,
    Complication,
    Hospital,
    HospitalStat,
)
from app.utils.data_import import (
    clean_dataframe,
    parse_age_group,
    parse_bmi_category,
    parse_smoking_status,
    parse_first_visit_category,
    parse_gestational_age_category,
    parse_birth_weight_category,
    parse_apgar_category,
    extract_statistics_from_table,
)
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_excel_safely(path: str) -> pd.DataFrame:
    """Safely read Excel file with appropriate engine."""
    engine = "xlrd" if path.lower().endswith(".xls") else "openpyxl"
    try:
        return pd.read_excel(path, engine=engine)
    except Exception:
        logger.exception(f"Failed to read {path} (engine={engine})")
        raise


def read_excel_file(file_path: Path) -> dict[str, pd.DataFrame]:
    """Read Excel file and return all sheets as a dictionary."""
    engine = "xlrd" if str(file_path).lower().endswith(".xls") else "openpyxl"
    try:
        return pd.read_excel(str(file_path), sheet_name=None, engine=engine)
    except Exception:
        logger.exception(f"Failed to read {file_path} (engine={engine})")
        raise


async def import_maternal_age(session: AsyncSession, data_dir: Path):
    """Import maternal age data."""
    file_path = data_dir / "2023-table-3-maternal-age.xls"
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return
    
    logger.info(f"Importing maternal age data from {file_path}")
    try:
        # Read with header=None to get raw data, then find the right rows
        df = pd.read_excel(str(file_path), engine='xlrd', header=None)
    except Exception:
        logger.error(f"Failed to import maternal age data from {file_path}")
        raise
    
    batch = []
    batch_size = 100
    
    # Find the row with 2023 data
    # Row 3 (index 3) has years: 2019, 2020, 2021, 2022, 2023
    # Row 4 (index 4) has headers: No., %, No., %, etc.
    # Data starts from row 5 (index 5)
    
    # Find 2023 columns
    year_row_idx = 3  # Row 4 with years
    header_row_idx = 4  # Row 5 with No./%
    
    if year_row_idx < len(df) and header_row_idx < len(df):
        year_row = df.iloc[year_row_idx]
        header_row = df.iloc[header_row_idx]
        
        # Find columns with 2023
        col_2023_no = None
        col_2023_pct = None
        
        for idx, (year_val, header_val) in enumerate(zip(year_row, header_row)):
            if pd.notna(year_val) and str(year_val).strip() == "2023":
                if pd.notna(header_val):
                    header_str = str(header_val).strip().lower()
                    if "no" in header_str or "number" in header_str:
                        col_2023_no = idx
                    elif "%" in header_str or "percentage" in header_str:
                        col_2023_pct = idx
        
        # Age group is in first column
        age_col = 0
        
        # Process data rows (start from row 6, index 5)
        for idx in range(5, len(df)):
            row = df.iloc[idx]
            
            age_val = row[age_col] if age_col < len(row) else None
            if pd.isna(age_val):
                continue
            
            age_str = str(age_val).strip()
            
            # Skip header rows and invalid age groups
            if any(x in age_str.lower() for x in ["maternal", "age", "year", "less than", "total", "all"]):
                continue
            
            # Parse age group
            age_group = parse_age_group(age_str)
            if not age_group:
                continue
            
            # Get 2023 data
            total = None
            percentage = None
            
            if col_2023_no is not None and col_2023_no < len(row):
                val = row[col_2023_no]
                if pd.notna(val):
                    try:
                        total = int(float(str(val).replace(",", "")))
                    except (ValueError, TypeError):
                        pass
            
            if col_2023_pct is not None and col_2023_pct < len(row):
                val = row[col_2023_pct]
                if pd.notna(val):
                    try:
                        percentage = float(str(val).replace("%", "").replace(",", ""))
                    except (ValueError, TypeError):
                        pass
            
            if age_group and (total is not None or percentage is not None):
                mother = Mother(
                    age_group=age_group,
                    total_mothers=total,
                    percentage=percentage,
                    year=2023,
                )
                batch.append(mother)
                
                if len(batch) >= batch_size:
                    session.add_all(batch)
                    await session.commit()
                    logger.info(f"Committed batch of {len(batch)} records")
                    batch = []
    
    # Commit remaining
    if batch:
        session.add_all(batch)
        await session.commit()
        logger.info(f"Committed final batch of {len(batch)} records")
    
    logger.info("Maternal age data imported successfully")


async def import_maternal_bmi(session: AsyncSession, data_dir: Path):
    """Import maternal BMI data."""
    file_path = data_dir / "2023-table-29-maternal-bmi.xls"
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return
    
    logger.info(f"Importing maternal BMI data from {file_path}")
    try:
        # Read raw data - similar structure to age data
        df = pd.read_excel(str(file_path), engine='xlrd', header=None)
    except Exception:
        logger.error(f"Failed to import maternal BMI data from {file_path}")
        raise
    
    batch = []
    batch_size = 100
    
    # BMI file structure: Row 3 has BMI categories, Row 4 has No./%, data starts from Row 5
    # This file is organized by LHD, not by year
    if len(df) > 4:
        category_row = df.iloc[3]  # Row 4 with BMI categories
        header_row = df.iloc[4]  # Row 5 with No./%
        
        lhd_col = 0
        
        # Find columns for each BMI category
        bmi_categories = {
            "underweight": {"no": None, "pct": None},
            "normal": {"no": None, "pct": None},
            "overweight": {"no": None, "pct": None},
            "obese": {"no": None, "pct": None},
        }
        
        for idx, (cat_val, header_val) in enumerate(zip(category_row, header_row)):
            if pd.notna(cat_val):
                cat_str = str(cat_val).strip().lower()
                if pd.notna(header_val):
                    header_str = str(header_val).strip().lower()
                    
                    if "underweight" in cat_str:
                        if "no" in header_str or "number" in header_str:
                            bmi_categories["underweight"]["no"] = idx
                        elif "%" in header_str or "percentage" in header_str:
                            bmi_categories["underweight"]["pct"] = idx
                    elif "healthy" in cat_str or "normal" in cat_str:
                        if "no" in header_str or "number" in header_str:
                            bmi_categories["normal"]["no"] = idx
                        elif "%" in header_str or "percentage" in header_str:
                            bmi_categories["normal"]["pct"] = idx
                    elif "overweight" in cat_str and "obese" not in cat_str:
                        if "no" in header_str or "number" in header_str:
                            bmi_categories["overweight"]["no"] = idx
                        elif "%" in header_str or "percentage" in header_str:
                            bmi_categories["overweight"]["pct"] = idx
                    elif "obese" in cat_str:
                        if "no" in header_str or "number" in header_str:
                            bmi_categories["obese"]["no"] = idx
                        elif "%" in header_str or "percentage" in header_str:
                            bmi_categories["obese"]["pct"] = idx
        
        # Process data rows
        for idx in range(5, len(df)):
            row = df.iloc[idx]
            
            lhd_val = row[lhd_col] if lhd_col < len(row) else None
            if pd.isna(lhd_val):
                continue
            
            lhd = str(lhd_val).strip()
            
            # Skip header rows
            if any(x in lhd.lower() for x in ["local health", "district", "total", "all", "source"]):
                continue
            
            # Process each BMI category
            for bmi_cat, cols in bmi_categories.items():
                total = None
                percentage = None
                
                if cols["no"] is not None and cols["no"] < len(row):
                    val = row[cols["no"]]
                    if pd.notna(val):
                        try:
                            total = int(float(str(val).replace(",", "")))
                        except (ValueError, TypeError):
                            pass
                
                if cols["pct"] is not None and cols["pct"] < len(row):
                    val = row[cols["pct"]]
                    if pd.notna(val):
                        try:
                            percentage = float(str(val).replace("%", "").replace(",", ""))
                        except (ValueError, TypeError):
                            pass
                
                if total is not None or percentage is not None:
                    mother = Mother(
                        bmi_category=bmi_cat,
                        lhd=lhd,
                        total_mothers=total,
                        percentage=percentage,
                        year=2023,
                    )
                    batch.append(mother)
                    
                    if len(batch) >= batch_size:
                        session.add_all(batch)
                        await session.commit()
                        logger.info(f"Committed batch of {len(batch)} records")
                        batch = []
    
    if batch:
        session.add_all(batch)
        await session.commit()
        logger.info(f"Committed final batch of {len(batch)} records")
    
    logger.info("Maternal BMI data imported successfully")


async def import_smoking(session: AsyncSession, data_dir: Path):
    """Import smoking data."""
    file_path = data_dir / "2023-table-28-smoking-by-lhd.xls"
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return
    
    logger.info(f"Importing smoking data from {file_path}")
    try:
        # Read raw data
        df = pd.read_excel(str(file_path), engine='xlrd', header=None)
    except Exception:
        logger.error(f"Failed to import smoking data from {file_path}")
        raise
    
    batch = []
    batch_size = 100
    
    # Find LHD column (first column) and "Did not smoke" columns
    # Row 3 (index 3) has headers like "Did not smoke"
    # Row 4 (index 4) has "No." and "%"
    # Data starts from row 5 (index 5)
    
    if len(df) > 4:
        header_row = df.iloc[3]  # Row 4 with smoking status headers
        subheader_row = df.iloc[4]  # Row 5 with No./%
        
        lhd_col = 0
        
        # Find "Did not smoke" columns (No. and %)
        did_not_smoke_no_col = None
        did_not_smoke_pct_col = None
        
        for idx, (header_val, subheader_val) in enumerate(zip(header_row, subheader_row)):
            if pd.notna(header_val):
                header_str = str(header_val).strip().lower()
                if "did not smoke" in header_str or "not smoke" in header_str:
                    if pd.notna(subheader_val):
                        subheader_str = str(subheader_val).strip().lower()
                        if "no" in subheader_str or "number" in subheader_str:
                            did_not_smoke_no_col = idx
                        elif "%" in subheader_str or "percentage" in subheader_str:
                            did_not_smoke_pct_col = idx
        
        # Process data rows
        for idx in range(5, len(df)):
            row = df.iloc[idx]
            
            # Get LHD
            lhd_val = row[lhd_col] if lhd_col < len(row) else None
            if pd.isna(lhd_val):
                continue
            
            lhd = str(lhd_val).strip()
            
            # Skip header rows
            if any(x in lhd.lower() for x in ["local health", "district", "total", "all"]):
                continue
            
            # Get "Did not smoke" data (this means smoking_status = "no")
            total = None
            percentage = None
            
            if did_not_smoke_no_col is not None and did_not_smoke_no_col < len(row):
                val = row[did_not_smoke_no_col]
                if pd.notna(val):
                    try:
                        total = int(float(str(val).replace(",", "")))
                    except (ValueError, TypeError):
                        pass
            
            if did_not_smoke_pct_col is not None and did_not_smoke_pct_col < len(row):
                val = row[did_not_smoke_pct_col]
                if pd.notna(val):
                    try:
                        percentage = float(str(val).replace("%", "").replace(",", ""))
                    except (ValueError, TypeError):
                        pass
            
            if lhd and (total is not None or percentage is not None):
                mother = Mother(
                    smoking_status="no",  # "Did not smoke"
                    lhd=lhd,
                    total_mothers=total,
                    percentage=percentage,
                    year=2023,
                )
                batch.append(mother)
                
                if len(batch) >= batch_size:
                    session.add_all(batch)
                    await session.commit()
                    logger.info(f"Committed batch of {len(batch)} records")
                    batch = []
    
    if batch:
        session.add_all(batch)
        await session.commit()
        logger.info(f"Committed final batch of {len(batch)} records")
    
    logger.info("Smoking data imported successfully")


async def import_diabetes_hypertension_csv(session: AsyncSession, data_dir: Path):
    """Import diabetes and hypertension data from CSV file."""
    file_path = data_dir / "AIHW-PER-101-National-Perinatal-Data-Collection-annual-update-data-visualisation-D&H-2023.csv"
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return
    
    logger.info(f"Importing diabetes and hypertension data from {file_path}")
    try:
        df = pd.read_csv(str(file_path))
    except Exception:
        logger.error(f"Failed to import data from {file_path}")
        raise
    
    batch = []
    batch_size = 100
    
    # Store data by age group, year, sub-group disaggregation, and factor
    # Structure: {(age_group, year, sub_group_disagg): {'num': num, 'den': den}}
    diabetes_data = {}
    hypertension_data = {}
    
    # Process each row
    for idx, row in df.iterrows():
        # Skip header row if it exists
        if pd.isna(row.get('Year')) or str(row.get('Year')).strip() == 'Year':
            continue
        
        # Get basic info
        sub_group = str(row.get('Sub-group', '')).strip()
        sub_group_disagg = str(row.get('Sub-group disaggregation', '')).strip()
        topic_disagg = str(row.get('Topic disaggregation', '')).strip()
        year = row.get('Year')
        numerator = row.get('Numerator')
        denominator = row.get('Denominator')
        
        # Skip if missing essential data
        if pd.isna(year) or pd.isna(denominator):
            continue
        
        # Parse year
        try:
            year_int = int(float(str(year)))
        except (ValueError, TypeError):
            continue
        
        # Parse numerator and denominator (remove commas and quotes)
        try:
            num_str = str(numerator).replace(',', '').replace('"', '').strip()
            den_str = str(denominator).replace(',', '').replace('"', '').strip()
            num_val = int(float(num_str)) if num_str and num_str != 'nan' and num_str != '' else 0
            den_val = int(float(den_str)) if den_str and den_str != 'nan' and den_str != '' else 0
        except (ValueError, TypeError):
            continue
        
        if den_val == 0:
            continue
        
        # Parse age group
        age_group = None
        if topic_disagg and topic_disagg.lower() not in ['total', 'not stated']:
            age_group = topic_disagg.strip()
        
        if not age_group:
            continue
        
        # Process diabetes data
        if 'diabetes' in sub_group.lower():
            key = (age_group, year_int, sub_group_disagg)
            if key not in diabetes_data:
                diabetes_data[key] = {'num': 0, 'den': 0}
            
            # Update denominator (use the largest for each age group/year/subgroup)
            if den_val > diabetes_data[key]['den']:
                diabetes_data[key]['den'] = den_val
            
            diabetes_data[key]['num'] = num_val
        
        # Process hypertension data
        elif 'hypertension' in sub_group.lower():
            key = (age_group, year_int, sub_group_disagg)
            if key not in hypertension_data:
                hypertension_data[key] = {'num': 0, 'den': 0}
            
            # Update denominator (use the largest for each age group/year/subgroup)
            if den_val > hypertension_data[key]['den']:
                hypertension_data[key]['den'] = den_val
            
            hypertension_data[key]['num'] = num_val
    
    # Create Mother records for diabetes
    for (age_group, year, sub_group_disagg), data in diabetes_data.items():
        if data['den'] > 0:
            percentage = (data['num'] / data['den'] * 100) if data['den'] > 0 else 0.0
            # Determine diabetes_pre based on sub_group_disagg
            has_diabetes = sub_group_disagg.lower() not in ['diabetes - none', 'diabetes - not stated']
            mother = Mother(
                age_group=age_group,
                diabetes_pre=has_diabetes,
                diabetes_subgroup=sub_group_disagg,
                total_mothers=data['num'],
                percentage=percentage,
                year=year,
            )
            batch.append(mother)
            
            if len(batch) >= batch_size:
                session.add_all(batch)
                await session.commit()
                logger.info(f"Committed batch of {len(batch)} diabetes records")
                batch = []
    
    # Create Mother records for hypertension
    for (age_group, year, sub_group_disagg), data in hypertension_data.items():
        if data['den'] > 0:
            percentage = (data['num'] / data['den'] * 100) if data['den'] > 0 else 0.0
            # Determine hypertension_pre based on sub_group_disagg
            has_hypertension = sub_group_disagg.lower() not in ['hypertension - none', 'hypertension - not stated']
            mother = Mother(
                age_group=age_group,
                hypertension_pre=has_hypertension,
                hypertension_subgroup=sub_group_disagg,
                total_mothers=data['num'],
                percentage=percentage,
                year=year,
            )
            batch.append(mother)
            
            if len(batch) >= batch_size:
                session.add_all(batch)
                await session.commit()
                logger.info(f"Committed batch of {len(batch)} hypertension records")
                batch = []
    
    if batch:
        session.add_all(batch)
        await session.commit()
        logger.info(f"Committed final batch of {len(batch)} records")
    
    logger.info("Diabetes and hypertension CSV data imported successfully")


async def import_diabetes(session: AsyncSession, data_dir: Path):
    """Import diabetes data - Yes/No distribution."""
    file_path = data_dir / "2023-table-11-diabetes.xls"
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return
    
    logger.info(f"Importing diabetes data from {file_path}")
    try:
        df = pd.read_excel(str(file_path), engine='xlrd', header=None)
    except Exception:
        logger.error(f"Failed to import diabetes data from {file_path}")
        raise
    
    # Find 2023 columns
    if len(df) > 4:
        year_row = df.iloc[3]  # Row 4 with years
        header_row = df.iloc[4]  # Row 5 with No./%
        
        col_2023_no = None
        col_2023_pct = None
        
        for idx, (year_val, header_val) in enumerate(zip(year_row, header_row)):
            if pd.notna(year_val) and str(year_val).strip() == "2023":
                if pd.notna(header_val):
                    header_str = str(header_val).strip().lower()
                    if "no" in header_str or "number" in header_str:
                        col_2023_no = idx
                    elif "%" in header_str or "percentage" in header_str:
                        col_2023_pct = idx
        
        # Find "Total diabetes" row
        total_diabetes_count = None
        total_diabetes_pct = None
        total_births = None
        
        for idx in range(5, len(df)):
            row = df.iloc[idx]
            condition_val = row[0] if len(row) > 0 else None
            if pd.isna(condition_val):
                continue
            
            condition_str = str(condition_val).strip().lower()
            
            if "total diabetes" in condition_str:
                if col_2023_no is not None and col_2023_no < len(row):
                    val = row[col_2023_no]
                    if pd.notna(val):
                        try:
                            total_diabetes_count = int(float(str(val).replace(",", "")))
                        except (ValueError, TypeError):
                            pass
                
                if col_2023_pct is not None and col_2023_pct < len(row):
                    val = row[col_2023_pct]
                    if pd.notna(val):
                        try:
                            total_diabetes_pct = float(str(val).replace("%", "").replace(",", ""))
                        except (ValueError, TypeError):
                            pass
        
        # Get total births from age file
        age_file = data_dir / "2023-table-3-maternal-age.xls"
        if age_file.exists():
            try:
                age_df = pd.read_excel(str(age_file), engine='xlrd', header=None)
                for idx in range(5, len(age_df)):
                    row = age_df.iloc[idx]
                    val = row[0] if len(row) > 0 else None
                    if pd.notna(val) and "total" in str(val).lower():
                        # Find 2023 total column
                        year_row_age = age_df.iloc[3]
                        header_row_age = age_df.iloc[4]
                        for col_idx, (y, h) in enumerate(zip(year_row_age, header_row_age)):
                            if pd.notna(y) and str(y).strip() == "2023":
                                if pd.notna(h) and ("no" in str(h).lower() or "number" in str(h).lower()):
                                    total_val = row[col_idx] if col_idx < len(row) else None
                                    if pd.notna(total_val):
                                        try:
                                            total_births = int(float(str(total_val).replace(",", "")))
                                            break
                                        except (ValueError, TypeError):
                                            pass
                        break
            except Exception:
                logger.warning("Could not read total births from age file")
        
        # Calculate Yes/No distribution
        if total_diabetes_count is not None and total_births is not None:
            no_diabetes_count = total_births - total_diabetes_count
            no_diabetes_pct = (no_diabetes_count / total_births * 100) if total_births > 0 else 0.0
            
            # Import Yes
            if total_diabetes_count > 0:
                mother_yes = Mother(
                    diabetes_pre=True,
                    total_mothers=total_diabetes_count,
                    percentage=total_diabetes_pct if total_diabetes_pct else (total_diabetes_count / total_births * 100),
                    year=2023,
                )
                session.add(mother_yes)
            
            # Import No
            if no_diabetes_count > 0:
                mother_no = Mother(
                    diabetes_pre=False,
                    total_mothers=no_diabetes_count,
                    percentage=no_diabetes_pct,
                    year=2023,
                )
                session.add(mother_no)
            
            await session.commit()
            logger.info(f"Diabetes data imported: Yes={total_diabetes_count}, No={no_diabetes_count}")
        else:
            logger.warning("Could not determine diabetes distribution - missing data")
    
    logger.info("Diabetes data imported successfully")


async def import_hypertension(session: AsyncSession, data_dir: Path):
    """Import hypertension data - Yes/No distribution."""
    file_path = data_dir / "2023-table-12-hypertension.xls"
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return
    
    logger.info(f"Importing hypertension data from {file_path}")
    try:
        df = pd.read_excel(str(file_path), engine='xlrd', header=None)
    except Exception:
        logger.error(f"Failed to import hypertension data from {file_path}")
        raise
    
    # Find 2023 columns
    if len(df) > 4:
        year_row = df.iloc[3]  # Row 4 with years
        header_row = df.iloc[4]  # Row 5 with No./%
        
        col_2023_no = None
        col_2023_pct = None
        
        for idx, (year_val, header_val) in enumerate(zip(year_row, header_row)):
            if pd.notna(year_val) and str(year_val).strip() == "2023":
                if pd.notna(header_val):
                    header_str = str(header_val).strip().lower()
                    if "no" in header_str or "number" in header_str:
                        col_2023_no = idx
                    elif "%" in header_str or "percentage" in header_str:
                        col_2023_pct = idx
        
        # Find "Any type of hypertension" row
        total_hypertension_count = None
        total_hypertension_pct = None
        total_births = None
        
        for idx in range(5, len(df)):
            row = df.iloc[idx]
            condition_val = row[0] if len(row) > 0 else None
            if pd.isna(condition_val):
                continue
            
            condition_str = str(condition_val).strip().lower()
            
            if "any type" in condition_str and "hypertension" in condition_str:
                if col_2023_no is not None and col_2023_no < len(row):
                    val = row[col_2023_no]
                    if pd.notna(val):
                        try:
                            total_hypertension_count = int(float(str(val).replace(",", "")))
                        except (ValueError, TypeError):
                            pass
                
                if col_2023_pct is not None and col_2023_pct < len(row):
                    val = row[col_2023_pct]
                    if pd.notna(val):
                        try:
                            total_hypertension_pct = float(str(val).replace("%", "").replace(",", ""))
                        except (ValueError, TypeError):
                            pass
        
        # Get total births from age file
        age_file = data_dir / "2023-table-3-maternal-age.xls"
        if age_file.exists():
            try:
                age_df = pd.read_excel(str(age_file), engine='xlrd', header=None)
                for idx in range(5, len(age_df)):
                    row = age_df.iloc[idx]
                    val = row[0] if len(row) > 0 else None
                    if pd.notna(val) and "total" in str(val).lower():
                        # Find 2023 total column
                        year_row_age = age_df.iloc[3]
                        header_row_age = age_df.iloc[4]
                        for col_idx, (y, h) in enumerate(zip(year_row_age, header_row_age)):
                            if pd.notna(y) and str(y).strip() == "2023":
                                if pd.notna(h) and ("no" in str(h).lower() or "number" in str(h).lower()):
                                    total_val = row[col_idx] if col_idx < len(row) else None
                                    if pd.notna(total_val):
                                        try:
                                            total_births = int(float(str(total_val).replace(",", "")))
                                            break
                                        except (ValueError, TypeError):
                                            pass
                        break
            except Exception:
                logger.warning("Could not read total births from age file")
        
        # Calculate Yes/No distribution
        if total_hypertension_count is not None and total_births is not None:
            no_hypertension_count = total_births - total_hypertension_count
            no_hypertension_pct = (no_hypertension_count / total_births * 100) if total_births > 0 else 0.0
            
            # Import Yes
            if total_hypertension_count > 0:
                mother_yes = Mother(
                    hypertension_pre=True,
                    total_mothers=total_hypertension_count,
                    percentage=total_hypertension_pct if total_hypertension_pct else (total_hypertension_count / total_births * 100),
                    year=2023,
                )
                session.add(mother_yes)
            
            # Import No
            if no_hypertension_count > 0:
                mother_no = Mother(
                    hypertension_pre=False,
                    total_mothers=no_hypertension_count,
                    percentage=no_hypertension_pct,
                    year=2023,
                )
                session.add(mother_no)
            
            await session.commit()
            logger.info(f"Hypertension data imported: Yes={total_hypertension_count}, No={no_hypertension_count}")
        else:
            logger.warning("Could not determine hypertension distribution - missing data")
    
    logger.info("Hypertension data imported successfully")


async def import_birth_type(session: AsyncSession, data_dir: Path):
    """Import birth type data."""
    file_path = data_dir / "2023-table-14-birth-type.xls"
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return
    
    logger.info(f"Importing birth type data from {file_path}")
    try:
        sheets = read_excel_file(file_path)
    except Exception:
        logger.error(f"Failed to import birth type data from {file_path}")
        raise
    
    batch = []
    batch_size = 100
    
    for sheet_name, df in sheets.items():
        df = clean_dataframe(df)
        if df.empty:
            continue
        
        logger.info(f"Processing sheet: {sheet_name}, rows: {len(df)}")
        
        for idx, row in df.iterrows():
            birth_type = None
            total = None
            percentage = None
            
            for col in df.columns:
                val = row[col]
                if pd.notna(val):
                    val_str = str(val).strip().lower()
                    # Check if it's birth type
                    if any(x in val_str for x in ["spontaneous", "induced", "caesarean", "cesarean", "vaginal"]):
                        birth_type = val_str
                    # Check if it's a number
                    try:
                        num_val = float(val_str.replace("%", "").replace(",", ""))
                        if "total" in col.lower() or "number" in col.lower():
                            total = int(num_val)
                        elif "%" in str(row[col]) or "percentage" in col.lower():
                            percentage = num_val
                    except (ValueError, AttributeError):
                        pass
            
            if birth_type or total or percentage:
                birth = Birth(
                    birth_type=birth_type,
                    total_births=total,
                    percentage=percentage,
                    year=2023,
                )
                batch.append(birth)
                
                if len(batch) >= batch_size:
                    session.add_all(batch)
                    await session.commit()
                    logger.info(f"Committed batch of {len(batch)} records")
                    batch = []
    
    if batch:
        session.add_all(batch)
        await session.commit()
        logger.info(f"Committed final batch of {len(batch)} records")
    
    logger.info("Birth type data imported successfully")


async def import_first_visit(session: AsyncSession, data_dir: Path):
    """Import first visit duration data."""
    file_path = data_dir / "2023-table-27-first-visit-duration-lhd.xls"
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return
    
    logger.info(f"Importing first visit duration data from {file_path}")
    try:
        sheets = read_excel_file(file_path)
    except Exception:
        logger.error(f"Failed to import first visit duration data from {file_path}")
        raise
    
    batch = []
    batch_size = 100
    
    for sheet_name, df in sheets.items():
        df = clean_dataframe(df)
        if df.empty:
            continue
        
        logger.info(f"Processing sheet: {sheet_name}, rows: {len(df)}")
        
        for idx, row in df.iterrows():
            first_visit_week = None
            first_visit_category = None
            lhd = None
            total = None
            percentage = None
            
            for col in df.columns:
                val = row[col]
                if pd.notna(val):
                    val_str = str(val).strip()
                    # Check if it's a week number
                    try:
                        week_num = float(val_str)
                        if week_num > 0 and week_num < 50:
                            first_visit_week = int(week_num)
                            first_visit_category = parse_first_visit_category(week_num)
                    except (ValueError, TypeError):
                        pass
                    # Check if it's LHD
                    if "lhd" in col.lower() or "health" in col.lower():
                        lhd = val_str
                    # Check if it's a number
                    try:
                        num_val = float(val_str.replace("%", "").replace(",", ""))
                        if "total" in col.lower() or "number" in col.lower():
                            total = int(num_val)
                        elif "%" in str(row[col]) or "percentage" in col.lower():
                            percentage = num_val
                    except (ValueError, AttributeError):
                        pass
            
            if first_visit_week or first_visit_category or lhd or total or percentage:
                antenatal = AntenatalCare(
                    first_visit_week=first_visit_week,
                    first_visit_category=first_visit_category,
                    lhd=lhd,
                    total_cases=total,
                    percentage=percentage,
                    year=2023,
                )
                batch.append(antenatal)
                
                if len(batch) >= batch_size:
                    session.add_all(batch)
                    await session.commit()
                    logger.info(f"Committed batch of {len(batch)} records")
                    batch = []
    
    if batch:
        session.add_all(batch)
        await session.commit()
        logger.info(f"Committed final batch of {len(batch)} records")
    
    logger.info("First visit duration data imported successfully")


async def import_hospital_data(session: AsyncSession, data_dir: Path):
    """Import hospital data."""
    # Birth type by hospital
    file_path = data_dir / "2023-table-41-birth-type-hospital.xls"
    if file_path.exists():
        logger.info(f"Importing hospital birth type data from {file_path}")
        try:
            sheets = read_excel_file(file_path)
        except Exception:
            logger.error(f"Failed to import hospital birth type data from {file_path}")
            raise
        
        batch = []
        batch_size = 50
        
        for sheet_name, df in sheets.items():
            df = clean_dataframe(df)
            if df.empty:
                continue
            
            logger.info(f"Processing sheet: {sheet_name}, rows: {len(df)}")
            
            for idx, row in df.iterrows():
                hospital_name = None
                birth_type = None
                total = None
                percentage = None
                
                for col in df.columns:
                    val = row[col]
                    if pd.notna(val):
                        val_str = str(val).strip()
                        # Check if it's hospital name
                        if "hospital" in col.lower() or "name" in col.lower():
                            hospital_name = val_str
                        # Check if it's birth type
                        elif any(x in val_str.lower() for x in ["spontaneous", "induced", "caesarean"]):
                            birth_type = val_str.lower()
                        # Check if it's a number
                        try:
                            num_val = float(val_str.replace("%", "").replace(",", ""))
                            if "total" in col.lower() or "number" in col.lower():
                                total = int(num_val)
                            elif "%" in str(row[col]) or "percentage" in col.lower():
                                percentage = num_val
                        except (ValueError, AttributeError):
                            pass
                
                if hospital_name:
                    # Get or create hospital
                    result = await session.execute(
                        select(Hospital).where(Hospital.hospital_name == hospital_name)
                    )
                    hospital = result.scalar_one_or_none()
                    if not hospital:
                        hospital = Hospital(hospital_name=hospital_name, year=2023)
                        session.add(hospital)
                        await session.flush()
                    
                    # Add hospital stat
                    if birth_type or total or percentage:
                        stat = HospitalStat(
                            hospital_id=hospital.id,
                            metric_name="birth_type",
                            metric_category=birth_type,
                            total_cases=total,
                            percentage=percentage,
                            year=2023,
                        )
                        batch.append(stat)
                        
                        if len(batch) >= batch_size:
                            session.add_all(batch)
                            await session.commit()
                            logger.info(f"Committed batch of {len(batch)} records")
                            batch = []
        
        if batch:
            session.add_all(batch)
            await session.commit()
            logger.info(f"Committed final batch of {len(batch)} records")
        
        logger.info("Hospital birth type data imported successfully")


async def clear_mother_table(session: AsyncSession):
    """Clear all data from mother table before importing new data."""
    logger.info("Clearing all existing data from mother table...")
    result = await session.execute(select(Mother))
    all_mothers = result.scalars().all()
    for mother in all_mothers:
        await session.delete(mother)
    await session.commit()
    logger.info(f"Cleared {len(all_mothers)} records from mother table")


async def main():
    """Main import function."""
    data_dir = Path(__file__).parent.parent / "data"
    
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return
    
    # Initialize database
    logger.info("Initializing database...")
    await init_db()
    logger.info("Database initialized")
    
    async with async_session() as session:
        # Clear old data first
        await clear_mother_table(session)
        
        # Import data - only diabetes and hypertension from CSV
        logger.info("Starting data import...")
        await import_diabetes_hypertension_csv(session, data_dir)
    
    logger.info("=" * 50)
    logger.info("Data import completed successfully!")
    logger.info("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())


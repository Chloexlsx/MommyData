"""Scenario controllers for guided questions flow."""
from typing import Optional, Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select, func, and_, or_

from app.models import (
    Mother,
    AntenatalCare,
    Birth,
    Baby,
    Complication,
)


async def get_preparing_scenario_data(
    db: AsyncSession,
    age_group: Optional[str] = None,
    smoking: Optional[str] = None,
    bmi: Optional[str] = None,
    diabetes: Optional[bool] = None,
    hypertension: Optional[bool] = None,
    lhd: Optional[str] = None,
) -> Dict[str, Any]:
    """Get data for preparing pregnancy scenario."""
    results = {}
    
    # Build filters
    filters = []
    if age_group:
        filters.append(Mother.age_group == age_group)
    if smoking:
        filters.append(Mother.smoking_status == smoking)
    if bmi:
        filters.append(Mother.bmi_category == bmi)
    if diabetes is not None:
        filters.append(Mother.diabetes_pre == diabetes)
    if hypertension is not None:
        filters.append(Mother.hypertension_pre == hypertension)
    if lhd:
        filters.append(Mother.lhd == lhd)
    
    # Get matching mothers statistics
    query = select(
        func.count(Mother.id).label("total"),
        func.avg(Mother.percentage).label("avg_percentage"),
    ).where(Mother.year == 2023)
    if filters:
        query = query.where(and_(*filters))
    
    result = await db.execute(query)
    stats = result.first()
    results["mothers"] = {
        "total": stats.total if stats else 0,
        "avg_percentage": float(stats.avg_percentage) if stats and stats.avg_percentage else 0.0,
    }
    
    # Get complication risks
    comp_query = select(
        Complication.complication_type,
        func.count(Complication.id).label("count"),
        func.avg(Complication.percentage).label("avg_percentage"),
    ).where(
        Complication.year == 2023
    ).group_by(Complication.complication_type)
    
    comp_result = await db.execute(comp_query)
    complications = []
    for row in comp_result:
        complications.append({
            "type": row.complication_type,
            "count": row.count,
            "percentage": float(row.avg_percentage) if row.avg_percentage else 0.0,
        })
    results["complications"] = complications
    
    # Get overall averages for comparison
    overall_query = select(
        func.count(Mother.id).label("total"),
        func.avg(Mother.percentage).label("avg_percentage"),
    ).where(Mother.year == 2023)
    
    overall_result = await db.execute(overall_query)
    overall_stats = overall_result.first()
    results["overall_average"] = {
        "total": overall_stats.total if overall_stats else 0,
        "avg_percentage": float(overall_stats.avg_percentage) if overall_stats and overall_stats.avg_percentage else 0.0,
    }
    
    return results


async def get_pregnant_scenario_data(
    db: AsyncSession,
    age_group: Optional[str] = None,
    antenatal_week: Optional[str] = None,
    current_week: Optional[int] = None,
    lhd: Optional[str] = None,
) -> Dict[str, Any]:
    """Get data for pregnant scenario."""
    results = {}
    
    # Build filters for antenatal care
    antenatal_filters = []
    if age_group:
        # Need to join with mothers table for age
        pass
    if antenatal_week:
        antenatal_filters.append(AntenatalCare.first_visit_category == antenatal_week)
    if lhd:
        antenatal_filters.append(AntenatalCare.lhd == lhd)
    
    # Get birth outcomes
    birth_filters = []
    if age_group:
        # Join with mothers
        pass
    if lhd:
        birth_filters.append(Birth.lhd == lhd)
    
    # Get induction/C-section rates
    induction_query = select(
        Birth.onset_labour,
        func.count(Birth.id).label("count"),
        func.avg(Birth.percentage).label("avg_percentage"),
    ).where(
        and_(
            Birth.year == 2023,
            *birth_filters
        )
    ).group_by(Birth.onset_labour)
    
    induction_result = await db.execute(induction_query)
    results["labour_onset"] = []
    for row in induction_result:
        results["labour_onset"].append({
            "type": row.onset_labour,
            "count": row.count,
            "percentage": float(row.avg_percentage) if row.avg_percentage else 0.0,
        })
    
    # Get birth type rates
    birth_type_query = select(
        Birth.birth_type,
        func.count(Birth.id).label("count"),
        func.avg(Birth.percentage).label("avg_percentage"),
    ).where(
        and_(
            Birth.year == 2023,
            *birth_filters
        )
    ).group_by(Birth.birth_type)
    
    birth_type_result = await db.execute(birth_type_query)
    results["birth_types"] = []
    for row in birth_type_result:
        results["birth_types"].append({
            "type": row.birth_type,
            "count": row.count,
            "percentage": float(row.avg_percentage) if row.avg_percentage else 0.0,
        })
    
    # Get preterm rates
    preterm_query = select(
        func.count(Birth.id).label("preterm_count"),
        func.avg(Birth.percentage).label("preterm_percentage"),
    ).where(
        and_(
            Birth.year == 2023,
            Birth.gestational_age_category == "preterm",
            *birth_filters
        )
    )
    
    preterm_result = await db.execute(preterm_query)
    preterm_stats = preterm_result.first()
    results["preterm"] = {
        "count": preterm_stats.preterm_count if preterm_stats else 0,
        "percentage": float(preterm_stats.preterm_percentage) if preterm_stats and preterm_stats.preterm_percentage else 0.0,
    }
    
    # Get low birth weight
    low_bw_query = select(
        func.count(Baby.id).label("low_bw_count"),
        func.avg(Baby.percentage).label("low_bw_percentage"),
    ).where(
        and_(
            Baby.year == 2023,
            Baby.birth_weight_category == "low",
        )
    )
    
    low_bw_result = await db.execute(low_bw_query)
    low_bw_stats = low_bw_result.first()
    results["low_birth_weight"] = {
        "count": low_bw_stats.low_bw_count if low_bw_stats else 0,
        "percentage": float(low_bw_stats.low_bw_percentage) if low_bw_stats and low_bw_stats.low_bw_percentage else 0.0,
    }
    
    # Get NICU rates
    nicu_query = select(
        func.count(Baby.id).label("nicu_count"),
        func.avg(Baby.percentage).label("nicu_percentage"),
    ).where(
        and_(
            Baby.year == 2023,
            or_(
                Baby.nicu_admission == True,
                Baby.scunicu_admission == True
            )
        )
    )
    
    nicu_result = await db.execute(nicu_query)
    nicu_stats = nicu_result.first()
    results["nicu"] = {
        "count": nicu_stats.nicu_count if nicu_stats else 0,
        "percentage": float(nicu_stats.nicu_percentage) if nicu_stats and nicu_stats.nicu_percentage else 0.0,
    }
    
    return results


async def get_factor_data(
    db: AsyncSession,
    factor_name: str,
    age_group: Optional[str] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    sub_group: Optional[list[str]] = None,
) -> Dict[str, Any]:
    """Get data for a specific factor with trends by age group over years."""
    results = {
        "factor": factor_name,
        "user_age_group": age_group,
        "years": [],
        "age_groups": {},
    }
    
    # Determine which field to query
    if factor_name == "diabetes":
        factor_field = Mother.diabetes_pre
        subgroup_field = Mother.diabetes_subgroup
    elif factor_name == "hypertension":
        factor_field = Mother.hypertension_pre
        subgroup_field = Mother.hypertension_subgroup
    else:
        return results
    
    # Build query conditions
    conditions = [
        factor_field.isnot(None),
        Mother.age_group.isnot(None),
    ]
    
    # Filter by sub_group(s) if provided
    if sub_group and subgroup_field and len(sub_group) > 0:
        if len(sub_group) == 1:
            conditions.append(subgroup_field == sub_group[0])
        else:
            # Multiple sub_groups - use IN clause
            conditions.append(subgroup_field.in_(sub_group))
    
    # Note: start_year is the latest year, end_year is the earliest year
    # We want years between end_year and start_year (inclusive)
    if start_year and end_year:
        conditions.append(Mother.year >= end_year)
        conditions.append(Mother.year <= start_year)
    elif start_year:
        conditions.append(Mother.year <= start_year)
    elif end_year:
        conditions.append(Mother.year >= end_year)
    
    # Get data grouped by year, age group, and sub_group
    # We need to include sub_group in the select and group_by to differentiate between different types
    query = select(
        Mother.year,
        Mother.age_group,
        subgroup_field.label("sub_group"),
        func.avg(Mother.percentage).label("avg_percentage"),
    ).where(
        and_(
            *conditions,
        )
    ).group_by(
        Mother.year,
        Mother.age_group,
        subgroup_field,
    ).order_by(
        Mother.year,
        Mother.age_group,
        subgroup_field,
    )
    
    result = await db.execute(query)
    
    # Organize data by age group and sub_group
    # Structure: {age_group: {sub_group: {year: percentage}}}
    age_groups_data = {}
    years_set = set()
    
    for row in result:
        if not row.age_group or row.age_group.lower() in ['total', 'not stated']:
            continue
        
        year = int(row.year)
        age_group_key = str(row.age_group)
        sub_group_value = str(row.sub_group) if row.sub_group else 'Unknown'
        percentage = float(row.avg_percentage) if row.avg_percentage else 0.0
        
        years_set.add(year)
        
        # Organize by age_group -> sub_group -> year
        if age_group_key not in age_groups_data:
            age_groups_data[age_group_key] = {}
        
        if sub_group_value not in age_groups_data[age_group_key]:
            age_groups_data[age_group_key][sub_group_value] = {}
        
        age_groups_data[age_group_key][sub_group_value][year] = percentage
    
    # Sort years
    results["years"] = sorted(list(years_set))
    results["age_groups"] = age_groups_data
    
    return results


async def get_factor_data_simple(
    db: AsyncSession,
    factor_name: str,
    age_group: Optional[str] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> Dict[str, Any]:
    """Get simplified data for a factor (diabetes/hypertension) without sub-group disaggregation.
    Only shows Yes/No (True/False) distribution by age group over years."""
    results = {
        "factor": factor_name,
        "user_age_group": age_group,
        "years": [],
        "age_groups": {},
    }
    
    # Determine which field to query
    if factor_name == "diabetes":
        factor_field = Mother.diabetes_pre
    elif factor_name == "hypertension":
        factor_field = Mother.hypertension_pre
    else:
        return results
    
    # Build query conditions - exclude "Total" subgroup records
    conditions = [
        factor_field.isnot(None),
        Mother.age_group.isnot(None),
    ]
    
    # Exclude "Total" subgroup records
    if factor_name == "diabetes":
        conditions.append(
            or_(
                Mother.diabetes_subgroup.is_(None),
                Mother.diabetes_subgroup != "Total"
            )
        )
    elif factor_name == "hypertension":
        conditions.append(
            or_(
                Mother.hypertension_subgroup.is_(None),
                Mother.hypertension_subgroup != "Total"
            )
        )
    
    if start_year and end_year:
        conditions.append(Mother.year >= end_year)
        conditions.append(Mother.year <= start_year)
    elif start_year:
        conditions.append(Mother.year <= start_year)
    elif end_year:
        conditions.append(Mother.year >= end_year)
    
    # Query: group by year, age_group, and factor (True/False)
    # Sum up total_mothers for each group to get the count
    query = select(
        Mother.year,
        Mother.age_group,
        factor_field.label("has_factor"),
        func.sum(Mother.total_mothers).label("total_mothers"),
    ).where(
        and_(*conditions)
    ).group_by(
        Mother.year,
        Mother.age_group,
        factor_field,
    ).order_by(
        Mother.year,
        Mother.age_group,
        factor_field,
    )
    
    result = await db.execute(query)
    
    # Organize data by age group and factor status (True/False)
    # Structure: {age_group: {Yes/No: {year: percentage}}}
    age_groups_data = {}
    years_set = set()
    
    # First pass: collect all data
    temp_data = {}  # {(age_group, year): {True: count, False: count}}
    
    for row in result:
        if not row.age_group or row.age_group.lower() in ['total', 'not stated']:
            continue
        
        year = int(row.year)
        age_group_key = str(row.age_group)
        has_factor = bool(row.has_factor)
        count = int(row.total_mothers) if row.total_mothers else 0
        
        years_set.add(year)
        
        key = (age_group_key, year)
        if key not in temp_data:
            temp_data[key] = {True: 0, False: 0}
        
        temp_data[key][has_factor] = count
    
    # Second pass: calculate percentages
    for (age_group_key, year), counts in temp_data.items():
        total = counts[True] + counts[False]
        if total == 0:
            continue
        
        if age_group_key not in age_groups_data:
            age_groups_data[age_group_key] = {}
        
        for has_factor, label in [(True, "Yes"), (False, "No")]:
            if label not in age_groups_data[age_group_key]:
                age_groups_data[age_group_key][label] = {}
            
            percentage = (counts[has_factor] / total * 100) if total > 0 else 0.0
            age_groups_data[age_group_key][label][year] = percentage
    
    results["years"] = sorted(list(years_set))
    results["age_groups"] = age_groups_data
    
    return results



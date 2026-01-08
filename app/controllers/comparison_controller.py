"""Comparison controller for personalized data comparison."""
from typing import Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select, func, and_

from app.models import Mother, Birth, Baby, AntenatalCare


async def get_comparison_data(
    db: AsyncSession,
    scenario: str,
    filters: Dict[str, Any],
) -> Dict[str, Any]:
    """Get comparison data for user's conditions vs overall average."""
    results = {
        "user_conditions": filters,
        "user_stats": {},
        "overall_average": {},
    }
    
    if scenario == "preparing":
        # Get user's stats
        user_filters = []
        if filters.get("age_group"):
            user_filters.append(Mother.age_group == filters["age_group"])
        if filters.get("smoking"):
            user_filters.append(Mother.smoking_status == filters["smoking"])
        if filters.get("bmi"):
            user_filters.append(Mother.bmi_category == filters["bmi"])
        if filters.get("diabetes") is not None:
            user_filters.append(Mother.diabetes_pre == filters["diabetes"])
        if filters.get("hypertension") is not None:
            user_filters.append(Mother.hypertension_pre == filters["hypertension"])
        if filters.get("lhd"):
            user_filters.append(Mother.lhd == filters["lhd"])
        
        if user_filters:
            user_query = select(
                func.count(Mother.id).label("total"),
                func.avg(Mother.percentage).label("avg_percentage"),
            ).where(and_(*user_filters))
            
            user_result = await db.execute(user_query)
            user_stats = user_result.first()
            results["user_stats"] = {
                "total": user_stats.total if user_stats else 0,
                "avg_percentage": float(user_stats.avg_percentage) if user_stats and user_stats.avg_percentage else 0.0,
            }
        
        # Get overall average
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
    
    elif scenario == "pregnant":
        # Similar logic for pregnant scenario
        pass
    
    return results


"""API routes."""
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.controllers import scenario_controller, comparison_controller

router = APIRouter(prefix="/api/v1", tags=["api"])


@router.get("/scenario/preparing")
async def get_preparing_scenario(
    age: Optional[str] = Query(None, alias="age_group"),
    smoking: Optional[str] = Query(None),
    bmi: Optional[str] = Query(None),
    diabetes: Optional[bool] = Query(None),
    hypertension: Optional[bool] = Query(None),
    lhd: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Get data for preparing pregnancy scenario."""
    return await scenario_controller.get_preparing_scenario_data(
        db=db,
        age_group=age,
        smoking=smoking,
        bmi=bmi,
        diabetes=diabetes,
        hypertension=hypertension,
        lhd=lhd,
    )


@router.get("/scenario/pregnant")
async def get_pregnant_scenario(
    age: Optional[str] = Query(None, alias="age_group"),
    antenatal_week: Optional[str] = Query(None),
    current_week: Optional[int] = Query(None),
    lhd: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Get data for pregnant scenario."""
    return await scenario_controller.get_pregnant_scenario_data(
        db=db,
        age_group=age,
        antenatal_week=antenatal_week,
        current_week=current_week,
        lhd=lhd,
    )


@router.get("/factor/{factor_name}")
async def get_factor_data(
    factor_name: str,
    age_group: Optional[str] = Query(None),
    start_year: Optional[int] = Query(None),
    end_year: Optional[int] = Query(None),
    sub_group: Optional[list[str]] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Get data for a specific factor (diabetes, hypertension) with trends by age group over years."""
    return await scenario_controller.get_factor_data(
        db=db,
        factor_name=factor_name,
        age_group=age_group,
        start_year=start_year,
        end_year=end_year,
        sub_group=sub_group,
    )


@router.get("/factor/{factor_name}/simple")
async def get_factor_data_simple(
    factor_name: str,
    age_group: Optional[str] = Query(None),
    start_year: Optional[int] = Query(None),
    end_year: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Get simplified data for a factor (diabetes/hypertension) without sub-group disaggregation.
    Only shows Yes/No distribution by age group over years."""
    return await scenario_controller.get_factor_data_simple(
        db=db,
        factor_name=factor_name,
        age_group=age_group,
        start_year=start_year,
        end_year=end_year,
    )


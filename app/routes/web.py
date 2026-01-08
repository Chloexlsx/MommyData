"""Web routes."""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from starlette.templating import Jinja2Templates

templates = Jinja2Templates(directory="app/templates")

router = APIRouter(tags=["web"])


@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home page with three scenario options."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/scenario/preparing", response_class=HTMLResponse)
async def scenario_preparing(request: Request):
    """Preparing pregnancy scenario page."""
    return templates.TemplateResponse("scenario_preparing.html", {"request": request})


@router.get("/scenario/pregnant", response_class=HTMLResponse)
async def scenario_pregnant(request: Request):
    """Pregnant scenario page."""
    return templates.TemplateResponse("scenario_pregnant.html", {"request": request})


@router.get("/detail/{data_type}/{id}", response_class=HTMLResponse)
async def detail(request: Request, data_type: str, id: int):
    """Detail page for specific data."""
    return templates.TemplateResponse("detail.html", {
        "request": request,
        "data_type": data_type,
        "id": id,
    })


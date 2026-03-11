from fastapi import APIRouter

from app.api.routes.forecast import router as forecast_router

api_router = APIRouter()
api_router.include_router(forecast_router)

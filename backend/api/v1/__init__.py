"""API v1版本"""

from fastapi import APIRouter
from backend.api.v1.endpoints import movies, health, admin

api_router = APIRouter()

api_router.include_router(health.router, tags=["health"])
api_router.include_router(movies.router, prefix="/movies", tags=["movies"])
api_router.include_router(admin.router, prefix="/admin", tags=["admin"])


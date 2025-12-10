"""健康检查端点"""

from fastapi import APIRouter
from backend.models.schemas import HealthResponse
from backend.db.hbase import hbase_connection

router = APIRouter()


@router.get("/", response_model=HealthResponse)
async def health_check():
    """健康检查接口"""
    try:
        hbase_connection.connect()
        return HealthResponse(
            status="ok",
            hbase_connected=True,
            version="1.0.0"
        )
    except Exception:
        return HealthResponse(
            status="error",
            hbase_connected=False,
            version="1.0.0"
        )


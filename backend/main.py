"""FastAPI应用主入口"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.core.config import settings
from backend.core.logging import logger
from backend.db.hbase import hbase_connection
from backend.api.v1 import api_router


def create_app() -> FastAPI:
    """创建FastAPI应用实例
    
    Returns:
        FastAPI: 配置好的应用实例
    """
    app = FastAPI(
        title="MovieLens API",
        description="电影搜索和查询API - 专业架构版",
        version="1.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc"
    )
    
    # 配置CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # 注册路由
    app.include_router(api_router, prefix="/api")
    
    # 生命周期事件
    @app.on_event("startup")
    async def startup_event():
        """应用启动事件"""
        try:
            hbase_connection.connect()
            logger.info("应用启动成功")
        except Exception as e:
            logger.error(f"应用启动失败: {e}")
    
    @app.on_event("shutdown")
    async def shutdown_event():
        """应用关闭事件"""
        hbase_connection.close()
        logger.info("应用已关闭")
    
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host=settings.server_host,
        port=settings.server_port,
        reload=settings.debug
    )

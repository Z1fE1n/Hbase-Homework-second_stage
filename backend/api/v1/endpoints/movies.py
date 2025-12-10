"""电影相关端点"""

from fastapi import APIRouter, HTTPException, Query
from backend.services.movie_service import MovieService
from backend.models.schemas import MovieListResponse, MovieSchema, SearchResponse
from backend.core.logging import logger

router = APIRouter()
movie_service = MovieService()


@router.get("/featured")
async def get_featured_movies(
    count: int = Query(8, ge=1, le=20, description="推荐数量")
):
    """获取固定推荐电影（ID 1-x）"""
    try:
        movies = movie_service.get_featured_movies(count)
        return {
            "movies": [MovieSchema.model_validate(m.__dict__) for m in movies],
            "total": len(movies)
        }
    except Exception as e:
        logger.error(f"获取推荐电影失败: {e}")
        raise HTTPException(status_code=500, detail="获取推荐电影失败")


@router.get("", response_model=MovieListResponse)
async def list_movies(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量")
):
    """获取电影列表（分页）"""
    try:
        movies, total, total_pages = movie_service.get_movies_list(page, page_size)
        
        return MovieListResponse(
            movies=[MovieSchema.model_validate(m.__dict__) for m in movies],
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
    except Exception as e:
        logger.error(f"获取电影列表失败: {e}")
        raise HTTPException(status_code=500, detail="获取电影列表失败")


@router.get("/search", response_model=SearchResponse)
async def search_movies(
    q: str = Query(..., min_length=1, description="搜索关键词"),
    limit: int = Query(50, ge=1, le=100, description="返回结果数量")
):
    """搜索电影"""
    try:
        movies = movie_service.search_movies(q, limit)
        
        return SearchResponse(
            movies=[MovieSchema.model_validate(m.__dict__) for m in movies],
            query=q,
            total=len(movies)
        )
    except Exception as e:
        logger.error(f"搜索电影失败: {e}")
        raise HTTPException(status_code=500, detail="搜索失败")


@router.get("/{movie_id}")
async def get_movie(movie_id: str):
    """获取电影详情"""
    try:
        movie = movie_service.get_movie_basic_info(movie_id)
        if not movie:
            raise HTTPException(status_code=404, detail="电影不存在")
        
        return {
            "id": movie.id,
            "title": movie.title,
            "genres": movie.genres,
            "avg_rating": movie.avg_rating,
            "rating_count": movie.rating_count
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取电影详情失败: {e}")
        raise HTTPException(status_code=500, detail="获取电影详情失败")

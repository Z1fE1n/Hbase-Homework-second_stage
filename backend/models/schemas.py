"""API请求响应模型"""

from pydantic import BaseModel, Field
from typing import List, Dict


class MovieSchema(BaseModel):
    """电影响应模型"""
    id: str = Field(..., description="电影ID")
    title: str = Field(..., description="电影标题")
    genres: str = Field(..., description="电影类型")
    avg_rating: float = Field(0.0, description="平均评分")
    rating_count: int = Field(0, description="评分数量")
    
    class Config:
        from_attributes = True


class RatingSchema(BaseModel):
    """评分响应模型"""
    user_id: str = Field(..., description="用户ID")
    movie_id: str = Field(..., description="电影ID")
    rating: float = Field(..., description="评分")
    timestamp: str = Field(..., description="时间戳")
    
    class Config:
        from_attributes = True


class RatingStatsSchema(BaseModel):
    """评分统计模型"""
    avg_rating: float = Field(..., description="平均评分")
    total_count: int = Field(..., description="总评分数")
    rating_distribution: Dict[str, int] = Field(..., description="评分分布")


class MovieDetailSchema(MovieSchema):
    """电影详情响应模型"""
    recent_ratings: List[RatingSchema] = Field(default_factory=list, description="最近的评分")
    rating_stats: RatingStatsSchema = Field(None, description="评分统计")


class RatingListResponse(BaseModel):
    """评分列表响应"""
    ratings: List[RatingSchema]
    total: int
    page: int
    page_size: int
    total_pages: int


class MovieListResponse(BaseModel):
    """电影列表响应"""
    movies: List[MovieSchema]
    total: int
    page: int
    page_size: int
    total_pages: int


class SearchResponse(BaseModel):
    """搜索响应"""
    movies: List[MovieSchema]
    query: str
    total: int


class HealthResponse(BaseModel):
    """健康检查响应"""
    status: str
    hbase_connected: bool
    version: str = "1.0.0"

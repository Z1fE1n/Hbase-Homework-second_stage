"""领域模型定义"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Movie:
    """电影领域模型"""
    id: str
    title: str
    genres: str
    avg_rating: float
    rating_count: int


@dataclass
class Rating:
    """评分领域模型"""
    user_id: str
    movie_id: str
    rating: float
    timestamp: str


@dataclass
class MovieDetail:
    """电影详情领域模型"""
    id: str
    title: str
    genres: str
    avg_rating: float
    rating_count: int
    recent_ratings: List[Rating]


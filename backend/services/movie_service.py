"""电影业务逻辑服务"""

import json
from pathlib import Path
from typing import List, Optional, Tuple
from backend.db.repositories.movie_repository import MovieRepository
from backend.db.repositories.rating_repository import RatingRepository
from backend.models.domain import Movie, Rating, MovieDetail
from backend.core.config import settings
from backend.core.logging import logger


class MovieIndexService:
    """电影索引服务 - 使用 JSON 索引文件进行快速搜索"""
    
    _instance = None
    _movies_index: List[dict] = []
    _movies_by_id: dict = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_index()
        return cls._instance
    
    def _load_index(self):
        """加载电影索引"""
        index_path = Path("backend/data/movie_index.json")
        if not index_path.exists():
            logger.warning(f"电影索引文件不存在: {index_path}")
            return
        
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                self._movies_index = json.load(f)
            
            # 构建 ID 映射
            self._movies_by_id = {m['id']: m for m in self._movies_index}
            logger.info(f"已加载电影索引: {len(self._movies_index)} 部电影")
        except Exception as e:
            logger.error(f"加载电影索引失败: {e}")
    
    def get_featured_movies(self, count: int = 8) -> List[dict]:
        """获取固定推荐电影（ID 1-x）"""
        featured = []
        for i in range(1, count + 1):
            movie_id = str(i)
            if movie_id in self._movies_by_id:
                featured.append(self._movies_by_id[movie_id])
        return featured
    
    def search(self, query: str, limit: int = 50) -> List[dict]:
        """搜索电影（使用索引）"""
        if not query or not query.strip():
            return []
        
        query_lower = query.lower().strip()
        matched = []
        
        for movie in self._movies_index:
            title_lower = movie['title'].lower()
            genres_lower = movie['genres'].lower()
            
            if query_lower in title_lower or query_lower in genres_lower:
                matched.append(movie)
                if len(matched) >= limit:
                    break
        
        # 按相关度排序（标题匹配优先，然后按评分）
        def sort_key(m):
            title_match = query_lower in m['title'].lower()
            return (not title_match, -m['avg_rating'], -m['rating_count'])
        
        matched.sort(key=sort_key)
        return matched[:limit]
    
    def reload_index(self):
        """重新加载索引"""
        self._load_index()


class MovieService:
    """电影业务服务"""
    
    def __init__(self):
        self.movie_repo = MovieRepository()
        self.rating_repo = RatingRepository()
        self.index_service = MovieIndexService()
    
    def get_movies_list(self, page: int = 1, page_size: int = 20) -> tuple:
        """获取电影列表（分页）
        
        Args:
            page: 页码
            page_size: 每页数量
            
        Returns:
            tuple: (电影列表, 总数, 总页数)
        """
        try:
            # 获取所有电影
            all_movies_data = self.movie_repo.find_all()
            
            # 转换为领域模型
            all_movies = [
                Movie(
                    id=m['id'],
                    title=m['title'],
                    genres=m['genres'],
                    avg_rating=float(m['avg_rating']),
                    rating_count=int(m['rating_count'])
                )
                for m in all_movies_data
            ]
            
            # 按评分排序
            all_movies.sort(key=lambda x: (x.avg_rating, x.rating_count), reverse=True)
            
            # 分页处理
            total = len(all_movies)
            total_pages = (total + page_size - 1) // page_size
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            movies = all_movies[start_idx:end_idx]
            
            return movies, total, total_pages
        except Exception as e:
            logger.error(f"获取电影列表失败: {e}")
            raise
    
    def get_movie_basic_info(self, movie_id: str) -> Optional[Movie]:
        """根据ID获取电影基本信息（不获取评分列表）
        
        Args:
            movie_id: 电影ID
            
        Returns:
            Optional[Movie]: 电影基本信息，不存在返回None
        """
        try:
            movie_data = self.movie_repo.find_by_id(movie_id)
            if not movie_data:
                return None
            
            return Movie(
                id=movie_data['id'],
                title=movie_data['title'],
                genres=movie_data['genres'],
                avg_rating=float(movie_data['avg_rating']),
                rating_count=int(movie_data['rating_count'])
            )
        except Exception as e:
            logger.error(f"获取电影基本信息失败 movie_id={movie_id}: {e}")
            raise
    
    def get_movie_by_id(self, movie_id: str) -> Optional[MovieDetail]:
        """根据ID获取电影详情（包含评分列表，已弃用）
        
        Args:
            movie_id: 电影ID
            
        Returns:
            Optional[MovieDetail]: 电影详情，不存在返回None
        """
        try:
            # 获取电影基本信息
            movie_data = self.movie_repo.find_by_id(movie_id)
            if not movie_data:
                return None
            
            # 获取最近评分（前10条）
            # 设置较小的 max_scan_rows 限制扫描范围，提高响应速度
            ratings_data = self.rating_repo.find_by_movie_id(
                movie_id, 
                limit=10, 
                max_scan_rows=20000  # 最多扫描2万行，快速返回
            )
            ratings = [
                Rating(
                    user_id=r['user_id'],
                    movie_id=r['movie_id'],
                    rating=float(r['rating']),
                    timestamp=r['timestamp']
                )
                for r in ratings_data
            ]
            
            # 构建详情对象
            return MovieDetail(
                id=movie_data['id'],
                title=movie_data['title'],
                genres=movie_data['genres'],
                avg_rating=float(movie_data['avg_rating']),
                rating_count=int(movie_data['rating_count']),
                recent_ratings=ratings
            )
        except Exception as e:
            logger.error(f"获取电影详情失败 movie_id={movie_id}: {e}")
            raise
    
    def get_movie_ratings(self, movie_id: str, page: int = 1, page_size: int = 20) -> Tuple[List[Rating], int, int]:
        """获取电影的所有评分（分页）
        
        Args:
            movie_id: 电影ID
            page: 页码
            page_size: 每页数量
            
        Returns:
            tuple: (评分列表, 总数, 总页数)
        """
        try:
            # 限制最大返回数量，防止性能问题
            # 由于 rowkey 设计 (user_id_movie_id) 不支持按 movie_id 高效查询
            # 这里设置一个合理的上限
            max_ratings = 500
            all_ratings_data = self.rating_repo.find_by_movie_id(
                movie_id, 
                limit=max_ratings,
                max_scan_rows=100000  # 最多扫描10万行
            )
            
            # 转换为领域模型
            all_ratings = [
                Rating(
                    user_id=r['user_id'],
                    movie_id=r['movie_id'],
                    rating=float(r['rating']),
                    timestamp=r['timestamp']
                )
                for r in all_ratings_data
            ]
            
            # 按时间戳倒序排序（最新的在前）
            all_ratings.sort(key=lambda x: x.timestamp, reverse=True)
            
            # 分页处理
            total = len(all_ratings)
            total_pages = (total + page_size - 1) // page_size if total > 0 else 0
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            ratings = all_ratings[start_idx:end_idx]
            
            return ratings, total, total_pages
        except Exception as e:
            logger.error(f"获取电影评分列表失败 movie_id={movie_id}: {e}")
            raise
    
    def get_rating_stats(self, movie_id: str) -> dict:
        """获取电影评分统计
        
        Args:
            movie_id: 电影ID
            
        Returns:
            dict: 评分统计信息
        """
        try:
            return self.rating_repo.get_rating_stats(movie_id)
        except Exception as e:
            logger.error(f"获取评分统计失败 movie_id={movie_id}: {e}")
            raise
    
    def get_featured_movies(self, count: int = 8) -> List[Movie]:
        """获取固定推荐电影（ID 1-x）
        
        Args:
            count: 返回数量
            
        Returns:
            List[Movie]: 推荐电影列表
        """
        try:
            featured_data = self.index_service.get_featured_movies(count)
            return [
                Movie(
                    id=m['id'],
                    title=m['title'],
                    genres=m['genres'],
                    avg_rating=float(m['avg_rating']),
                    rating_count=int(m['rating_count'])
                )
                for m in featured_data
            ]
        except Exception as e:
            logger.error(f"获取推荐电影失败: {e}")
            raise
    
    def search_movies(self, query: str, limit: int = 50) -> List[Movie]:
        """搜索电影（使用 JSON 索引，不扫描 HBase）
        
        Args:
            query: 搜索关键词
            limit: 返回数量限制
            
        Returns:
            List[Movie]: 匹配的电影列表
        """
        try:
            query = query.strip()
            if not query:
                return []
            
            # 使用索引搜索
            matched_data = self.index_service.search(query, limit)
            return [
                Movie(
                    id=m['id'],
                    title=m['title'],
                    genres=m['genres'],
                    avg_rating=float(m['avg_rating']),
                    rating_count=int(m['rating_count'])
                )
                for m in matched_data
            ]
        except Exception as e:
            logger.error(f"搜索电影失败 query={query}: {e}")
            raise

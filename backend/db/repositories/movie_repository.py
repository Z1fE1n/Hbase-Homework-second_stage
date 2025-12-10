"""电影数据仓库"""

from typing import List, Optional, Tuple
from functools import wraps
from backend.db.hbase import hbase_connection
from backend.core.config import settings
from backend.core.logging import logger


def retry_on_connection_error(max_retries=2):
    """连接错误时自动重试的装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    # 每次重试前刷新表连接
                    if hasattr(self, '_refresh_table'):
                        self._refresh_table()
                    return func(self, *args, **kwargs)
                except Exception as e:
                    last_error = e
                    error_msg = str(e).lower()
                    # 判断是否是连接错误
                    if 'connection' in error_msg or 'broken pipe' in error_msg or '10053' in error_msg:
                        logger.warning(f"连接错误，尝试重连 (attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            continue
                    # 非连接错误直接抛出
                    raise
            raise last_error
        return wrapper
    return decorator


class MovieRepository:
    """电影数据访问对象"""
    
    def __init__(self):
        self.table = None
        self._refresh_table()
    
    def _refresh_table(self):
        """刷新表连接"""
        self.table = hbase_connection.get_movies_table()
    
    @retry_on_connection_error(max_retries=2)
    def find_by_id(self, movie_id: str) -> Optional[dict]:
        """根据ID查找电影
        
        Args:
            movie_id: 电影ID
            
        Returns:
            Optional[dict]: 电影数据字典，不存在返回None
        """
        try:
            row = self.table.row(movie_id.encode('utf-8'))
            if not row:
                return None
            
            return {
                'id': movie_id,
                'title': row.get(b'info:title', b'').decode('utf-8'),
                'genres': row.get(b'info:genres', b'').decode('utf-8'),
                'avg_rating': row.get(b'info:avg_rating', b'0').decode('utf-8'),
                'rating_count': row.get(b'info:rating_count', b'0').decode('utf-8')
            }
        except Exception as e:
            logger.error(f"查询电影失败 ID={movie_id}: {e}")
            raise
    
    @retry_on_connection_error(max_retries=2)
    def find_all(self, limit: Optional[int] = None) -> List[dict]:
        """查找所有电影
        
        Args:
            limit: 限制返回数量
            
        Returns:
            List[dict]: 电影列表
        """
        movies = []
        try:
            scan_kwargs = {'limit': limit} if limit else {}
            for key, data in self.table.scan(**scan_kwargs):
                movies.append({
                    'id': key.decode('utf-8'),
                    'title': data.get(b'info:title', b'').decode('utf-8'),
                    'genres': data.get(b'info:genres', b'').decode('utf-8'),
                    'avg_rating': data.get(b'info:avg_rating', b'0').decode('utf-8'),
                    'rating_count': data.get(b'info:rating_count', b'0').decode('utf-8')
                })
            return movies
        except Exception as e:
            logger.error(f"查询电影列表失败: {e}")
            raise
    
    @retry_on_connection_error(max_retries=2)
    def search_by_text(self, query: str, limit: int = 100) -> List[dict]:
        """文本搜索电影
        
        Args:
            query: 搜索关键词
            limit: 返回结果限制
            
        Returns:
            List[dict]: 匹配的电影列表
        """
        query_lower = query.lower().strip()
        matched_movies = []
        scan_count = 0
        max_scan = settings.max_scan_rows
        
        try:
            for key, data in self.table.scan():
                if scan_count >= max_scan:
                    break
                scan_count += 1
                
                title = data.get(b'info:title', b'').decode('utf-8').lower()
                genres = data.get(b'info:genres', b'').decode('utf-8').lower()
                
                if query_lower in title or query_lower in genres:
                    matched_movies.append({
                        'id': key.decode('utf-8'),
                        'title': data.get(b'info:title', b'').decode('utf-8'),
                        'genres': data.get(b'info:genres', b'').decode('utf-8'),
                        'avg_rating': data.get(b'info:avg_rating', b'0').decode('utf-8'),
                        'rating_count': data.get(b'info:rating_count', b'0').decode('utf-8')
                    })
                    
                    if len(matched_movies) >= limit:
                        break
            
            return matched_movies
        except Exception as e:
            logger.error(f"搜索电影失败 query={query}: {e}")
            raise


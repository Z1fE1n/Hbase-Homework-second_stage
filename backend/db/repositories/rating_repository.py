"""评分数据仓库"""

from typing import List, Dict
from collections import defaultdict
from functools import wraps
from backend.db.hbase import hbase_connection
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


class RatingRepository:
    """评分数据访问对象"""
    
    def __init__(self):
        self.table = None
        self._refresh_table()
    
    def _refresh_table(self):
        """刷新表连接"""
        self.table = hbase_connection.get_ratings_table()
    
    @retry_on_connection_error(max_retries=2)
    def find_by_movie_id(self, movie_id: str, limit: int = None, max_scan_rows: int = 50000) -> List[dict]:
        """查找电影的评分记录
        
        Args:
            movie_id: 电影ID
            limit: 返回数量限制，None表示返回全部（但受max_scan_rows限制）
            max_scan_rows: 最大扫描行数，防止全表扫描导致假死
            
        Returns:
            List[dict]: 评分记录列表
        """
        ratings = []
        scan_count = 0
        try:
            for key, data in self.table.scan():
                scan_count += 1
                
                # 防止无限扫描导致假死
                if scan_count > max_scan_rows:
                    logger.warning(f"扫描行数超过限制 {max_scan_rows}，停止扫描")
                    break
                
                key_str = key.decode('utf-8')
                parts = key_str.split('_')
                
                if len(parts) == 2:
                    user_id, mid = parts
                    if mid == movie_id:
                        ratings.append({
                            'user_id': user_id,
                            'movie_id': movie_id,
                            'rating': data.get(b'data:rating', b'0').decode('utf-8'),
                            'timestamp': data.get(b'data:timestamp', b'').decode('utf-8')
                        })
                        
                        if limit and len(ratings) >= limit:
                            break
            
            return ratings
        except Exception as e:
            logger.error(f"查询电影评分失败 movie_id={movie_id}: {e}")
            raise
    
    @retry_on_connection_error(max_retries=2)
    def get_rating_stats(self, movie_id: str) -> dict:
        """获取电影评分统计
        
        从电影表获取预计算的统计数据，避免扫描评分表导致性能问题
        
        Args:
            movie_id: 电影ID
            
        Returns:
            dict: 评分统计信息
        """
        try:
            # 从电影表获取预计算的统计数据，避免全表扫描评分表
            movies_table = hbase_connection.get_movies_table()
            row = movies_table.row(movie_id.encode('utf-8'))
            
            if not row:
                return {
                    'avg_rating': 0.0,
                    'total_count': 0,
                    'rating_distribution': {}
                }
            
            avg_rating = float(row.get(b'info:avg_rating', b'0').decode('utf-8') or '0')
            rating_count = int(row.get(b'info:rating_count', b'0').decode('utf-8') or '0')
            
            # 评分分布使用估算值（基于平均评分和总数）
            # 如果需要精确分布，应该在导入数据时预计算并存储
            rating_distribution = {}
            if rating_count > 0:
                # 简单估算：假设正态分布，以平均评分为中心
                rating_distribution = self._estimate_distribution(avg_rating, rating_count)
            
            return {
                'avg_rating': avg_rating,
                'total_count': rating_count,
                'rating_distribution': rating_distribution
            }
        except Exception as e:
            logger.error(f"获取评分统计失败 movie_id={movie_id}: {e}")
            raise
    
    def _estimate_distribution(self, avg_rating: float, total_count: int) -> dict:
        """估算评分分布
        
        基于平均评分估算分布，用于前端展示
        """
        # 简单的分布估算，实际项目中应该在数据导入时预计算
        distribution = {}
        
        # 根据平均评分估算各评分段的比例
        avg = avg_rating
        for score in [1, 2, 3, 4, 5]:
            # 使用简单的距离权重
            distance = abs(score - avg)
            weight = max(0.05, 1.0 - distance * 0.25)
            count = int(total_count * weight / 5)  # 大致分配
            if count > 0:
                distribution[str(score)] = count
        
        return distribution
    
    @retry_on_connection_error(max_retries=2)
    def find_by_user_id(self, user_id: str, limit: int = 10) -> List[dict]:
        """查找用户的评分记录
        
        Args:
            user_id: 用户ID
            limit: 返回数量限制
            
        Returns:
            List[dict]: 评分记录列表
        """
        ratings = []
        try:
            # 使用前缀扫描优化查询
            start_row = f"{user_id}_".encode('utf-8')
            stop_row = f"{user_id}_~".encode('utf-8')
            
            for key, data in self.table.scan(row_start=start_row, row_stop=stop_row):
                key_str = key.decode('utf-8')
                parts = key_str.split('_')
                
                if len(parts) == 2:
                    uid, movie_id = parts
                    ratings.append({
                        'user_id': uid,
                        'movie_id': movie_id,
                        'rating': data.get(b'data:rating', b'0').decode('utf-8'),
                        'timestamp': data.get(b'data:timestamp', b'').decode('utf-8')
                    })
                    
                    if len(ratings) >= limit:
                        break
            
            return ratings
        except Exception as e:
            logger.error(f"查询用户评分失败 user_id={user_id}: {e}")
            raise

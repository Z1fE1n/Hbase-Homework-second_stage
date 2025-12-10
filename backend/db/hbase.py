"""HBase连接管理"""

import happybase
from typing import Optional
from backend.core.config import settings
from backend.core.logging import logger


class HBaseConnection:
    """HBase连接管理器（单例模式）- 支持自动重连"""
    
    _instance: Optional['HBaseConnection'] = None
    _connection: Optional[happybase.Connection] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def _create_connection(self) -> happybase.Connection:
        """创建新的HBase连接"""
        return happybase.Connection(
            host=settings.hbase_host,
            port=settings.hbase_port,
            timeout=30000,  # 30秒超时
            autoconnect=True,
            table_prefix=None,
            table_prefix_separator=b'_',
            compat='0.98',
            transport='buffered',
            protocol='binary'
        )
    
    def _test_connection(self) -> bool:
        """测试连接是否有效"""
        if self._connection is None:
            return False
        try:
            # 尝试列出表来测试连接
            self._connection.tables()
            return True
        except Exception as e:
            logger.warning(f"连接测试失败: {e}")
            return False
    
    def connect(self) -> happybase.Connection:
        """建立或重连HBase连接
        
        Returns:
            happybase.Connection: HBase连接对象
        """
        # 如果连接不存在或已失效，重新创建
        if not self._test_connection():
            try:
                # 关闭旧连接
                if self._connection:
                    try:
                        self._connection.close()
                    except:
                        pass
                    self._connection = None
                
                # 创建新连接
                self._connection = self._create_connection()
                logger.info(f"HBase连接成功: {settings.hbase_host}:{settings.hbase_port}")
            except Exception as e:
                logger.error(f"HBase连接失败: {e}")
                raise
        
        return self._connection
    
    def get_connection(self) -> happybase.Connection:
        """获取HBase连接（自动重连）
        
        Returns:
            happybase.Connection: HBase连接对象
        """
        return self.connect()
    
    def get_movies_table(self) -> happybase.Table:
        """获取movies表（自动重连）
        
        Returns:
            happybase.Table: movies表对象
        """
        conn = self.connect()
        return conn.table(settings.movies_table)
    
    def get_ratings_table(self) -> happybase.Table:
        """获取ratings表（自动重连）
        
        Returns:
            happybase.Table: ratings表对象
        """
        conn = self.connect()
        return conn.table(settings.ratings_table)
    
    def close(self):
        """关闭HBase连接"""
        if self._connection:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None
            logger.info("HBase连接已关闭")


# 全局连接实例
hbase_connection = HBaseConnection()


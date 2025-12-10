"""配置管理模块"""

from pydantic_settings import BaseSettings
from typing import List
import yaml
from pathlib import Path


class Settings(BaseSettings):
    """应用配置类"""
    
    # HBase配置
    hbase_host: str = "192.168.98.88"
    hbase_port: int = 9090
    
    # 数据库表名
    movies_table: str = "movies"
    ratings_table: str = "ratings"
    
    # 服务器配置
    server_host: str = "0.0.0.0"
    server_port: int = 8000
    debug: bool = True
    
    # CORS配置
    cors_origins: List[str] = ["http://localhost:5173", "http://localhost:3000"]
    
    # 分页配置
    default_page_size: int = 20
    max_page_size: int = 100
    
    # 搜索配置
    max_search_limit: int = 100
    max_scan_rows: int = 10000
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


def load_config_from_yaml(config_path: str = "config.yaml") -> Settings:
    """从YAML文件加载配置
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        Settings: 配置对象
    """
    if not Path(config_path).exists():
        return Settings()
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = yaml.safe_load(f)
    
    return Settings(
        hbase_host=config_data.get('hbase', {}).get('host', '192.168.98.88'),
        hbase_port=config_data.get('hbase', {}).get('port', 9090),
        movies_table=config_data.get('database', {}).get('movies_table', 'movies'),
        ratings_table=config_data.get('database', {}).get('ratings_table', 'ratings'),
        server_host=config_data.get('server', {}).get('host', '0.0.0.0'),
        server_port=config_data.get('server', {}).get('port', 8000),
        debug=config_data.get('server', {}).get('debug', True),
    )


# 全局配置实例
settings = load_config_from_yaml()


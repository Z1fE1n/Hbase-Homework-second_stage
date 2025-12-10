"""日志配置模块"""

import logging
import sys
from typing import Optional


def setup_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None
) -> logging.Logger:
    """配置应用日志
    
    Args:
        level: 日志级别
        format_string: 日志格式字符串
        
    Returns:
        logging.Logger: 配置好的日志记录器
    """
    if format_string is None:
        format_string = "[%(asctime)s] %(levelname)s [%(name)s] %(message)s"
    
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger("movielens")


# 全局日志实例
logger = setup_logging()


#!/usr/bin/env python3
"""
Spark 批处理任务
计算电影评分统计并更新 HBase
"""

import json
import sys
import time
from pathlib import Path
from datetime import datetime
import yaml

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("[警告] PySpark 未安装，将使用 Pandas 进行计算")

import happybase


class BatchProcessor:
    """批处理器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """初始化"""
        # 获取脚本所在目录作为项目根目录
        self.project_root = Path(__file__).parent.resolve()
        
        config_file = self.project_root / config_path
        with open(config_file, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.spark = None
        self.connection = None
        
        # 使用绝对路径
        data_dir = self.project_root / "backend" / "data"
        self.log_file = data_dir / "batch_log.txt"
        self.status_file = data_dir / "batch_status.json"
        
        # 确保目录存在
        data_dir.mkdir(parents=True, exist_ok=True)
    
    def log(self, message: str, level: str = "INFO"):
        """写日志"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] [{level}] {message}"
        print(log_line, flush=True)
        
        # 追加到日志文件，确保立即写入
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_line + "\n")
            f.flush()
    
    def update_status(self, status: str, progress: int = 0, message: str = ""):
        """更新状态"""
        status_data = {
            "status": status,  # running, completed, failed
            "progress": progress,
            "message": message,
            "updated_at": datetime.now().isoformat()
        }
        with open(self.status_file, 'w', encoding='utf-8') as f:
            json.dump(status_data, f, ensure_ascii=False)
            f.flush()
    
    def clear_log(self):
        """清空日志"""
        with open(self.log_file, 'w', encoding='utf-8') as f:
            f.write("")
    
    def init_spark(self):
        """初始化 Spark"""
        if not SPARK_AVAILABLE:
            self.log("PySpark 不可用，使用 Pandas 模式", "WARN")
            return False
        
        try:
            self.log("初始化 Spark Session...")
            self.spark = SparkSession.builder \
                .appName("MovieLens Rating Calculator") \
                .master("local[*]") \
                .config("spark.driver.memory", "2g") \
                .config("spark.sql.shuffle.partitions", "4") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            self.log("Spark Session 初始化成功")
            return True
        except Exception as e:
            self.log(f"Spark 初始化失败: {e}", "ERROR")
            return False
    
    def connect_hbase(self):
        """连接 HBase"""
        self.log(f"连接 HBase: {self.config['hbase']['host']}:{self.config['hbase']['port']}")
        
        try:
            self.connection = happybase.Connection(
                host=self.config['hbase']['host'],
                port=self.config['hbase']['port'],
                timeout=60000,
                transport='buffered',
                protocol='binary'
            )
            tables = self.connection.tables()
            self.log(f"HBase 连接成功，当前有 {len(tables)} 个表")
            return True
        except Exception as e:
            self.log(f"HBase 连接失败: {e}", "ERROR")
            return False
    
    def calculate_with_spark(self, ratings_path: str):
        """使用 Spark 计算评分统计"""
        self.log(f"使用 Spark 读取评分数据: {ratings_path}")
        
        # 读取 CSV
        df = self.spark.read.csv(ratings_path, header=True, inferSchema=True)
        total_ratings = df.count()
        self.log(f"总评分数: {total_ratings:,}")
        
        # 计算每个电影的统计
        self.log("开始计算评分统计...")
        stats_df = df.groupBy("movieId").agg(
            F.avg("rating").alias("avg_rating"),
            F.count("rating").alias("rating_count")
        )
        
        # 收集结果
        results = stats_df.collect()
        self.log(f"计算完成，共 {len(results)} 部电影")
        
        return {
            str(row.movieId): {
                "avg": float(row.avg_rating),
                "count": int(row.rating_count)
            }
            for row in results
        }
    
    def calculate_with_pandas(self, ratings_path: str):
        """使用 Pandas 计算评分统计（备选方案）"""
        import pandas as pd
        
        self.log(f"使用 Pandas 读取评分数据: {ratings_path}")
        
        # 分块读取大文件
        chunk_size = 100000
        stats = {}
        total_ratings = 0
        
        for chunk in pd.read_csv(ratings_path, chunksize=chunk_size):
            total_ratings += len(chunk)
            
            # 按电影分组计算
            grouped = chunk.groupby('movieId')['rating'].agg(['sum', 'count'])
            
            for movie_id, row in grouped.iterrows():
                movie_id = str(movie_id)
                if movie_id in stats:
                    stats[movie_id]['sum'] += row['sum']
                    stats[movie_id]['count'] += row['count']
                else:
                    stats[movie_id] = {
                        'sum': row['sum'],
                        'count': int(row['count'])
                    }
            
            self.log(f"已处理 {total_ratings:,} 条评分...")
        
        self.log(f"总评分数: {total_ratings:,}")
        
        # 计算平均值
        results = {
            movie_id: {
                "avg": data['sum'] / data['count'],
                "count": data['count']
            }
            for movie_id, data in stats.items()
        }
        
        self.log(f"计算完成，共 {len(results)} 部电影")
        return results
    
    def update_hbase(self, rating_stats: dict):
        """更新 HBase 中的评分统计"""
        self.log("开始更新 HBase...")
        
        movies_table = self.connection.table(self.config['database']['movies_table'])
        batch = movies_table.batch(batch_size=1000)
        
        updated = 0
        total = len(rating_stats)
        
        for movie_id, stats in rating_stats.items():
            data = {
                b'info:avg_rating': f"{stats['avg']:.2f}".encode('utf-8'),
                b'info:rating_count': str(stats['count']).encode('utf-8')
            }
            batch.put(movie_id.encode('utf-8'), data)
            updated += 1
            
            if updated % 5000 == 0:
                self.log(f"已更新 {updated:,}/{total:,} 部电影 ({updated*100//total}%)")
                self.update_status("running", updated * 80 // total + 10, f"更新 HBase: {updated}/{total}")
        
        batch.send()
        self.log(f"HBase 更新完成: {updated:,} 部电影")
    
    def update_index(self, rating_stats: dict):
        """更新 JSON 索引文件"""
        self.log("开始更新 JSON 索引...")
        
        # 使用绝对路径
        index_path = self.project_root / "backend" / "data" / "movie_index.json"
        if not index_path.exists():
            self.log("索引文件不存在，跳过更新", "WARN")
            return
        
        # 读取现有索引
        with open(index_path, 'r', encoding='utf-8') as f:
            movies = json.load(f)
        
        # 更新评分
        updated = 0
        for movie in movies:
            movie_id = movie['id']
            if movie_id in rating_stats:
                stats = rating_stats[movie_id]
                movie['avg_rating'] = round(stats['avg'], 2)
                movie['rating_count'] = stats['count']
                updated += 1
        
        # 写回索引
        with open(index_path, 'w', encoding='utf-8') as f:
            json.dump(movies, f, ensure_ascii=False)
        
        self.log(f"索引更新完成: {updated} 部电影")
    
    def run(self):
        """执行批处理"""
        start_time = time.time()
        
        try:
            # 清空日志
            self.clear_log()
            self.update_status("running", 0, "初始化...")
            
            self.log("=" * 60)
            self.log("批处理任务开始（使用 Pandas）")
            self.log("=" * 60)
            
            # 检查数据文件（使用绝对路径）
            csv_dir = self.project_root / self.config['data']['csv_dir']
            ratings_path = csv_dir / self.config['data']['ratings_file']
            
            if not ratings_path.exists():
                raise FileNotFoundError(f"评分文件不存在: {ratings_path}")
            
            self.log(f"评分文件: {ratings_path}")
            self.update_status("running", 5, "读取数据...")
            
            # 直接使用 Pandas 计算（更快更稳定）
            # Spark 在本地环境初始化很慢，对于这个任务 Pandas 足够了
            self.update_status("running", 10, "计算评分统计...")
            rating_stats = self.calculate_with_pandas(str(ratings_path))
            
            self.update_status("running", 50, "连接 HBase...")
            
            # 连接 HBase
            if not self.connect_hbase():
                raise ConnectionError("无法连接 HBase")
            
            # 更新 HBase
            self.update_hbase(rating_stats)
            self.update_status("running", 90, "更新索引...")
            
            # 更新索引
            self.update_index(rating_stats)
            
            elapsed = time.time() - start_time
            self.log("=" * 60)
            self.log(f"批处理完成！耗时: {elapsed:.1f} 秒")
            self.log("=" * 60)
            
            self.update_status("completed", 100, f"完成，耗时 {elapsed:.1f} 秒")
            return True
            
        except Exception as e:
            self.log(f"批处理失败: {e}", "ERROR")
            import traceback
            self.log(traceback.format_exc(), "ERROR")
            self.update_status("failed", 0, str(e))
            return False
        
        finally:
            if self.connection:
                self.connection.close()
                self.log("HBase 连接已关闭")


def main():
    """主函数"""
    # 获取脚本所在目录
    project_root = Path(__file__).parent.resolve()
    log_dir = project_root / "backend" / "data"
    
    # 确保输出不被缓冲
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)
    
    # 确保日志目录存在
    log_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        processor = BatchProcessor()
        success = processor.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        # 确保错误被记录
        error_msg = f"批处理启动失败: {e}"
        print(error_msg, file=sys.stderr)
        
        # 写入错误到日志文件
        log_file = log_dir / "batch_log.txt"
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"[ERROR] {error_msg}\n")
            import traceback
            f.write(traceback.format_exc())
        
        # 更新状态
        status_file = log_dir / "batch_status.json"
        with open(status_file, 'w', encoding='utf-8') as f:
            json.dump({
                "status": "failed",
                "progress": 0,
                "message": str(e),
                "updated_at": None
            }, f, ensure_ascii=False)
        
        sys.exit(1)


if __name__ == "__main__":
    main()


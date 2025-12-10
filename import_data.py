#!/usr/bin/env python3
"""
MovieLens 数据导入脚本
将 movies.csv 和 ratings.csv 导入到 HBase
"""

import csv
import json
import sys
import time
from pathlib import Path
from typing import Dict, List
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import happybase
import yaml
from tqdm import tqdm


class HBaseImporter:
    """HBase数据导入器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """初始化导入器"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.connection = None
        self.movies_table = None
        self.ratings_table = None
    
    def _check_hbase_service(self):
        """检查 HBase 服务状态"""
        import socket
        
        host = self.config['hbase']['host']
        port = self.config['hbase']['port']
        
        print(f"[检查] HBase Thrift 服务 {host}:{port}...")
        
        try:
            # 尝试连接端口
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                print(f"   ✓ 端口 {port} 可访问")
                return True
            else:
                print(f"   ✗ 端口 {port} 无法连接")
                return False
        except Exception as e:
            print(f"   ✗ 检查失败: {e}")
            return False
    
    def connect(self):
        """连接HBase - 自动尝试多种配置"""
        print(f"\n[连接] HBase: {self.config['hbase']['host']}:{self.config['hbase']['port']}")
        
        # 先检查服务
        if not self._check_hbase_service():
            print("\n[错误] HBase Thrift 服务不可用")
            print("[提示] 请启动 HBase 和 Thrift 服务:")
            print("   1. 启动 HBase: start-hbase.sh")
            print("   2. 启动 Thrift: hbase-daemon.sh start thrift")
            return False
        
        # 尝试不同的配置组合
        configs = [
            {'transport': 'buffered', 'protocol': 'binary', 'timeout': 120000},
            {'transport': 'framed', 'protocol': 'binary', 'timeout': 120000},
            {'transport': 'buffered', 'protocol': 'compact', 'timeout': 120000},
        ]
        
        for idx, config in enumerate(configs, 1):
            try:
                print(f"\n[尝试 {idx}/{len(configs)}] transport={config['transport']}, protocol={config['protocol']}")
                print(f"   超时时间: {config['timeout']/1000:.0f}秒")
                
                self.connection = happybase.Connection(
                    host=self.config['hbase']['host'],
                    port=self.config['hbase']['port'],
                    timeout=config['timeout'],
                    transport=config['transport'],
                    protocol=config['protocol']
                )
                
                # 测试连接
                print("   正在测试连接...")
                tables = self.connection.tables()
                print(f"   ✓ 连接成功！(当前有 {len(tables)} 个表)")
                print(f"   使用配置: transport={config['transport']}, protocol={config['protocol']}")
                
                return True
                
            except Exception as e:
                print(f"   ✗ 失败: {e}")
                if self.connection:
                    try:
                        self.connection.close()
                    except:
                        pass
                    self.connection = None
                
                if idx == len(configs):
                    # 最后一次尝试也失败了
                    print(f"\n[错误] 所有连接配置都失败了")
                    print("\n[诊断建议]")
                    print("1. 检查 HBase 是否真的在运行:")
                    print("   jps | grep HMaster")
                    print("   jps | grep HRegionServer")
                    print("\n2. 检查 Thrift 服务状态:")
                    print("   jps | grep ThriftServer")
                    print("\n3. 查看 HBase 日志:")
                    print("   tail -f $HBASE_HOME/logs/hbase-*-master-*.log")
                    print("   tail -f $HBASE_HOME/logs/hbase-*-thrift-*.log")
                    print("\n4. 尝试重启服务:")
                    print("   hbase-daemon.sh stop thrift")
                    print("   hbase-daemon.sh start thrift")
                    return False
        
        return False
    
    def create_tables(self):
        """创建HBase表"""
        print("\n[创建] HBase 表...")
        
        try:
            # 获取现有表列表
            print("[步骤1] 检查现有表...")
            existing_tables = [t.decode('utf-8') for t in self.connection.tables()]
            print(f"   当前表列表: {existing_tables if existing_tables else '(空)'}")
            
            # 创建 movies 表
            movies_table_name = self.config['database']['movies_table']
            print(f"\n[步骤2] 处理 {movies_table_name} 表...")
            
            if movies_table_name.encode() in self.connection.tables():
                print(f"   表已存在，准备删除...")
                try:
                    # 先禁用
                    print(f"   正在禁用表...")
                    self.connection.disable_table(movies_table_name)
                    print(f"   正在删除表...")
                    self.connection.delete_table(movies_table_name)
                    print(f"   ✓ 删除成功")
                except Exception as e:
                    print(f"   [警告] 删除表时出错: {e}")
                    print(f"   尝试强制重建...")
            
            print(f"   正在创建表...")
            self.connection.create_table(
                movies_table_name,
                {'info': dict()}
            )
            print(f"   ✓ 创建成功: {movies_table_name}")
            
            # 创建 ratings 表
            ratings_table_name = self.config['database']['ratings_table']
            print(f"\n[步骤3] 处理 {ratings_table_name} 表...")
            
            if ratings_table_name.encode() in self.connection.tables():
                print(f"   表已存在，准备删除...")
                try:
                    print(f"   正在禁用表...")
                    self.connection.disable_table(ratings_table_name)
                    print(f"   正在删除表...")
                    self.connection.delete_table(ratings_table_name)
                    print(f"   ✓ 删除成功")
                except Exception as e:
                    print(f"   [警告] 删除表时出错: {e}")
                    print(f"   尝试强制重建...")
            
            print(f"   正在创建表...")
            self.connection.create_table(
                ratings_table_name,
                {'data': dict()}
            )
            print(f"   ✓ 创建成功: {ratings_table_name}")
            
            # 获取表对象
            print(f"\n[步骤4] 获取表对象...")
            self.movies_table = self.connection.table(movies_table_name)
            self.ratings_table = self.connection.table(ratings_table_name)
            print(f"   ✓ 表对象获取成功")
            
            print(f"\n[成功] 所有表创建完成！")
            
        except Exception as e:
            print(f"\n[错误] 创建表失败: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def import_movies(self, csv_path: str):
        """导入电影数据"""
        print(f"\n[导入] 电影数据: {csv_path}")
        
        if not Path(csv_path).exists():
            print(f"[错误] 文件不存在: {csv_path}")
            return False
        
        # 先统计总行数用于进度显示
        print("[准备] 统计数据量...")
        total_movies = sum(1 for _ in open(csv_path, 'r', encoding='utf-8')) - 1  # 减去header
        
        # 计算评分统计
        print("[步骤1] 计算评分统计...")
        rating_stats = self._calculate_rating_stats()
        
        # 导入电影数据
        print(f"[步骤2] 导入电影信息 (总计 {total_movies:,} 部)...")
        movies_count = 0
        start_time = time.time()
        
        # 用于生成索引的列表
        movie_index_list = []
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # 增大batch_size提升性能
            batch = self.movies_table.batch(batch_size=5000)
            
            # 增强进度条显示
            with tqdm(total=total_movies, desc="导入电影", unit="部", 
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
                
                for row in reader:
                    movie_id = row['movieId']
                    title = row['title']
                    genres = row['genres']
                    
                    # 构建数据
                    data = {
                        b'info:title': title.encode('utf-8'),
                        b'info:genres': genres.encode('utf-8'),
                    }
                    
                    # 添加评分统计
                    if movie_id in rating_stats:
                        stats = rating_stats[movie_id]
                        avg_rating = stats['avg']
                        rating_count = stats['count']
                        data[b'info:avg_rating'] = f"{avg_rating:.2f}".encode('utf-8')
                        data[b'info:rating_count'] = str(rating_count).encode('utf-8')
                    else:
                        avg_rating = 0.0
                        rating_count = 0
                        data[b'info:avg_rating'] = b'0.00'
                        data[b'info:rating_count'] = b'0'
                    
                    batch.put(movie_id.encode('utf-8'), data)
                    movies_count += 1
                    pbar.update(1)
                    
                    # 添加到索引列表
                    movie_index_list.append({
                        'id': movie_id,
                        'title': title,
                        'genres': genres,
                        'avg_rating': round(avg_rating, 2),
                        'rating_count': rating_count
                    })
                    
                    # 每5000条显示一次统计
                    if movies_count % 5000 == 0:
                        elapsed = time.time() - start_time
                        speed = movies_count / elapsed
                        pbar.set_postfix({'速度': f'{speed:.0f}部/s'})
            
            batch.send()
        
        elapsed = time.time() - start_time
        print(f"[成功] 导入电影完成: {movies_count:,} 部，耗时 {elapsed:.1f}秒，平均 {movies_count/elapsed:.0f}部/秒")
        
        # 生成 JSON 索引文件
        self._generate_movie_index(movie_index_list)
        
        return True
    
    def _generate_movie_index(self, movie_list: List[Dict]):
        """生成电影搜索索引 JSON 文件"""
        print(f"\n[索引] 生成搜索索引文件...")
        
        # 确保目录存在
        index_dir = Path("backend/data")
        index_dir.mkdir(parents=True, exist_ok=True)
        
        index_path = index_dir / "movie_index.json"
        
        # 按 ID 排序（确保数字顺序）
        movie_list.sort(key=lambda x: int(x['id']))
        
        # 写入 JSON 文件
        with open(index_path, 'w', encoding='utf-8') as f:
            json.dump(movie_list, f, ensure_ascii=False)
        
        file_size_mb = index_path.stat().st_size / (1024 * 1024)
        print(f"[成功] 索引文件已生成: {index_path}")
        print(f"   电影数量: {len(movie_list):,}")
        print(f"   文件大小: {file_size_mb:.2f} MB")
    
    def _calculate_rating_stats(self) -> Dict[str, Dict]:
        """计算每个电影的评分统计"""
        ratings_path = Path(self.config['data']['csv_dir']) / self.config['data']['ratings_file']
        
        if not ratings_path.exists():
            print(f"[警告] 评分文件不存在，跳过统计: {ratings_path}")
            return {}
        
        # 统计总行数
        total_ratings = sum(1 for _ in open(ratings_path, 'r', encoding='utf-8')) - 1
        print(f"   总评分数: {total_ratings:,} 条")
        
        stats = defaultdict(lambda: {'sum': 0.0, 'count': 0})
        start_time = time.time()
        
        with open(ratings_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            with tqdm(total=total_ratings, desc="统计评分", unit="条",
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
                
                processed = 0
                for row in reader:
                    movie_id = row['movieId']
                    rating = float(row['rating'])
                    stats[movie_id]['sum'] += rating
                    stats[movie_id]['count'] += 1
                    processed += 1
                    pbar.update(1)
                    
                    # 显示速度
                    if processed % 50000 == 0:
                        elapsed = time.time() - start_time
                        speed = processed / elapsed
                        pbar.set_postfix({'速度': f'{speed:.0f}条/s'})
        
        # 计算平均值
        result = {}
        for movie_id, data in stats.items():
            result[movie_id] = {
                'avg': data['sum'] / data['count'],
                'count': data['count']
            }
        
        elapsed = time.time() - start_time
        print(f"   统计完成: {len(result):,} 部电影，耗时 {elapsed:.1f}秒")
        return result
    
    def import_ratings(self, csv_path: str):
        """导入评分数据 - 使用多线程优化"""
        print(f"\n[导入] 评分数据: {csv_path}")
        
        if not Path(csv_path).exists():
            print(f"[错误] 文件不存在: {csv_path}")
            return False
        
        # 统计总行数
        print("[准备] 统计数据量...")
        total_ratings = sum(1 for _ in open(csv_path, 'r', encoding='utf-8')) - 1
        print(f"   总评分数: {total_ratings:,} 条")
        
        ratings_count = 0
        start_time = time.time()
        
        # 分块大小：10万条一批，可根据内存调整
        chunk_size = 100000
        max_workers = 4  # 并发线程数
        
        # 读取所有数据并分块
        print(f"[策略] 使用 {max_workers} 线程并发导入，每批 {chunk_size:,} 条...")
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            with tqdm(total=total_ratings, desc="导入评分", unit="条",
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
                
                # 使用更大的batch提升性能
                batch = self.ratings_table.batch(batch_size=10000)
                
                for row in reader:
                    # 行键：userId_movieId
                    row_key = f"{row['userId']}_{row['movieId']}".encode('utf-8')
                    
                    data = {
                        b'data:rating': row['rating'].encode('utf-8'),
                        b'data:timestamp': row['timestamp'].encode('utf-8'),
                    }
                    
                    batch.put(row_key, data)
                    ratings_count += 1
                    pbar.update(1)
                    
                    # 显示实时速度
                    if ratings_count % 50000 == 0:
                        elapsed = time.time() - start_time
                        speed = ratings_count / elapsed
                        pbar.set_postfix({'速度': f'{speed:.0f}条/s', '已完成': f'{ratings_count:,}'})
                
                batch.send()
        
        elapsed = time.time() - start_time
        print(f"[成功] 导入评分完成: {ratings_count:,} 条，耗时 {elapsed:.1f}秒，平均 {ratings_count/elapsed:.0f}条/秒")
        return True
    
    def verify_import(self):
        """验证导入结果"""
        print("\n[验证] 导入结果...")
        
        # 验证movies表
        movies_count = 0
        for _ in self.movies_table.scan(limit=10):
            movies_count += 1
        print(f"[成功] movies 表有数据: 至少 {movies_count} 条记录")
        
        # 显示示例
        print("\n[示例] 电影表数据:")
        for key, data in list(self.movies_table.scan(limit=3)):
            movie_id = key.decode('utf-8')
            title = data.get(b'info:title', b'').decode('utf-8')
            genres = data.get(b'info:genres', b'').decode('utf-8')
            avg_rating = data.get(b'info:avg_rating', b'0').decode('utf-8')
            rating_count = data.get(b'info:rating_count', b'0').decode('utf-8')
            print(f"  ID: {movie_id}")
            print(f"  标题: {title}")
            print(f"  类型: {genres}")
            print(f"  平均评分: {avg_rating} ({rating_count} 人)")
            print()
        
        # 验证ratings表
        ratings_count = 0
        for _ in self.ratings_table.scan(limit=10):
            ratings_count += 1
        print(f"[成功] ratings 表有数据: 至少 {ratings_count} 条记录")
        
        print("\n[示例] 评分表数据:")
        for key, data in list(self.ratings_table.scan(limit=3)):
            user_movie = key.decode('utf-8')
            rating = data.get(b'data:rating', b'').decode('utf-8')
            timestamp = data.get(b'data:timestamp', b'').decode('utf-8')
            print(f"  {user_movie}: {rating} (时间戳: {timestamp})")
    
    def close(self):
        """关闭连接"""
        if self.connection:
            self.connection.close()
            print("\n[成功] HBase 连接已关闭")
    
    def run(self):
        """执行完整导入流程"""
        try:
            # 连接
            if not self.connect():
                return False
            
            # 创建表
            self.create_tables()
            
            # 导入数据
            csv_dir = Path(self.config['data']['csv_dir'])
            movies_csv = csv_dir / self.config['data']['movies_file']
            ratings_csv = csv_dir / self.config['data']['ratings_file']
            
            self.import_movies(str(movies_csv))
            self.import_ratings(str(ratings_csv))
            
            # 验证
            self.verify_import()
            
            print("\n[完成] 数据导入成功！")
            return True
            
        except Exception as e:
            print(f"\n[错误] 导入过程出错: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.close()


def check_hbase_service():
    """快速检查 HBase 服务状态"""
    print("\n" + "=" * 60)
    print("HBase 服务诊断工具")
    print("=" * 60)
    
    import subprocess
    
    # 检查 Java 进程
    print("\n[检查 1] Java 进程...")
    try:
        result = subprocess.run(['jps'], capture_output=True, text=True, timeout=5)
        processes = result.stdout.strip().split('\n')
        
        required_services = ['HMaster', 'HRegionServer', 'ThriftServer']
        found_services = []
        
        for proc in processes:
            for service in required_services:
                if service in proc:
                    found_services.append(service)
                    print(f"   ✓ {service} 正在运行")
        
        missing_services = set(required_services) - set(found_services)
        if missing_services:
            print(f"   ✗ 缺失服务: {', '.join(missing_services)}")
            return False
        else:
            print(f"   ✓ 所有必需服务都在运行")
            return True
            
    except FileNotFoundError:
        print("   ✗ 找不到 jps 命令，请确保 Java 已安装并配置好 PATH")
        return False
    except Exception as e:
        print(f"   ✗ 检查失败: {e}")
        return False


def main():
    """主函数"""
    print("=" * 60)
    print("MovieLens 数据导入工具")
    print("=" * 60)
    
    # 如果启动参数包含 --check，只做诊断
    if len(sys.argv) > 1 and sys.argv[1] == '--check':
        check_hbase_service()
        sys.exit(0)
    
    importer = HBaseImporter()
    success = importer.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()


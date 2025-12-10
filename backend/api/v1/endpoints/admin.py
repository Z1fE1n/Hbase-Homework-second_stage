"""后台管理端点"""

import json
import subprocess
import os
import signal
import sys
from pathlib import Path
from fastapi import APIRouter, HTTPException
from backend.core.logging import logger

router = APIRouter()

# 获取项目根目录（相对于此文件的位置）
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.resolve()

# 数据目录（使用绝对路径）
DATA_DIR = PROJECT_ROOT / "backend" / "data"
STATUS_FILE = DATA_DIR / "batch_status.json"
LOG_FILE = DATA_DIR / "batch_log.txt"
PID_FILE = DATA_DIR / "batch.pid"


def ensure_data_dir():
    """确保数据目录存在"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_status() -> dict:
    """获取批处理状态"""
    ensure_data_dir()
    
    if not STATUS_FILE.exists():
        return {
            "status": "idle",
            "progress": 0,
            "message": "未运行",
            "updated_at": None
        }
    
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            status = json.load(f)
        
        # 如果状态是 running，检查进程是否真的还在运行
        if status.get("status") == "running":
            if not is_process_running():
                # 进程已结束但状态未更新，可能是异常退出
                status["status"] = "failed"
                status["message"] = "进程异常退出"
                save_status(status)
        
        return status
    except Exception as e:
        logger.error(f"读取状态失败: {e}")
        return {
            "status": "idle",
            "progress": 0,
            "message": "状态读取失败",
            "updated_at": None
        }


def save_status(status: dict):
    """保存状态"""
    ensure_data_dir()
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(status, f, ensure_ascii=False)


def get_logs() -> str:
    """获取日志内容"""
    if not LOG_FILE.exists():
        return ""
    
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"读取日志失败: {e}")
        return ""


def is_process_running() -> bool:
    """检查批处理进程是否在运行"""
    if not PID_FILE.exists():
        return False
    
    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        
        # 检查进程是否存在
        if sys.platform == 'win32':
            # Windows
            import ctypes
            kernel32 = ctypes.windll.kernel32
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if handle:
                kernel32.CloseHandle(handle)
                return True
            return False
        else:
            # Unix/Linux
            os.kill(pid, 0)
            return True
    except (ValueError, OSError, FileNotFoundError):
        return False


def kill_process():
    """终止批处理进程"""
    if not PID_FILE.exists():
        return False
    
    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        
        if sys.platform == 'win32':
            # Windows: 使用 taskkill
            subprocess.run(['taskkill', '/F', '/PID', str(pid)], 
                          capture_output=True, check=False)
        else:
            # Unix/Linux
            os.kill(pid, signal.SIGTERM)
        
        # 删除 PID 文件
        PID_FILE.unlink(missing_ok=True)
        return True
    except Exception as e:
        logger.error(f"终止进程失败: {e}")
        return False


@router.get("/batch/status")
async def batch_status():
    """获取批处理状态"""
    return get_status()


@router.get("/batch/logs")
async def batch_logs():
    """获取批处理日志"""
    return {
        "logs": get_logs()
    }


@router.post("/batch/start")
async def start_batch():
    """启动批处理任务"""
    ensure_data_dir()
    
    # 检查是否已有任务在运行
    if is_process_running():
        raise HTTPException(status_code=400, detail="已有批处理任务正在运行")
    
    try:
        # 获取项目根目录（spark_batch.py 所在目录）
        project_root = Path(__file__).parent.parent.parent.parent.parent.resolve()
        spark_script = project_root / "spark_batch.py"
        
        if not spark_script.exists():
            raise FileNotFoundError(f"找不到批处理脚本: {spark_script}")
        
        logger.info(f"项目根目录: {project_root}")
        logger.info(f"批处理脚本: {spark_script}")
        
        # 清空旧日志
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write("")
        
        # 初始化状态
        save_status({
            "status": "running",
            "progress": 0,
            "message": "启动中...",
            "updated_at": None
        })
        
        # 启动子进程运行批处理，设置正确的工作目录
        if sys.platform == 'win32':
            # Windows: 使用 CREATE_NEW_PROCESS_GROUP
            process = subprocess.Popen(
                [sys.executable, str(spark_script)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd=str(project_root),
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
            )
        else:
            # Unix/Linux
            process = subprocess.Popen(
                [sys.executable, str(spark_script)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                cwd=str(project_root),
                start_new_session=True
            )
        
        # 保存 PID
        with open(PID_FILE, 'w') as f:
            f.write(str(process.pid))
        
        logger.info(f"批处理任务已启动，PID: {process.pid}")
        return {"message": "批处理任务已启动", "status": "running", "pid": process.pid}
        
    except Exception as e:
        logger.error(f"启动批处理失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        save_status({
            "status": "failed",
            "progress": 0,
            "message": f"启动失败: {str(e)}",
            "updated_at": None
        })
        raise HTTPException(status_code=500, detail=f"启动失败: {str(e)}")


@router.post("/batch/stop")
async def stop_batch():
    """停止批处理任务"""
    if not is_process_running():
        # 清理状态
        save_status({
            "status": "idle",
            "progress": 0,
            "message": "未运行",
            "updated_at": None
        })
        PID_FILE.unlink(missing_ok=True)
        return {"message": "没有正在运行的任务"}
    
    if kill_process():
        save_status({
            "status": "stopped",
            "progress": 0,
            "message": "已手动停止",
            "updated_at": None
        })
        return {"message": "批处理任务已停止"}
    else:
        raise HTTPException(status_code=500, detail="停止任务失败")


@router.post("/index/reload")
async def reload_index():
    """重新加载电影索引"""
    try:
        from backend.services.movie_service import MovieIndexService
        index_service = MovieIndexService()
        index_service.reload_index()
        return {"message": "索引已重新加载"}
    except Exception as e:
        logger.error(f"重载索引失败: {e}")
        raise HTTPException(status_code=500, detail=f"重载失败: {str(e)}")

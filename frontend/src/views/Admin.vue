<template>
  <div class="admin-page">
    <div class="admin-container">
      <div class="admin-header">
        <h1 class="page-title">
          <i class="ri-settings-3-line"></i>
          后台管理
        </h1>
        <p class="page-subtitle">Spark 批处理任务控制台</p>
      </div>
      
      <div class="admin-content">
        <!-- 任务控制区 -->
        <div class="control-section">
          <div class="control-card">
            <div class="card-header">
              <h2 class="card-title">
                <i class="ri-database-2-line"></i>
                评分统计更新
              </h2>
              <div class="status-badge" :class="statusClass">
                <span class="status-dot"></span>
                {{ statusText }}
              </div>
            </div>
            
            <div class="card-body">
              <p class="card-desc">
                使用 Spark 重新计算所有电影的评分统计（平均分、评分数量），并更新 HBase 和搜索索引。
              </p>
              
              <div v-if="status.status === 'running'" class="progress-section">
                <div class="progress-bar">
                  <div class="progress-fill" :style="{ width: status.progress + '%' }"></div>
                </div>
                <span class="progress-text">{{ status.progress }}%</span>
              </div>
              
              <div class="action-buttons">
                <button
                  v-if="status.status !== 'running'"
                  class="btn btn-primary"
                  @click="startBatch"
                  :disabled="starting"
                >
                  <i class="ri-play-fill"></i>
                  {{ starting ? '启动中...' : '开始计算' }}
                </button>
                
                <button
                  v-else
                  class="btn btn-danger"
                  @click="stopBatch"
                >
                  <i class="ri-stop-fill"></i>
                  停止任务
                </button>
                
                <button
                  class="btn btn-secondary"
                  @click="reloadIndex"
                  :disabled="reloading"
                >
                  <i class="ri-refresh-line"></i>
                  {{ reloading ? '重载中...' : '重载索引' }}
                </button>
              </div>
            </div>
          </div>
        </div>
        
        <!-- 日志区 -->
        <div class="log-section">
          <div class="log-card">
            <div class="log-header">
              <h2 class="log-title">
                <i class="ri-terminal-line"></i>
                实时日志
              </h2>
              <div class="log-actions">
                <button class="icon-btn" @click="clearLogs" title="清空日志">
                  <i class="ri-delete-bin-line"></i>
                </button>
                <button class="icon-btn" @click="scrollToBottom" title="滚动到底部">
                  <i class="ri-arrow-down-line"></i>
                </button>
              </div>
            </div>
            
            <div ref="logContainer" class="log-container">
              <div v-if="logs.length === 0" class="log-empty">
                <i class="ri-file-list-3-line"></i>
                <p>暂无日志</p>
              </div>
              
              <div
                v-for="(log, index) in logs"
                :key="index"
                class="log-line"
                :class="getLogClass(log)"
              >
                <span class="log-content">{{ log }}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, nextTick } from 'vue'
import { adminApi } from '@/services/api'

const logs = ref([])
const status = ref({
  status: 'idle',
  progress: 0,
  message: ''
})
const starting = ref(false)
const reloading = ref(false)
const logContainer = ref(null)

let pollInterval = null

const statusClass = computed(() => ({
  'status-idle': status.value.status === 'idle',
  'status-running': status.value.status === 'running',
  'status-completed': status.value.status === 'completed',
  'status-failed': status.value.status === 'failed',
  'status-stopped': status.value.status === 'stopped'
}))

const statusText = computed(() => {
  const map = {
    'idle': '空闲',
    'running': '运行中',
    'completed': '已完成',
    'failed': '失败',
    'stopped': '已停止'
  }
  return map[status.value.status] || '未知'
})

const getLogClass = (log) => {
  if (log.includes('[ERROR]')) return 'log-error'
  if (log.includes('[WARN]')) return 'log-warn'
  if (log.includes('[INFO]')) return 'log-info'
  if (log.includes('====')) return 'log-separator'
  return ''
}

const scrollToBottom = () => {
  nextTick(() => {
    if (logContainer.value) {
      logContainer.value.scrollTop = logContainer.value.scrollHeight
    }
  })
}

const clearLogs = () => {
  logs.value = []
}

const fetchStatus = async () => {
  try {
    const res = await adminApi.getBatchStatus()
    status.value = res
  } catch (error) {
    console.error('获取状态失败:', error)
  }
}

const fetchLogs = async () => {
  try {
    const res = await adminApi.getBatchLogs()
    if (res.logs) {
      const newLogs = res.logs.split('\n').filter(line => line.trim())
      // 只有当日志有变化时才更新
      if (newLogs.length !== logs.value.length) {
        logs.value = newLogs
        scrollToBottom()
      }
    }
  } catch (error) {
    console.error('获取日志失败:', error)
  }
}

// 使用轮询方式获取状态和日志
const startPolling = () => {
  stopPolling()
  // 每秒轮询一次
  pollInterval = setInterval(async () => {
    await fetchStatus()
    await fetchLogs()
    
    // 如果任务已完成，停止轮询
    if (['completed', 'failed', 'stopped'].includes(status.value.status)) {
      // 继续轮询几次以确保获取最终日志
      setTimeout(() => {
        if (['completed', 'failed', 'stopped'].includes(status.value.status)) {
          stopPolling()
        }
      }, 3000)
    }
  }, 1000)
}

const stopPolling = () => {
  if (pollInterval) {
    clearInterval(pollInterval)
    pollInterval = null
  }
}

const startBatch = async () => {
  try {
    starting.value = true
    logs.value = []
    
    await adminApi.startBatch()
    
    // 开始轮询
    startPolling()
    
    // 立即获取一次状态
    await fetchStatus()
  } catch (error) {
    console.error('启动批处理失败:', error)
    alert('启动失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    starting.value = false
  }
}

const stopBatch = async () => {
  try {
    await adminApi.stopBatch()
    stopPolling()
    await fetchStatus()
    await fetchLogs()
  } catch (error) {
    console.error('停止批处理失败:', error)
  }
}

const reloadIndex = async () => {
  try {
    reloading.value = true
    await adminApi.reloadIndex()
    alert('索引已重新加载')
  } catch (error) {
    console.error('重载索引失败:', error)
    alert('重载失败: ' + (error.response?.data?.detail || error.message))
  } finally {
    reloading.value = false
  }
}

onMounted(async () => {
  // 先获取状态
  await fetchStatus()
  await fetchLogs()
  
  // 如果正在运行，开始轮询
  if (status.value.status === 'running') {
    startPolling()
  }
})

onUnmounted(() => {
  stopPolling()
})
</script>

<style scoped>
.admin-page {
  min-height: 100vh;
  padding: 40px;
  background: linear-gradient(135deg, rgba(20, 20, 30, 0.95) 0%, rgba(10, 10, 15, 0.98) 100%);
}

.admin-container {
  max-width: 1400px;
  margin: 0 auto;
}

.admin-header {
  margin-bottom: 48px;
}

.page-title {
  font-size: 40px;
  font-weight: 700;
  margin: 0 0 12px 0;
  display: flex;
  align-items: center;
  gap: 16px;
  color: #fff;
}

.page-title i {
  font-size: 36px;
  color: rgba(255, 255, 255, 0.6);
}

.page-subtitle {
  font-size: 16px;
  color: rgba(255, 255, 255, 0.4);
  margin: 0;
}

.admin-content {
  display: grid;
  grid-template-columns: 400px 1fr;
  gap: 32px;
}

/* 控制区 */
.control-section {
  position: sticky;
  top: 120px;
  height: fit-content;
}

.control-card {
  background: rgba(255, 255, 255, 0.04);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 20px;
  overflow: hidden;
}

.card-header {
  padding: 24px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.card-title {
  font-size: 18px;
  font-weight: 600;
  margin: 0;
  display: flex;
  align-items: center;
  gap: 10px;
  color: #fff;
}

.card-title i {
  color: rgba(255, 255, 255, 0.5);
}

.status-badge {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 14px;
  border-radius: 20px;
  font-size: 13px;
  font-weight: 500;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
}

.status-idle {
  background: rgba(150, 150, 150, 0.15);
  color: rgba(200, 200, 200, 0.8);
}
.status-idle .status-dot {
  background: rgba(150, 150, 150, 0.6);
}

.status-running {
  background: rgba(59, 130, 246, 0.15);
  color: rgba(147, 197, 253, 0.9);
}
.status-running .status-dot {
  background: #3b82f6;
  animation: pulse 1.5s ease-in-out infinite;
}

.status-completed {
  background: rgba(34, 197, 94, 0.15);
  color: rgba(134, 239, 172, 0.9);
}
.status-completed .status-dot {
  background: #22c55e;
}

.status-failed {
  background: rgba(239, 68, 68, 0.15);
  color: rgba(252, 165, 165, 0.9);
}
.status-failed .status-dot {
  background: #ef4444;
}

.status-stopped {
  background: rgba(234, 179, 8, 0.15);
  color: rgba(253, 224, 71, 0.9);
}
.status-stopped .status-dot {
  background: #eab308;
}

@keyframes pulse {
  0%, 100% { opacity: 1; transform: scale(1); }
  50% { opacity: 0.5; transform: scale(1.2); }
}

.card-body {
  padding: 24px;
}

.card-desc {
  font-size: 14px;
  color: rgba(255, 255, 255, 0.5);
  line-height: 1.6;
  margin: 0 0 24px 0;
}

.progress-section {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 24px;
}

.progress-bar {
  flex: 1;
  height: 8px;
  background: rgba(255, 255, 255, 0.1);
  border-radius: 4px;
  overflow: hidden;
}

.progress-fill {
  height: 100%;
  background: linear-gradient(90deg, #3b82f6, #8b5cf6);
  border-radius: 4px;
  transition: width 0.3s ease;
}

.progress-text {
  font-size: 14px;
  font-weight: 600;
  color: rgba(255, 255, 255, 0.7);
  min-width: 45px;
  text-align: right;
}

.action-buttons {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.btn {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 14px 24px;
  border-radius: 12px;
  font-size: 15px;
  font-weight: 600;
  border: none;
  cursor: pointer;
  transition: all 0.3s ease;
}

.btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-primary {
  background: linear-gradient(135deg, #3b82f6, #8b5cf6);
  color: #fff;
}

.btn-primary:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 8px 24px rgba(59, 130, 246, 0.3);
}

.btn-danger {
  background: linear-gradient(135deg, #ef4444, #dc2626);
  color: #fff;
}

.btn-danger:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 8px 24px rgba(239, 68, 68, 0.3);
}

.btn-secondary {
  background: rgba(255, 255, 255, 0.08);
  border: 1px solid rgba(255, 255, 255, 0.12);
  color: rgba(255, 255, 255, 0.8);
}

.btn-secondary:hover:not(:disabled) {
  background: rgba(255, 255, 255, 0.12);
  border-color: rgba(255, 255, 255, 0.2);
}

/* 日志区 */
.log-card {
  background: rgba(0, 0, 0, 0.3);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 20px;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  height: calc(100vh - 240px);
  min-height: 500px;
}

.log-header {
  padding: 20px 24px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: rgba(0, 0, 0, 0.2);
}

.log-title {
  font-size: 16px;
  font-weight: 600;
  margin: 0;
  display: flex;
  align-items: center;
  gap: 10px;
  color: rgba(255, 255, 255, 0.8);
}

.log-title i {
  color: rgba(255, 255, 255, 0.4);
}

.log-actions {
  display: flex;
  gap: 8px;
}

.icon-btn {
  width: 36px;
  height: 36px;
  border: none;
  background: rgba(255, 255, 255, 0.06);
  color: rgba(255, 255, 255, 0.5);
  border-radius: 8px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s ease;
}

.icon-btn:hover {
  background: rgba(255, 255, 255, 0.1);
  color: rgba(255, 255, 255, 0.8);
}

.log-container {
  flex: 1;
  overflow-y: auto;
  padding: 16px;
  font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
  font-size: 13px;
  line-height: 1.8;
}

.log-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: rgba(255, 255, 255, 0.3);
  gap: 12px;
}

.log-empty i {
  font-size: 48px;
}

.log-line {
  padding: 4px 12px;
  border-radius: 4px;
  margin-bottom: 2px;
  color: rgba(255, 255, 255, 0.7);
}

.log-line:hover {
  background: rgba(255, 255, 255, 0.03);
}

.log-info {
  color: rgba(147, 197, 253, 0.9);
}

.log-warn {
  color: rgba(253, 224, 71, 0.9);
  background: rgba(234, 179, 8, 0.08);
}

.log-error {
  color: rgba(252, 165, 165, 0.9);
  background: rgba(239, 68, 68, 0.08);
}

.log-separator {
  color: rgba(255, 255, 255, 0.3);
  font-weight: 600;
}

/* 滚动条 */
.log-container::-webkit-scrollbar {
  width: 8px;
}

.log-container::-webkit-scrollbar-track {
  background: rgba(255, 255, 255, 0.02);
}

.log-container::-webkit-scrollbar-thumb {
  background: rgba(255, 255, 255, 0.1);
  border-radius: 4px;
}

.log-container::-webkit-scrollbar-thumb:hover {
  background: rgba(255, 255, 255, 0.15);
}

@media (max-width: 1024px) {
  .admin-content {
    grid-template-columns: 1fr;
  }
  
  .control-section {
    position: static;
  }
  
  .log-card {
    height: 500px;
  }
}

@media (max-width: 768px) {
  .admin-page {
    padding: 20px;
  }
  
  .page-title {
    font-size: 28px;
  }
  
  .page-title i {
    font-size: 24px;
  }
}
</style>


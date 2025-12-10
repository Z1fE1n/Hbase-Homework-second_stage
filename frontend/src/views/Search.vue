<template>
  <div class="search-page">
    <div class="page-container">
      <div class="search-header">
        <h1 class="page-title">搜索电影</h1>
        
        <div class="search-input-wrapper">
          <i class="ri-search-line search-icon"></i>
          <input
            v-model="searchQuery"
            type="text"
            placeholder="输入电影名称..."
            class="search-input"
            @keyup.enter="handleSearch"
          />
          <button
            v-if="searchQuery"
            class="clear-btn"
            @click="clearSearch"
          >
            <i class="ri-close-line"></i>
          </button>
        </div>
      </div>
      
      <div v-if="!searched" class="empty-state">
        <i class="ri-search-2-line"></i>
        <p>输入关键词开始搜索</p>
      </div>
      
      <div v-else-if="loading" class="loading-state">
        <div class="spinner"></div>
      </div>
      
      <div v-else>
        <div v-if="results.length" class="search-results">
          <div class="results-header">
            <h2 class="results-title">
              找到 <span class="highlight">{{ results.length }}</span> 个结果
            </h2>
          </div>
          
          <div class="movies-grid">
            <MovieCard
              v-for="movie in results"
              :key="movie.id"
              :movie="movie"
            />
          </div>
        </div>
        
        <div v-else class="empty-state">
          <i class="ri-film-line"></i>
          <p>未找到相关电影</p>
          <span class="empty-hint">试试其他关键词</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { movieApi } from '@/services/api'
import MovieCard from '@/components/MovieCard.vue'

const route = useRoute()
const router = useRouter()

const searchQuery = ref('')
const results = ref([])
const loading = ref(false)
const searched = ref(false)

const handleSearch = async () => {
  if (!searchQuery.value.trim()) return
  
  router.push({ query: { q: searchQuery.value } })
  await performSearch()
}

const performSearch = async () => {
  const query = route.query.q
  if (!query) {
    searched.value = false
    return
  }
  
  try {
    loading.value = true
    searched.value = true
    const response = await movieApi.searchMovies(query, 50)
    results.value = response.movies
  } catch (error) {
    console.error('搜索失败:', error)
    results.value = []
  } finally {
    loading.value = false
  }
}

const clearSearch = () => {
  searchQuery.value = ''
  results.value = []
  searched.value = false
  router.push({ query: {} })
}

onMounted(() => {
  const query = route.query.q
  if (query) {
    searchQuery.value = query
    performSearch()
  }
})
</script>

<style scoped>
.search-page {
  min-height: 100vh;
  padding: 40px;
}

.page-container {
  max-width: 1400px;
  margin: 0 auto;
}

.search-header {
  margin-bottom: 60px;
  display: flex;
  flex-direction: column;
  gap: 32px;
}

.page-title {
  font-size: 48px;
  font-weight: 700;
  margin: 0;
  letter-spacing: -1px;
}

.search-input-wrapper {
  position: relative;
  max-width: 600px;
}

.search-icon {
  position: absolute;
  left: 24px;
  top: 50%;
  transform: translateY(-50%);
  font-size: 24px;
  color: rgba(255, 255, 255, 0.4);
  pointer-events: none;
}

.search-input {
  width: 100%;
  height: 64px;
  padding: 0 64px;
  background: rgba(255, 255, 255, 0.08);
  backdrop-filter: blur(10px);
  border: 2px solid rgba(255, 255, 255, 0.15);
  border-radius: 16px;
  color: #fff;
  font-size: 18px;
  outline: none;
  transition: all 0.3s ease;
}

.search-input:focus {
  background: rgba(255, 255, 255, 0.12);
  border-color: rgba(255, 255, 255, 0.25);
}

.search-input::placeholder {
  color: rgba(255, 255, 255, 0.3);
}

.clear-btn {
  position: absolute;
  right: 16px;
  top: 50%;
  transform: translateY(-50%);
  width: 32px;
  height: 32px;
  border: none;
  background: rgba(255, 255, 255, 0.1);
  color: rgba(255, 255, 255, 0.6);
  border-radius: 50%;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.3s ease;
}

.clear-btn:hover {
  background: rgba(255, 255, 255, 0.2);
  color: #fff;
}

.clear-btn i {
  font-size: 20px;
}

.empty-state,
.loading-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 120px 20px;
  gap: 16px;
}

.empty-state i {
  font-size: 64px;
  color: rgba(255, 255, 255, 0.2);
}

.empty-state p {
  font-size: 18px;
  color: rgba(255, 255, 255, 0.4);
  margin: 0;
}

.empty-hint {
  font-size: 14px;
  color: rgba(255, 255, 255, 0.3);
}

.spinner {
  width: 48px;
  height: 48px;
  border: 3px solid rgba(255, 255, 255, 0.1);
  border-top-color: rgba(255, 255, 255, 0.6);
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.search-results {
  display: flex;
  flex-direction: column;
  gap: 40px;
}

.results-header {
  padding-bottom: 24px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}

.results-title {
  font-size: 24px;
  font-weight: 600;
  margin: 0;
  color: rgba(255, 255, 255, 0.7);
}

.highlight {
  color: #fff;
  font-weight: 700;
}

.movies-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 32px;
}

@media (max-width: 768px) {
  .search-page {
    padding: 20px;
  }
  
  .page-title {
    font-size: 32px;
  }
  
  .search-input {
    height: 56px;
    font-size: 16px;
  }
  
  .movies-grid {
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 20px;
  }
}
</style>


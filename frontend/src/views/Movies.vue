<template>
  <div class="movies-page" :class="{ 'has-results': hasSearched }">
    <div class="search-section" :class="{ 'search-top': hasSearched }">
      <div class="search-content">
        <h1 v-if="!hasSearched" class="search-title">探索电影库</h1>
        <p v-if="!hasSearched" class="search-subtitle">搜索数万部电影，发现你的最爱</p>
        
        <div class="search-input-wrapper" :class="{ 'compact': hasSearched }">
          <i class="ri-search-line search-icon"></i>
          <input
            ref="searchInput"
            v-model="searchQuery"
            type="text"
            placeholder="输入电影名称或类型..."
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
          <button
            class="search-btn"
            @click="handleSearch"
            :disabled="!searchQuery.trim()"
          >
            <i class="ri-arrow-right-line"></i>
          </button>
        </div>
      </div>
    </div>
    
    <div v-if="hasSearched" class="results-section">
      <div class="results-container">
        <div v-if="loading" class="loading-state">
          <div class="spinner"></div>
        </div>
        
        <template v-else>
          <div v-if="results.length" class="search-results">
            <div class="results-header">
              <h2 class="results-title">
                搜索 "<span class="query-text">{{ lastQuery }}</span>" 找到 
                <span class="highlight">{{ results.length }}</span> 个结果
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
            <p>未找到 "{{ lastQuery }}" 相关电影</p>
            <span class="empty-hint">试试其他关键词</span>
          </div>
        </template>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { movieApi } from '@/services/api'
import MovieCard from '@/components/MovieCard.vue'

const route = useRoute()
const router = useRouter()

const searchInput = ref(null)
const searchQuery = ref('')
const lastQuery = ref('')
const results = ref([])
const loading = ref(false)
const hasSearched = ref(false)

const handleSearch = async () => {
  const query = searchQuery.value.trim()
  if (!query) return
  
  router.push({ query: { q: query } })
  await performSearch(query)
}

const performSearch = async (query) => {
  if (!query) {
    hasSearched.value = false
    return
  }
  
  try {
    loading.value = true
    hasSearched.value = true
    lastQuery.value = query
    
    await nextTick()
    
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
  hasSearched.value = false
  lastQuery.value = ''
  router.push({ query: {} })
  
  nextTick(() => {
    searchInput.value?.focus()
  })
}

onMounted(() => {
  const query = route.query.q
  if (query) {
    searchQuery.value = query
    performSearch(query)
  } else {
    searchInput.value?.focus()
  }
})
</script>

<style scoped>
.movies-page {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}

.search-section {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
  padding: 40px;
  transition: all 0.5s cubic-bezier(0.4, 0, 0.2, 1);
}

.search-section.search-top {
  min-height: auto;
  padding: 32px 40px;
  background: rgba(0, 0, 0, 0.3);
  backdrop-filter: blur(20px);
  border-bottom: 1px solid rgba(255, 255, 255, 0.08);
  position: sticky;
  top: 0;
  z-index: 100;
}

.search-content {
  text-align: center;
  width: 100%;
  max-width: 800px;
  transition: all 0.5s cubic-bezier(0.4, 0, 0.2, 1);
}

.search-top .search-content {
  max-width: 1400px;
}

.search-title {
  font-size: 56px;
  font-weight: 700;
  margin: 0 0 16px 0;
  letter-spacing: -2px;
  background: linear-gradient(135deg, #fff 0%, rgba(255,255,255,0.7) 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

.search-subtitle {
  font-size: 20px;
  color: rgba(255, 255, 255, 0.5);
  margin: 0 0 48px 0;
}

.search-input-wrapper {
  position: relative;
  display: flex;
  align-items: center;
  gap: 12px;
  transition: all 0.4s ease;
}

.search-input-wrapper.compact {
  max-width: 600px;
  margin: 0 auto;
}

.search-icon {
  position: absolute;
  left: 28px;
  font-size: 24px;
  color: rgba(255, 255, 255, 0.4);
  pointer-events: none;
  z-index: 1;
}

.search-input {
  flex: 1;
  height: 72px;
  padding: 0 140px 0 72px;
  background: rgba(255, 255, 255, 0.08);
  backdrop-filter: blur(10px);
  border: 2px solid rgba(255, 255, 255, 0.12);
  border-radius: 20px;
  color: #fff;
  font-size: 20px;
  outline: none;
  transition: all 0.3s ease;
}

.compact .search-input {
  height: 56px;
  font-size: 16px;
  padding: 0 120px 0 56px;
  border-radius: 16px;
}

.compact .search-icon {
  left: 20px;
  font-size: 20px;
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
  right: 80px;
  width: 36px;
  height: 36px;
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

.compact .clear-btn {
  right: 70px;
  width: 32px;
  height: 32px;
}

.clear-btn:hover {
  background: rgba(255, 255, 255, 0.2);
  color: #fff;
}

.clear-btn i {
  font-size: 20px;
}

.search-btn {
  position: absolute;
  right: 16px;
  width: 52px;
  height: 52px;
  border: none;
  background: linear-gradient(135deg, rgba(255,255,255,0.2) 0%, rgba(255,255,255,0.1) 100%);
  color: #fff;
  border-radius: 14px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.3s ease;
  font-size: 24px;
}

.compact .search-btn {
  right: 12px;
  width: 40px;
  height: 40px;
  border-radius: 10px;
  font-size: 20px;
}

.search-btn:hover:not(:disabled) {
  background: linear-gradient(135deg, rgba(255,255,255,0.3) 0%, rgba(255,255,255,0.15) 100%);
  transform: translateX(2px);
}

.search-btn:disabled {
  opacity: 0.3;
  cursor: not-allowed;
}

.results-section {
  flex: 1;
  padding: 48px 40px 80px;
  animation: fadeInUp 0.5s ease;
}

@keyframes fadeInUp {
  from {
    opacity: 0;
    transform: translateY(20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.results-container {
  max-width: 1400px;
  margin: 0 auto;
}

.results-header {
  margin-bottom: 40px;
  padding-bottom: 24px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}

.results-title {
  font-size: 24px;
  font-weight: 600;
  margin: 0;
  color: rgba(255, 255, 255, 0.7);
}

.query-text {
  color: rgba(255, 255, 255, 0.9);
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

.loading-state {
  display: flex;
  justify-content: center;
  padding: 120px 0;
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

.empty-state {
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

@media (max-width: 768px) {
  .search-section {
    padding: 24px 20px;
  }
  
  .search-section.search-top {
    padding: 20px;
  }
  
  .search-title {
    font-size: 36px;
  }
  
  .search-subtitle {
    font-size: 16px;
    margin-bottom: 32px;
  }
  
  .search-input {
    height: 60px;
    font-size: 16px;
    padding: 0 120px 0 56px;
    border-radius: 16px;
  }
  
  .search-icon {
    left: 20px;
    font-size: 20px;
  }
  
  .search-btn {
    width: 44px;
    height: 44px;
    right: 12px;
  }
  
  .clear-btn {
    right: 64px;
    width: 32px;
    height: 32px;
  }
  
  .results-section {
    padding: 32px 20px 60px;
  }
  
  .movies-grid {
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 20px;
  }
}
</style>

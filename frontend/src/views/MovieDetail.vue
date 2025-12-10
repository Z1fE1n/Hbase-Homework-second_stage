<template>
  <div class="movie-detail">
    <div v-if="loading" class="loading-state">
      <div class="spinner"></div>
    </div>
    
    <div v-else-if="movie" class="detail-container">
      <div class="detail-hero">
        <div class="hero-backdrop"></div>
        
        <div class="hero-content">
          <div class="poster-section">
            <div class="movie-poster-card">
              <div class="poster-bg" :style="{ background: getPosterColor() }"></div>
              <div class="poster-content">
                <div class="poster-initial">{{ getInitial(movie.title) }}</div>
                <div class="poster-genres">
                  <span v-for="genre in genres.slice(0, 3)" :key="genre" class="poster-genre">
                    {{ genre }}
                  </span>
                </div>
              </div>
            </div>
          </div>
          
          <div class="info-section">
            <h1 class="movie-title">{{ movie.title }}</h1>
            
            <div class="movie-genres">
              <span v-for="genre in genres" :key="genre" class="genre-tag">
                {{ genre }}
              </span>
            </div>
            
            <div class="movie-stats">
              <div class="stat-item">
                <div class="stat-value">
                  <i class="ri-star-fill"></i>
                  {{ formatRating(movie.avg_rating) }}
                </div>
                <div class="stat-label">平均评分</div>
              </div>
              
              <div class="stat-divider"></div>
              
              <div class="stat-item">
                <div class="stat-value">
                  {{ formatCount(movie.rating_count) }}
                </div>
                <div class="stat-label">评分人数</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    
    <div v-else class="error-state">
      <i class="ri-error-warning-line"></i>
      <p>电影不存在</p>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { movieApi } from '@/services/api'

const route = useRoute()
const movie = ref(null)
const loading = ref(true)

const genres = computed(() => {
  if (!movie.value || !movie.value.genres) return []
  return movie.value.genres.split('|').filter(g => g)
})

const getInitial = (title) => {
  if (!title) return '?'
  return title.charAt(0).toUpperCase()
}

const getPosterColor = () => {
  return 'rgba(255, 255, 255, 0.08)'
}

const formatRating = (rating) => {
  return rating > 0 ? rating.toFixed(1) : 'N/A'
}

const formatCount = (count) => {
  if (count >= 1000) {
    return (count / 1000).toFixed(1) + 'k'
  }
  return count
}

const loadMovie = async () => {
  try {
    loading.value = true
    const movieId = route.params.id
    const data = await movieApi.getMovieDetail(movieId)
    movie.value = data
  } catch (error) {
    console.error('加载电影详情失败:', error)
    movie.value = null
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  loadMovie()
})
</script>

<style scoped>
.movie-detail {
  min-height: 100vh;
}

.loading-state,
.error-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 120px 20px;
  gap: 16px;
}

.error-state i {
  font-size: 64px;
  color: rgba(255, 255, 255, 0.3);
}

.error-state p {
  font-size: 18px;
  color: rgba(255, 255, 255, 0.5);
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

.detail-container {
  max-width: 1400px;
  margin: 0 auto;
}

.detail-hero {
  position: relative;
  padding: 60px 40px;
  overflow: hidden;
}

.hero-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(255, 255, 255, 0.02);
}

.hero-content {
  position: relative;
  display: grid;
  grid-template-columns: 350px 1fr;
  gap: 60px;
  z-index: 1;
}

.poster-section {
  position: relative;
}

.movie-poster-card {
  position: relative;
  width: 100%;
  aspect-ratio: 2/3;
  border-radius: 20px;
  overflow: hidden;
  background: rgba(255, 255, 255, 0.05);
  backdrop-filter: blur(20px) saturate(180%);
  border: 1px solid rgba(255, 255, 255, 0.1);
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
}

.poster-bg {
  position: absolute;
  inset: 0;
}

.poster-content {
  position: absolute;
  inset: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 24px;
  padding: 32px;
}

.poster-initial {
  font-size: 128px;
  font-weight: 700;
  color: rgba(255, 255, 255, 0.2);
  line-height: 1;
  user-select: none;
}

.poster-genres {
  display: flex;
  flex-direction: column;
  gap: 12px;
  width: 100%;
}

.poster-genre {
  padding: 10px 16px;
  background: rgba(0, 0, 0, 0.5);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(255, 255, 255, 0.2);
  border-radius: 8px;
  font-size: 13px;
  font-weight: 600;
  color: #fff;
  text-align: center;
  text-transform: uppercase;
  letter-spacing: 1px;
}

.info-section {
  display: flex;
  flex-direction: column;
  gap: 32px;
  padding-top: 20px;
}

.movie-title {
  font-size: 48px;
  font-weight: 700;
  margin: 0;
  line-height: 1.2;
  letter-spacing: -1px;
}

.movie-genres {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
}

.genre-tag {
  padding: 8px 16px;
  background: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(255, 255, 255, 0.15);
  border-radius: 8px;
  font-size: 14px;
  font-weight: 500;
  color: rgba(255, 255, 255, 0.9);
}

.movie-stats {
  display: flex;
  gap: 40px;
  align-items: center;
}

.stat-item {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.stat-value {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 32px;
  font-weight: 700;
}

.stat-value i {
  font-size: 28px;
  color: #fbbf24;
}

.stat-label {
  font-size: 14px;
  color: rgba(255, 255, 255, 0.5);
}

.stat-divider {
  width: 1px;
  height: 60px;
  background: rgba(255, 255, 255, 0.15);
}

@media (max-width: 1024px) {
  .hero-content {
    grid-template-columns: 1fr;
    gap: 40px;
  }
  
  .movie-poster-card {
    max-width: 350px;
    margin: 0 auto;
  }
}

@media (max-width: 768px) {
  .detail-hero,
  .detail-body {
    padding: 20px;
  }
  
  .movie-title {
    font-size: 32px;
  }
  
  .movie-stats {
    gap: 24px;
  }
  
  .stat-value {
    font-size: 24px;
  }
  
  .poster-initial {
    font-size: 96px;
  }
}
</style>

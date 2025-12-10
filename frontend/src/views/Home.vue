<template>
  <div class="home">
    <section class="hero">
      <div class="hero-content">
        <h1 class="hero-title">
          探索电影世界
        </h1>
        <p class="hero-subtitle">
          精选数万部电影，发现你的下一部最爱
        </p>
        <div class="hero-actions">
          <router-link to="/movies" class="btn btn-primary">
            搜索电影
            <i class="ri-search-line"></i>
          </router-link>
        </div>
      </div>
      
      <div class="hero-visual">
        <div class="floating-text-container">
          <div 
            v-for="(genre, index) in floatingGenres" 
            :key="index"
            class="floating-text"
            :style="getFloatingStyle(index)"
          >
            {{ genre }}
          </div>
        </div>
      </div>
    </section>
    
    <section class="featured-section">
      <div class="section-container">
        <div class="section-header">
          <h2 class="section-title">精选推荐</h2>
          <router-link to="/movies" class="section-link">
            搜索更多
            <i class="ri-search-line"></i>
          </router-link>
        </div>
        
        <div v-if="loading" class="loading-state">
          <div class="spinner"></div>
        </div>
        
        <div v-else class="movies-grid">
          <MovieCard
            v-for="movie in featuredMovies"
            :key="movie.id"
            :movie="movie"
          />
        </div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { movieApi } from '@/services/api'
import MovieCard from '@/components/MovieCard.vue'

const featuredMovies = ref([])
const loading = ref(true)

const floatingGenres = [
  'Action', 'Adventure', 'Animation', 'Comedy', 'Crime',
  'Documentary', 'Drama', 'Fantasy', 'Horror', 'Mystery',
  'Romance', 'Sci-Fi', 'Thriller', 'Western', 'Musical',
  'War', 'History', 'Biography', 'Sport', 'Film-Noir'
]

const getFloatingStyle = (index) => {
  const positions = [
    { top: '10%', left: '5%' },
    { top: '20%', left: '35%' },
    { top: '15%', right: '15%' },
    { top: '35%', left: '15%' },
    { top: '40%', right: '25%' },
    { top: '55%', left: '25%' },
    { top: '50%', right: '10%' },
    { top: '65%', left: '10%' },
    { top: '70%', right: '30%' },
    { top: '80%', left: '40%' },
    { top: '5%', left: '60%' },
    { top: '25%', right: '5%' },
    { top: '45%', left: '50%' },
    { top: '60%', right: '45%' },
    { top: '75%', left: '5%' },
    { top: '30%', left: '70%' },
    { top: '85%', right: '15%' },
    { top: '12%', left: '45%' },
    { top: '68%', right: '60%' },
    { top: '90%', left: '55%' }
  ]
  
  const pos = positions[index % positions.length]
  const duration = 15 + (index * 2)
  const delay = index * 0.5
  
  return {
    ...pos,
    animationDuration: `${duration}s`,
    animationDelay: `${delay}s`,
    fontSize: `${12 + (index % 5) * 2}px`,
    opacity: 0.3 + (index % 4) * 0.1
  }
}

const loadFeaturedMovies = async () => {
  try {
    loading.value = true
    // 获取固定推荐（ID 1-8）
    const response = await movieApi.getFeaturedMovies(8)
    featuredMovies.value = response.movies
  } catch (error) {
    console.error('加载推荐电影失败:', error)
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  loadFeaturedMovies()
})
</script>

<style scoped>
.home {
  min-height: 100vh;
}

.hero {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 60px;
  align-items: center;
  min-height: calc(100vh - 80px);
  padding: 80px 40px;
  max-width: 1400px;
  margin: 0 auto;
  position: relative;
}

.hero-content {
  z-index: 2;
}

.hero-title {
  font-size: 72px;
  font-weight: 700;
  line-height: 1.1;
  margin: 0 0 24px 0;
  letter-spacing: -2px;
  color: #fff;
}

.hero-subtitle {
  font-size: 20px;
  color: rgba(255, 255, 255, 0.6);
  margin: 0 0 40px 0;
  line-height: 1.6;
  max-width: 500px;
}

.hero-actions {
  display: flex;
  gap: 16px;
}

.btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 16px 32px;
  border-radius: 12px;
  font-size: 16px;
  font-weight: 600;
  text-decoration: none;
  transition: all 0.3s ease;
  border: none;
  cursor: pointer;
}

.btn-primary {
  background: rgba(255, 255, 255, 0.15);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(255, 255, 255, 0.2);
  color: #fff;
}

.btn-primary:hover {
  background: rgba(255, 255, 255, 0.2);
  border-color: rgba(255, 255, 255, 0.3);
  transform: translateY(-2px);
}

.btn-primary i {
  transition: transform 0.3s ease;
}

.btn-primary:hover i {
  transform: translateX(4px);
}

.hero-visual {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  overflow: hidden;
}

.floating-text-container {
  position: relative;
  width: 100%;
  height: 100%;
}

.floating-text {
  position: absolute;
  font-weight: 600;
  color: rgba(255, 255, 255, 0.4);
  white-space: nowrap;
  animation: floatText 20s ease-in-out infinite;
  user-select: none;
  pointer-events: none;
  text-transform: uppercase;
  letter-spacing: 2px;
}

@keyframes floatText {
  0%, 100% {
    transform: translateY(0) translateX(0);
  }
  25% {
    transform: translateY(-30px) translateX(20px);
  }
  50% {
    transform: translateY(-15px) translateX(-15px);
  }
  75% {
    transform: translateY(-40px) translateX(10px);
  }
}

@keyframes fadeInUp {
  from {
    opacity: 0;
    transform: translateY(30px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.featured-section {
  padding: 80px 40px 120px;
  background: rgba(255, 255, 255, 0.02);
}

.section-container {
  max-width: 1400px;
  margin: 0 auto;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 40px;
}

.section-title {
  font-size: 32px;
  font-weight: 700;
  margin: 0;
}

.section-link {
  display: flex;
  align-items: center;
  gap: 6px;
  color: rgba(255, 255, 255, 0.6);
  text-decoration: none;
  font-size: 16px;
  font-weight: 500;
  transition: color 0.3s ease;
}

.section-link:hover {
  color: #fff;
}

.section-link i {
  transition: transform 0.3s ease;
}

.section-link:hover i {
  transform: translateX(4px);
}

.movies-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 32px;
}

.loading-state {
  display: flex;
  justify-content: center;
  padding: 80px 0;
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

@media (max-width: 1024px) {
  .hero {
    grid-template-columns: 1fr;
    gap: 40px;
    padding: 60px 20px;
  }
  
  .hero-title {
    font-size: 48px;
  }
  
  .hero-visual {
    height: 400px;
  }
  
  .floating-card {
    width: 120px;
  }
}

@media (max-width: 768px) {
  .movies-grid {
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 20px;
  }
}
</style>

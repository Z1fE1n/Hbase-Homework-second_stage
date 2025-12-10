<template>
  <router-link :to="`/movie/${movie.id}`" class="movie-card">
    <div class="movie-visual">
      <div class="visual-bg" :style="{ background: getCardColor() }"></div>
      <div class="visual-content">
        <div class="movie-initial">{{ getInitial(movie.title) }}</div>
        <div class="genre-tags">
          <span v-for="genre in getGenreList().slice(0, 2)" :key="genre" class="genre-tag">
            {{ genre }}
          </span>
        </div>
      </div>
      <div class="rating-display">
        <i class="ri-star-fill"></i>
        <span>{{ formatRating(movie.avg_rating) }}</span>
      </div>
    </div>
    
    <div class="movie-info">
      <h3 class="movie-title">{{ movie.title }}</h3>
      <div class="movie-meta">
        <span class="rating-count">
          <i class="ri-heart-line"></i>
          {{ formatCount(movie.rating_count) }} 评分
        </span>
      </div>
    </div>
  </router-link>
</template>

<script setup>
const props = defineProps({
  movie: {
    type: Object,
    required: true
  }
})

// 类型对应的颜色
const genreColors = {
  'Action': '#0ea5e9',
  'Adventure': '#8b5cf6',
  'Animation': '#ec4899',
  'Children': '#f59e0b',
  'Comedy': '#10b981',
  'Crime': '#ef4444',
  'Documentary': '#06b6d4',
  'Drama': '#6366f1',
  'Fantasy': '#a855f7',
  'Horror': '#dc2626',
  'Mystery': '#7c3aed',
  'Romance': '#f43f5e',
  'Sci-Fi': '#3b82f6',
  'Thriller': '#991b1b',
  'Western': '#d97706'
}

const getInitial = (title) => {
  if (!title) return '?'
  return title.charAt(0).toUpperCase()
}

const getGenreList = () => {
  if (!props.movie.genres) return []
  return props.movie.genres.split('|').filter(g => g)
}

const getCardColor = () => {
  const genres = getGenreList()
  if (genres.length === 0) return 'rgba(59, 130, 246, 0.1)'
  
  const firstGenre = genres[0]
  const color = genreColors[firstGenre] || '#3b82f6'
  return color + '1a' // 添加透明度
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
</script>

<style scoped>
.movie-card {
  display: block;
  text-decoration: none;
  color: inherit;
  transition: transform 0.3s ease;
}

.movie-card:hover {
  transform: translateY(-8px);
}

.movie-visual {
  position: relative;
  width: 100%;
  aspect-ratio: 3/4;
  border-radius: 16px;
  overflow: hidden;
  border: 1px solid rgba(255, 255, 255, 0.1);
  transition: all 0.3s ease;
}

.movie-card:hover .movie-visual {
  border-color: rgba(255, 255, 255, 0.3);
  box-shadow: 0 8px 32px rgba(255, 255, 255, 0.1);
}

.visual-bg {
  position: absolute;
  inset: 0;
  transition: transform 0.3s ease;
}

.movie-card:hover .visual-bg {
  transform: scale(1.05);
}

.visual-content {
  position: absolute;
  inset: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 20px;
  padding: 24px;
}

.movie-initial {
  font-size: 96px;
  font-weight: 700;
  color: rgba(255, 255, 255, 0.15);
  line-height: 1;
  user-select: none;
  transition: all 0.3s ease;
}

.movie-card:hover .movie-initial {
  color: rgba(255, 255, 255, 0.25);
  transform: scale(1.1);
}

.genre-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  justify-content: center;
}

.genre-tag {
  padding: 6px 12px;
  background: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(255, 255, 255, 0.2);
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  color: rgba(255, 255, 255, 0.9);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.rating-display {
  position: absolute;
  top: 16px;
  right: 16px;
  display: flex;
  align-items: center;
  gap: 6px;
  background: rgba(0, 0, 0, 0.6);
  backdrop-filter: blur(10px);
  padding: 8px 12px;
  border-radius: 8px;
  font-size: 14px;
  font-weight: 700;
  border: 1px solid rgba(255, 255, 255, 0.1);
}

.rating-display i {
  color: #fbbf24;
  font-size: 16px;
}

.movie-info {
  padding: 16px 4px;
}

.movie-title {
  font-size: 16px;
  font-weight: 600;
  margin: 0 0 8px 0;
  color: #fff;
  line-height: 1.4;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.movie-meta {
  display: flex;
  flex-direction: column;
  gap: 6px;
  font-size: 13px;
  color: rgba(255, 255, 255, 0.5);
}

.rating-count {
  display: flex;
  align-items: center;
  gap: 4px;
}

.rating-count i {
  font-size: 14px;
  color: rgba(255, 255, 255, 0.6);
}
</style>


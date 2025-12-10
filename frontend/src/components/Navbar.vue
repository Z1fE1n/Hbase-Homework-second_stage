<template>
  <nav class="navbar">
    <div class="navbar-container">
      <div class="navbar-left">
        <router-link to="/" class="navbar-brand">
          <span class="brand-text">MovieLens</span>
        </router-link>
        
        <div class="navbar-links">
          <router-link to="/" class="nav-link">
            <i class="ri-home-5-line"></i>
            <span>首页</span>
          </router-link>
          <router-link to="/movies" class="nav-link">
            <i class="ri-search-line"></i>
            <span>搜索</span>
          </router-link>
          <router-link to="/admin" class="nav-link">
            <i class="ri-settings-3-line"></i>
            <span>后台</span>
          </router-link>
        </div>
      </div>
      
      <div class="navbar-right">
        <div class="search-box" :class="{ active: searchActive }">
          <i class="ri-search-line search-icon" @click="toggleSearch"></i>
          <input
            v-model="searchQuery"
            type="text"
            placeholder="搜索电影..."
            class="search-input"
            @focus="searchActive = true"
            @blur="handleBlur"
            @keyup.enter="handleSearch"
          />
        </div>
      </div>
    </div>
  </nav>
</template>

<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'

const router = useRouter()
const searchQuery = ref('')
const searchActive = ref(false)

const toggleSearch = () => {
  searchActive.value = !searchActive.value
}

const handleBlur = () => {
  if (!searchQuery.value) {
    searchActive.value = false
  }
}

const handleSearch = () => {
  if (searchQuery.value.trim()) {
    router.push({ name: 'Movies', query: { q: searchQuery.value } })
    searchQuery.value = ''
    searchActive.value = false
  }
}
</script>

<style scoped>
.navbar {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  height: 80px;
  background: rgba(255, 255, 255, 0.05);
  backdrop-filter: blur(30px) saturate(180%);
  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
  z-index: 1000;
}

.navbar-container {
  max-width: 1400px;
  margin: 0 auto;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 40px;
}

.navbar-left {
  display: flex;
  align-items: center;
  gap: 60px;
}

.navbar-brand {
  text-decoration: none;
  display: flex;
  align-items: center;
}

.brand-text {
  font-size: 24px;
  font-weight: 600;
  color: #fff;
  letter-spacing: -0.5px;
}

.navbar-links {
  display: flex;
  gap: 30px;
}

.nav-link {
  display: flex;
  align-items: center;
  gap: 8px;
  color: rgba(255, 255, 255, 0.6);
  text-decoration: none;
  font-size: 15px;
  font-weight: 500;
  transition: color 0.3s ease;
  position: relative;
}

.nav-link i {
  font-size: 18px;
}

.nav-link:hover {
  color: rgba(255, 255, 255, 0.9);
}

.nav-link.router-link-active {
  color: #fff;
}

.nav-link.router-link-active::after {
  content: '';
  position: absolute;
  bottom: -28px;
  left: 0;
  right: 0;
  height: 2px;
  background: rgba(255, 255, 255, 0.8);
}

.navbar-right {
  display: flex;
  align-items: center;
}

.search-box {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(255, 255, 255, 0.08);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(255, 255, 255, 0.15);
  border-radius: 12px;
  height: 44px;
  width: 44px;
  transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  overflow: hidden;
}

.search-box.active {
  width: 300px;
  justify-content: flex-start;
  padding: 0 16px;
  background: rgba(255, 255, 255, 0.12);
  border-color: rgba(255, 255, 255, 0.25);
}

.search-icon {
  position: absolute;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -50%);
  font-size: 20px;
  color: rgba(255, 255, 255, 0.6);
  cursor: pointer;
  transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  z-index: 1;
}

.search-box.active .search-icon {
  position: static;
  left: auto;
  top: auto;
  transform: none;
  margin-right: 12px;
}

.search-input {
  background: none;
  border: none;
  outline: none;
  color: #fff;
  font-size: 15px;
  width: 0;
  opacity: 0;
  transition: opacity 0.3s ease, width 0.3s ease;
}

.search-box.active .search-input {
  width: 100%;
  opacity: 1;
}

.search-input::placeholder {
  color: rgba(255, 255, 255, 0.4);
}

@media (max-width: 768px) {
  .navbar-container {
    padding: 0 20px;
  }
  
  .navbar-left {
    gap: 30px;
  }
  
  .nav-link span {
    display: none;
  }
}
</style>


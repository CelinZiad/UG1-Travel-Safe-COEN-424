import { ref } from 'vue'

const API_BASE = '/api'

export function useApi() {
  const loading = ref(false)
  const error = ref(null)

  async function fetchAreas(quart = 'jour') {
    loading.value = true
    error.value = null
    try {
      const res = await fetch(`${API_BASE}/areas?quart=${quart}&limit=100`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      return data.items || []
    } catch (e) {
      error.value = e.message
      return []
    } finally {
      loading.value = false
    }
  }

  async function fetchAreaDetail(areaId, quart = 'jour') {
    loading.value = true
    error.value = null
    try {
      const encodedId = encodeURIComponent(areaId)
      const res = await fetch(`${API_BASE}/areas/${encodedId}?quart=${quart}`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      return await res.json()
    } catch (e) {
      error.value = e.message
      return null
    } finally {
      loading.value = false
    }
  }

  async function fetchAdvice(areaId, quart = 'jour') {
    loading.value = true
    error.value = null
    try {
      const encodedId = encodeURIComponent(areaId)
      const res = await fetch(`${API_BASE}/advice/${encodedId}?quart=${quart}`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      return await res.json()
    } catch (e) {
      error.value = e.message
      return null
    } finally {
      loading.value = false
    }
  }

  async function analyzeRoute(areaIds, quart = 'jour') {
    loading.value = true
    error.value = null
    try {
      const res = await fetch(`${API_BASE}/analyze-route`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ areaIds, quart })
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      return await res.json()
    } catch (e) {
      error.value = e.message
      return null
    } finally {
      loading.value = false
    }
  }

  async function askQuestion(query, areaIds = null, quart = null) {
    loading.value = true
    error.value = null
    try {
      const body = { query }
      if (areaIds) body.areaIds = areaIds
      if (quart) body.quart = quart

      const res = await fetch(`${API_BASE}/ask`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      return await res.json()
    } catch (e) {
      error.value = e.message
      return null
    } finally {
      loading.value = false
    }
  }

  async function checkHealth() {
    try {
      const res = await fetch(`${API_BASE}/health`)
      return res.ok
    } catch {
      return false
    }
  }

  return {
    loading,
    error,
    fetchAreas,
    fetchAreaDetail,
    fetchAdvice,
    analyzeRoute,
    askQuestion,
    checkHealth
  }
}

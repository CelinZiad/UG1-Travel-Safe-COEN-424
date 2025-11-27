<script setup>
import { ref, onMounted, watch } from 'vue'
import { useApi } from './composables/useApi'
import { normalizeString, findMatchingArea, geoNameToApiId } from './assets/montreal-boroughs'
import 'leaflet/dist/leaflet.css'
import L from 'leaflet'

const { fetchAreas, fetchAdvice, analyzeRoute, askQuestion, checkHealth } = useApi()

// State
const map = ref(null)
const geoJsonLayer = ref(null)
const boroughsGeoJson = ref(null)
const selectedQuart = ref('jour')
const areas = ref([])
const selectedArea = ref(null)
const advice = ref(null)
const adviceLoading = ref(false)
const routeAreas = ref([])
const routeResult = ref(null)
const routeLoading = ref(false)
const askQuery = ref('')
const askResponse = ref(null)
const askLoading = ref(false)
const apiOnline = ref(false)
const toast = ref(null)
const panelOpen = ref(false)
const activeTab = ref('info')

// Colors
const colors = {
  GREEN: '#22c55e',
  YELLOW: '#eab308',
  RED: '#ef4444',
  UNKNOWN: '#6b7280'
}

const statusText = {
  GREEN: 'Safe Area',
  YELLOW: 'Moderate Risk',
  RED: 'High Risk',
  UNKNOWN: 'No Data'
}

const quartLabels = {
  jour: 'Day',
  soir: 'Evening',
  nuit: 'Night'
}

const quartIcons = {
  jour: 'M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z',
  soir: 'M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z',
  nuit: 'M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z'
}

// Fallback safety data for all areas (used when API doesn't have data)
// Fallback safety data for all areas (used when API doesn't have data)
const fallbackData = {
  'Pierrefonds--Roxboro': { score: 78, colour: 'GREEN', risk_crime: 450.5, risk_acc: 120.2, crimes: 45, accidents: 12 },
  'Cote-des-Neiges--Notre-Dame-de-Grace': { score: 52, colour: 'YELLOW', risk_crime: 850.0, risk_acc: 300.5, crimes: 150, accidents: 45 },
  'Ahuntsic-Cartierville': { score: 57, colour: 'YELLOW', risk_crime: 780.2, risk_acc: 250.0, crimes: 120, accidents: 35 },
  'Outremont': { score: 89, colour: 'GREEN', risk_crime: 200.0, risk_acc: 50.0, crimes: 15, accidents: 5 },
  'Plateau-Mont-Royal': { score: 45, colour: 'YELLOW', risk_crime: 950.5, risk_acc: 400.0, crimes: 200, accidents: 60 },
  'LaSalle': { score: 71, colour: 'GREEN', risk_crime: 550.0, risk_acc: 180.0, crimes: 60, accidents: 20 },
  'Pointe-aux-Trembles-Rivieres-des-Prairies': { score: 65, colour: 'YELLOW', risk_crime: 620.0, risk_acc: 210.0, crimes: 75, accidents: 25 },
  'Rosemont--La-Petite-Patrie': { score: 48, colour: 'YELLOW', risk_crime: 900.0, risk_acc: 350.0, crimes: 180, accidents: 50 },
  'Ville-Marie': { score: 38, colour: 'RED', risk_crime: 1200.0, risk_acc: 600.0, crimes: 350, accidents: 100 },
  'Anjou': { score: 74, colour: 'GREEN', risk_crime: 480.0, risk_acc: 150.0, crimes: 50, accidents: 15 },
  'Montreal-Nord': { score: 35, colour: 'RED', risk_crime: 1100.0, risk_acc: 500.0, crimes: 300, accidents: 80 },
  'Lachine': { score: 68, colour: 'YELLOW', risk_crime: 580.0, risk_acc: 190.0, crimes: 65, accidents: 22 },
  'Mercier-Hochelaga-Maisonneuve': { score: 42, colour: 'YELLOW', risk_crime: 980.0, risk_acc: 420.0, crimes: 220, accidents: 70 },
  'Saint-Laurent': { score: 72, colour: 'GREEN', risk_crime: 520.0, risk_acc: 160.0, crimes: 55, accidents: 18 },
  'St-Leonard': { score: 77, colour: 'GREEN', risk_crime: 460.0, risk_acc: 130.0, crimes: 48, accidents: 14 },
  'Villeray-Saint-Michel-Parc-Extension': { score: 49, colour: 'YELLOW', risk_crime: 880.0, risk_acc: 320.0, crimes: 160, accidents: 40 },
  'Sud-Ouest': { score: 62, colour: 'YELLOW', risk_crime: 650.0, risk_acc: 230.0, crimes: 85, accidents: 28 },
  "L'Ile-Bizard--Sainte-Genevieve": { score: 91, colour: 'GREEN', risk_crime: 150.0, risk_acc: 40.0, crimes: 10, accidents: 3 },
  'Verdun--Ile-des-Soeurs': { score: 69, colour: 'YELLOW', risk_crime: 560.0, risk_acc: 185.0, crimes: 62, accidents: 21 },
  'Dollard-des-Ormeaux': { score: 88, colour: 'GREEN', risk_crime: 220.0, risk_acc: 60.0, crimes: 18, accidents: 6 },
  'Dorval': { score: 82, colour: 'GREEN', risk_crime: 350.0, risk_acc: 100.0, crimes: 30, accidents: 10 },
  'Pointe-Claire': { score: 86, colour: 'GREEN', risk_crime: 280.0, risk_acc: 80.0, crimes: 25, accidents: 8 },
  'Kirkland': { score: 92, colour: 'GREEN', risk_crime: 140.0, risk_acc: 35.0, crimes: 8, accidents: 2 },
  'Beaconsfield': { score: 94, colour: 'GREEN', risk_crime: 120.0, risk_acc: 30.0, crimes: 6, accidents: 2 },
  "Baie-d'Urfe": { score: 96, colour: 'GREEN', risk_crime: 100.0, risk_acc: 25.0, crimes: 5, accidents: 1 },
  'Sainte-Anne-de-Bellevue': { score: 90, colour: 'GREEN', risk_crime: 180.0, risk_acc: 55.0, crimes: 12, accidents: 4 },
  'Senneville': { score: 95, colour: 'GREEN', risk_crime: 110.0, risk_acc: 28.0, crimes: 5, accidents: 1 },
  'Westmount': { score: 93, colour: 'GREEN', risk_crime: 130.0, risk_acc: 32.0, crimes: 7, accidents: 2 },
  'Cote-Saint-Luc': { score: 85, colour: 'GREEN', risk_crime: 300.0, risk_acc: 90.0, crimes: 28, accidents: 9 },
  'Hampstead': { score: 91, colour: 'GREEN', risk_crime: 160.0, risk_acc: 45.0, crimes: 9, accidents: 3 },
  'Montreal-Ouest': { score: 87, colour: 'GREEN', risk_crime: 260.0, risk_acc: 75.0, crimes: 22, accidents: 7 },
  'Mont-Royal': { score: 89, colour: 'GREEN', risk_crime: 210.0, risk_acc: 58.0, crimes: 16, accidents: 5 }
}

// Methods
function showToast(message, isError = false) {
  toast.value = { message, isError }
  setTimeout(() => toast.value = null, 3000)
}

function getAreaForBorough(boroughName) {
  console.log('getAreaForBorough called with:', boroughName)
  // First try to find from API data
  const apiArea = findMatchingArea(boroughName, areas.value)
  console.log('findMatchingArea returned:', apiArea)
  if (apiArea) return apiArea

  // Fall back to local data
  const fb = fallbackData[boroughName]
  if (fb) {
    return {
      id: `BOROUGH#${boroughName}`,
      areaName: boroughName.replace(/--/g, ' - '),
      borough_code: boroughName,
      safetyScore: fb.score,
      colour: fb.colour,
      latestPeriod: '202510',
      quart: selectedQuart.value,
      risk_crime: fb.risk_crime,
      risk_acc: fb.risk_acc,
      numIncidentsCrime: fb.crimes,
      numIncidentsAccidents: fb.accidents
    }
  }
  return null
}

function styleFeature(feature) {
  const name = feature.properties.name
  const area = getAreaForBorough(name)
  const color = area ? (colors[area.colour] || colors.UNKNOWN) : colors.UNKNOWN
  const isSelected = selectedArea.value &&
    normalizeString(selectedArea.value.areaName || '') === normalizeString(name)

  return {
    fillColor: color,
    fillOpacity: isSelected ? 0.75 : 0.45,
    color: isSelected ? color : '#1f2937',
    weight: isSelected ? 4 : 1.5,
    opacity: 1
  }
}

function highlightFeature(e) {
  const layer = e.target
  const feature = layer.feature
  const area = getAreaForBorough(feature.properties.name)
  const color = area ? (colors[area.colour] || colors.UNKNOWN) : colors.UNKNOWN
  layer.setStyle({
    fillOpacity: 0.65,
    weight: 3,
    color: color
  })
  layer.bringToFront()
}

function resetHighlight(e) {
  geoJsonLayer.value.resetStyle(e.target)
}

function onEachFeature(feature, layer) {
  const name = feature.properties.name
  const area = getAreaForBorough(name)
  const score = area ? Math.round(area.safetyScore) : '?'
  const displayName = name.replace(/--/g, ' - ').replace(/-/g, ' ')

  layer.bindTooltip(`
    <div class="tooltip-content">
      <div class="tooltip-name">${displayName}</div>
      <div class="tooltip-score">Safety Score: <strong>${score}</strong>/100</div>
    </div>
  `, {
    sticky: true,
    className: 'custom-tooltip',
    direction: 'top',
    offset: [0, -10]
  })

  layer.on({
    click: () => selectBorough(name),
    mouseover: highlightFeature,
    mouseout: resetHighlight
  })
}

async function selectBorough(boroughName) {
  console.log('Selecting borough:', boroughName)
  const area = getAreaForBorough(boroughName)
  console.log('Found area:', area)
  panelOpen.value = true

  if (area) {
    selectedArea.value = area
    await loadAdvice(area.id)
  } else {
    selectedArea.value = {
      id: geoNameToApiId(boroughName),
      areaName: boroughName.replace(/--/g, ' - '),
      borough_code: boroughName,
      safetyScore: null,
      colour: 'UNKNOWN',
      latestPeriod: 'N/A',
      quart: selectedQuart.value
    }
    advice.value = null
  }

  if (geoJsonLayer.value) {
    geoJsonLayer.value.setStyle(styleFeature)
  }
}

async function loadAdvice(areaId) {
  adviceLoading.value = true
  advice.value = null
  try {
    const result = await fetchAdvice(areaId, selectedQuart.value)
    if (result) {
      advice.value = result
      selectedArea.value = {
        id: result.id,
        areaName: result.areaName,
        borough_code: result.borough_code,
        safetyScore: result.safetyScore,
        colour: result.colour,
        latestPeriod: result.period,
        latestPeriod: result.period,
        quart: result.quart,
        risk_crime: result.risk_crime,
        risk_acc: result.risk_acc,
        numIncidentsCrime: result.numIncidentsCrime,
        numIncidentsAccidents: result.numIncidentsAccidents
      }
    }
  } catch (e) {
    console.error('Failed to load advice:', e)
    // Fallback advice
    if (selectedArea.value) {
      const riskLevel = selectedArea.value.colour === 'GREEN' ? 'low' : (selectedArea.value.colour === 'YELLOW' ? 'moderate' : 'elevated')
      advice.value = {
        advice: `AI Safety Analysis (Estimated): ${selectedArea.value.areaName} currently shows a ${riskLevel} risk profile. Local data suggests ${selectedArea.value.risk_crime > 500 ? 'some attention to personal property is warranted' : 'conditions are generally safe'}. As always, maintain awareness of your surroundings.`
      }
    }
  } finally {
    adviceLoading.value = false
  }
}

function addToRoute() {
  if (!selectedArea.value) return
  if (routeAreas.value.length >= 5) {
    showToast('Maximum 5 areas allowed', true)
    return
  }
  if (routeAreas.value.find(a => a.id === selectedArea.value.id)) {
    showToast('Area already in route', true)
    return
  }
  routeAreas.value.push({ ...selectedArea.value })
  routeResult.value = null
  showToast('Added to route')
}

function removeFromRoute(id) {
  routeAreas.value = routeAreas.value.filter(a => a.id !== id)
  routeResult.value = null
}

function clearRoute() {
  routeAreas.value = []
  routeResult.value = null
}

async function runRouteAnalysis() {
  if (routeAreas.value.length < 2) {
    showToast('Need at least 2 areas', true)
    return
  }
  routeLoading.value = true
  routeResult.value = null
  try {
    const areaIds = routeAreas.value.map(a => a.id)
    const result = await analyzeRoute(areaIds, selectedQuart.value)
    if (result) {
      routeResult.value = result
    }
  } catch (e) {
    console.error('Route analysis failed:', e)
    // Fallback route advice
    routeResult.value = {
      routeAdvice: "Route Safety Analysis (Estimated): This route traverses areas with varying safety profiles. We recommend sticking to main thoroughfares and well-lit streets, especially during evening hours. Monitor your surroundings when transitioning between boroughs."
    }
  } finally {
    routeLoading.value = false
  }
}

async function submitQuestion() {
  if (!askQuery.value.trim()) return
  askLoading.value = true
  askResponse.value = null
  try {
    const areaIds = selectedArea.value ? [selectedArea.value.id] : null
    const result = await askQuestion(askQuery.value, areaIds, selectedQuart.value)
    if (result) {
      askResponse.value = result
    }
  } catch (e) {
    console.error('Failed to get answer:', e)
    // Fallback answer
    const areaName = selectedArea.value?.areaName || 'this area'
    askResponse.value = {
      answer: `AI Response (Estimated): regarding ${areaName}, it is generally safe to walk, but we recommend staying on well-lit streets and being aware of your surroundings. Public transport is generally safe, but exercise caution during late night hours.`
    }
  } finally {
    askLoading.value = false
  }
}

async function loadAreas() {
  const data = await fetchAreas(selectedQuart.value)
  areas.value = data
  if (geoJsonLayer.value) {
    geoJsonLayer.value.setStyle(styleFeature)
  }
}

async function loadGeoJson() {
  try {
    const response = await fetch('/montreal-boroughs.geojson')
    boroughsGeoJson.value = await response.json()
  } catch (e) {
    showToast('Failed to load map data', true)
  }
}

function initMap() {
  map.value = L.map('map', {
    zoomControl: false,
    attributionControl: false
  }).setView([45.55, -73.65], 11)

  L.control.zoom({ position: 'bottomright' }).addTo(map.value)

  L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
    maxZoom: 19,
    subdomains: 'abcd'
  }).addTo(map.value)

  if (boroughsGeoJson.value) {
    geoJsonLayer.value = L.geoJSON(boroughsGeoJson.value, {
      style: styleFeature,
      onEachFeature: onEachFeature
    }).addTo(map.value)

    map.value.fitBounds(geoJsonLayer.value.getBounds(), {
      padding: [50, 50],
      maxZoom: 12
    })
  }
}

function closePanel() {
  panelOpen.value = false
  selectedArea.value = null
  if (geoJsonLayer.value) {
    geoJsonLayer.value.setStyle(styleFeature)
  }
}

watch(selectedQuart, async () => {
  await loadAreas()
  routeResult.value = null
  askResponse.value = null
  if (selectedArea.value && selectedArea.value.colour !== 'UNKNOWN') {
    await loadAdvice(selectedArea.value.id)
  }
})

onMounted(async () => {
  apiOnline.value = await checkHealth()
  await loadGeoJson()
  await loadAreas()
  initMap()
})
</script>

<template>
  <div class="app">
    <div id="map"></div>

    <!-- Top Bar -->
    <div class="top-bar">
      <div class="brand">
        <div class="brand-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/>
          </svg>
        </div>
        <div class="brand-text">
          <span class="brand-name">TravelSafe</span>
          <span class="brand-location">Montreal</span>
        </div>
      </div>

      <div class="time-toggle">
        <button
          v-for="(label, key) in quartLabels"
          :key="key"
          :class="['time-btn', { active: selectedQuart === key }]"
          @click="selectedQuart = key"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path :d="quartIcons[key]"/>
          </svg>
          {{ label }}
        </button>
      </div>

      <div class="status-badge" :class="{ online: apiOnline }">
        <span class="status-dot"></span>
        {{ apiOnline ? 'Connected' : 'Offline' }}
      </div>
    </div>

    <!-- Legend -->
    <div class="legend">
      <div class="legend-title">Safety Level</div>
      <div class="legend-items">
        <div class="legend-item">
          <span class="legend-color green"></span>
          <span>Safe</span>
        </div>
        <div class="legend-item">
          <span class="legend-color yellow"></span>
          <span>Moderate</span>
        </div>
        <div class="legend-item">
          <span class="legend-color red"></span>
          <span>Caution</span>
        </div>
        <div class="legend-item">
          <span class="legend-color gray"></span>
          <span>No Data</span>
        </div>
      </div>
    </div>

    <!-- Info Panel -->
    <transition name="slide">
      <div v-if="panelOpen && selectedArea" class="info-panel">
        <button class="close-btn" @click="closePanel">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M6 18L18 6M6 6l12 12"/>
          </svg>
        </button>

        <div class="panel-header">
          <h2>{{ selectedArea.areaName?.replace(/--/g, ' - ') || 'Unknown Area' }}</h2>
          <span class="panel-time">{{ quartLabels[selectedQuart] }} Analysis</span>
        </div>

        <div class="score-display">
          <div :class="['score-circle', selectedArea.colour?.toLowerCase()]">
            <span class="score-number">{{ selectedArea.safetyScore != null ? Math.round(selectedArea.safetyScore) : '?' }}</span>
            <span class="score-max">/100</span>
          </div>
          <div class="score-details">
            <div :class="['score-status', selectedArea.colour?.toLowerCase()]">
              {{ statusText[selectedArea.colour] || 'Unknown' }}
            </div>
            <div class="score-period">Data: {{ selectedArea.latestPeriod || 'N/A' }}</div>
          </div>
        </div>



        <div class="tabs">
          <button :class="['tab', { active: activeTab === 'info' }]" @click="activeTab = 'info'">Advice</button>
          <button :class="['tab', { active: activeTab === 'ask' }]" @click="activeTab = 'ask'">Ask AI</button>
          <button :class="['tab', { active: activeTab === 'route' }]" @click="activeTab = 'route'">Route</button>
        </div>

        <div class="tab-content">
          <div v-if="activeTab === 'info'" class="tab-pane">
            <div v-if="adviceLoading" class="loading-state">
              <div class="spinner"></div>
              <span>Getting AI advice...</span>
            </div>
            <div v-else-if="advice" class="advice-content">
              <p>{{ advice.advice }}</p>
            </div>
            <div v-else class="empty-advice">
              <p>No safety advice available for this area.</p>
            </div>
          </div>

          <div v-if="activeTab === 'ask'" class="tab-pane">
            <div class="ask-section">
              <div class="input-group">
                <input v-model="askQuery" type="text" placeholder="Ask about safety in this area..." @keyup.enter="submitQuestion"/>
                <button @click="submitQuestion" :disabled="askLoading || !askQuery.trim()">
                  <span v-if="askLoading" class="spinner small"></span>
                  <svg v-else viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M14 5l7 7m0 0l-7 7m7-7H3"/>
                  </svg>
                </button>
              </div>
              <div v-if="askResponse" class="ai-response">
                <div class="response-header">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"/>
                  </svg>
                  AI Response
                </div>
                <p>{{ askResponse.answer }}</p>
              </div>
            </div>
          </div>

          <div v-if="activeTab === 'route'" class="tab-pane">
            <div class="route-section">
              <button class="add-route-btn" @click="addToRoute" :disabled="!selectedArea">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M12 4v16m8-8H4"/>
                </svg>
                Add {{ selectedArea?.areaName?.substring(0, 15) || 'Area' }} to Route
              </button>

              <div v-if="routeAreas.length > 0" class="route-list">
                <div class="route-path">
                  <div v-for="(area, idx) in routeAreas" :key="area.id" class="route-stop">
                    <div class="stop-marker" :class="area.colour?.toLowerCase()">{{ idx + 1 }}</div>
                    <div class="stop-info">
                      <span class="stop-name">{{ area.areaName?.substring(0, 20) }}</span>
                      <span class="stop-score">Score: {{ Math.round(area.safetyScore) || '?' }}</span>
                    </div>
                    <button class="remove-stop" @click="removeFromRoute(area.id)">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M6 18L18 6M6 6l12 12"/>
                      </svg>
                    </button>
                  </div>
                </div>

                <div class="route-actions">
                  <button class="analyze-btn" @click="runRouteAnalysis" :disabled="routeLoading || routeAreas.length < 2">
                    <span v-if="routeLoading" class="spinner small"></span>
                    <span v-else>Analyze Route Safety</span>
                  </button>
                  <button class="clear-btn" @click="clearRoute">Clear</button>
                </div>
              </div>

              <div v-if="routeResult" class="route-result">
                <div class="result-header">Route Analysis</div>
                <p>{{ routeResult.routeAdvice }}</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </transition>

    <!-- Welcome Card -->
    <transition name="fade">
      <div v-if="!panelOpen" class="welcome-card">
        <div class="welcome-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M17.657 16.657L13.414 20.9a1.998 1.998 0 01-2.827 0l-4.244-4.243a8 8 0 1111.314 0z"/>
            <path d="M15 11a3 3 0 11-6 0 3 3 0 016 0z"/>
          </svg>
        </div>
        <h3>Select a Neighborhood</h3>
        <p>Click any area on the map to view safety scores and AI-powered recommendations.</p>
      </div>
    </transition>

    <!-- Toast -->
    <transition name="toast">
      <div v-if="toast" :class="['toast', { error: toast.isError }]">
        {{ toast.message }}
      </div>
    </transition>
  </div>
</template>

<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

body {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: #0f172a;
  overflow: hidden;
}

.app {
  width: 100vw;
  height: 100vh;
  position: relative;
  overflow: hidden;
}

#map {
  position: absolute;
  inset: 0;
  z-index: 1;
}

/* Top Bar */
.top-bar {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  z-index: 1000;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 24px;
  background: linear-gradient(to bottom, rgba(15, 23, 42, 0.95), rgba(15, 23, 42, 0));
  pointer-events: none;
}

.top-bar > * { pointer-events: auto; }

.brand {
  display: flex;
  align-items: center;
  gap: 12px;
}

.brand-icon {
  width: 44px;
  height: 44px;
  background: linear-gradient(135deg, #22c55e, #16a34a);
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  box-shadow: 0 4px 12px rgba(34, 197, 94, 0.3);
}

.brand-icon svg {
  width: 24px;
  height: 24px;
  color: white;
}

.brand-text {
  display: flex;
  flex-direction: column;
}

.brand-name {
  font-size: 20px;
  font-weight: 700;
  color: white;
  letter-spacing: -0.5px;
}

.brand-location {
  font-size: 12px;
  color: #94a3b8;
  font-weight: 500;
}

.time-toggle {
  display: flex;
  background: rgba(30, 41, 59, 0.9);
  border-radius: 12px;
  padding: 4px;
  backdrop-filter: blur(12px);
  border: 1px solid rgba(255, 255, 255, 0.1);
}

.time-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 10px 16px;
  border: none;
  background: transparent;
  color: #94a3b8;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  border-radius: 8px;
  transition: all 0.2s;
}

.time-btn svg {
  width: 18px;
  height: 18px;
}

.time-btn:hover { color: white; }

.time-btn.active {
  background: #3b82f6;
  color: white;
  box-shadow: 0 2px 8px rgba(59, 130, 246, 0.4);
}

.status-badge {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 16px;
  background: rgba(30, 41, 59, 0.9);
  border-radius: 20px;
  font-size: 13px;
  color: #94a3b8;
  backdrop-filter: blur(12px);
  border: 1px solid rgba(255, 255, 255, 0.1);
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #ef4444;
}

.status-badge.online .status-dot {
  background: #22c55e;
  box-shadow: 0 0 8px rgba(34, 197, 94, 0.6);
}

.status-badge.online { color: #22c55e; }

/* Legend */
.legend {
  position: absolute;
  bottom: 32px;
  left: 24px;
  z-index: 1000;
  background: rgba(15, 23, 42, 0.95);
  border-radius: 16px;
  padding: 16px 20px;
  backdrop-filter: blur(12px);
  border: 1px solid rgba(255, 255, 255, 0.1);
}

.legend-title {
  font-size: 11px;
  font-weight: 600;
  color: #64748b;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 12px;
}

.legend-items {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 13px;
  color: #e2e8f0;
}

.legend-color {
  width: 16px;
  height: 16px;
  border-radius: 4px;
}

.legend-color.green { background: #22c55e; }
.legend-color.yellow { background: #eab308; }
.legend-color.red { background: #ef4444; }
.legend-color.gray { background: #6b7280; }

/* Info Panel */
.info-panel {
  position: absolute;
  top: 16px;
  right: 16px;
  bottom: 16px;
  width: 400px;
  z-index: 1000;
  background: rgba(15, 23, 42, 0.98);
  border-radius: 24px;
  backdrop-filter: blur(20px);
  border: 1px solid rgba(255, 255, 255, 0.1);
  display: flex;
  flex-direction: column;
  overflow: hidden;
  box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
}



.info-panel * {
  border-top: none !important;
  border-bottom: none !important;
}

.close-btn {
  position: absolute;
  top: 16px;
  right: 16px;
  width: 32px;
  height: 32px;
  border: none;
  background: rgba(255, 255, 255, 0.1);
  border-radius: 8px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s;
  z-index: 10;
}

.close-btn svg {
  width: 18px;
  height: 18px;
  color: #94a3b8;
}

.close-btn:hover { background: rgba(255, 255, 255, 0.2); }
.close-btn:hover svg { color: white; }

.panel-header { padding: 24px 24px 16px; }

.panel-header h2 {
  font-size: 22px;
  font-weight: 700;
  color: #ffffff !important;
  margin: 0 0 4px 0;
  padding-right: 40px;
}

.panel-time {
  font-size: 13px;
  color: #64748b;
}

/* Score Display */
.score-display {
  display: flex;
  align-items: center;
  gap: 20px;
  padding: 0 24px 24px;
}

.score-circle {
  width: 88px;
  height: 88px;
  border-radius: 50%;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  background: #22c55e;
  box-shadow: 0 8px 24px rgba(34, 197, 94, 0.3);
}

.score-circle.yellow {
  background: #eab308;
  box-shadow: 0 8px 24px rgba(234, 179, 8, 0.3);
}

.score-circle.red {
  background: #ef4444;
  box-shadow: 0 8px 24px rgba(239, 68, 68, 0.3);
}

.score-circle.unknown {
  background: #6b7280;
  box-shadow: 0 8px 24px rgba(107, 114, 128, 0.3);
}

.score-number {
  font-size: 32px;
  font-weight: 700;
  color: white;
  line-height: 1;
}

.score-max {
  font-size: 12px;
  color: rgba(255, 255, 255, 0.7);
}

.score-details { flex: 1; }

.score-status {
  font-size: 18px;
  font-weight: 600;
  color: #22c55e;
  margin-bottom: 4px;
}

.score-status.yellow { color: #eab308; }
.score-status.red { color: #ef4444; }
.score-status.unknown { color: #6b7280; }

.score-period {
  font-size: 13px;
  color: #64748b;
}

/* Tabs */
.tabs {
  display: flex;
  padding: 0 24px;
  background: rgba(0, 0, 0, 0.2);
  border-radius: 12px;
  margin: 0 24px;
}

.tab {
  flex: 1;
  padding: 12px 0;
  border: none;
  background: transparent;
  color: #64748b;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  border-radius: 8px;
  margin: 4px;
}

.tab:hover {
  color: #94a3b8;
  background: rgba(255, 255, 255, 0.05);
}

.tab.active {
  color: white;
  background: #3b82f6;
}

.tab-content {
  flex: 1;
  overflow-y: auto;
  padding: 20px 24px;
}

.tab-pane {
  animation: fadeIn 0.2s ease;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(8px); }
  to { opacity: 1; transform: translateY(0); }
}

/* Loading State */
.loading-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 40px 0;
  gap: 12px;
  color: #64748b;
}

.spinner {
  width: 32px;
  height: 32px;
  border: 3px solid rgba(59, 130, 246, 0.2);
  border-top-color: #3b82f6;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

.spinner.small {
  width: 18px;
  height: 18px;
  border-width: 2px;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

/* Advice Content */
.advice-content {
  background: rgba(34, 197, 94, 0.1);
  border: 1px solid rgba(34, 197, 94, 0.2) !important;
  border-radius: 12px;
  padding: 16px;
}

.advice-content p {
  color: #e2e8f0;
  font-size: 14px;
  line-height: 1.7;
}

.empty-advice {
  text-align: center;
  padding: 32px 16px;
  color: #64748b;
}

/* Ask Section */
.ask-section {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.input-group {
  display: flex;
  gap: 8px;
}

.input-group input {
  flex: 1;
  padding: 12px 16px;
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  border-radius: 12px;
  color: white;
  font-size: 14px;
  transition: all 0.2s;
}

.input-group input::placeholder { color: #64748b; }

.input-group input:focus {
  outline: none;
  border-color: #3b82f6 !important;
  background: rgba(59, 130, 246, 0.1);
}

.input-group button {
  width: 48px;
  height: 48px;
  border: none;
  background: #3b82f6;
  border-radius: 12px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s;
}

.input-group button svg {
  width: 20px;
  height: 20px;
  color: white;
}

.input-group button:hover { background: #2563eb; }
.input-group button:disabled { background: #374151; cursor: not-allowed; }

.ai-response {
  background: rgba(59, 130, 246, 0.1);
  border: 1px solid rgba(59, 130, 246, 0.2) !important;
  border-radius: 12px;
  padding: 16px;
}

.response-header {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  font-weight: 600;
  color: #3b82f6;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 12px;
}

.response-header svg {
  width: 16px;
  height: 16px;
}

.ai-response p {
  color: #e2e8f0;
  font-size: 14px;
  line-height: 1.7;
}

/* Route Section */
.route-section {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.add-route-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 14px;
  background: rgba(255, 255, 255, 0.05);
  border: 1px dashed rgba(255, 255, 255, 0.2) !important;
  border-radius: 12px;
  color: #94a3b8;
  font-size: 14px;
  cursor: pointer;
  transition: all 0.2s;
}

.add-route-btn svg {
  width: 20px;
  height: 20px;
}

.add-route-btn:hover {
  background: rgba(255, 255, 255, 0.1);
  border-color: rgba(255, 255, 255, 0.3) !important;
  color: white;
}

.add-route-btn:disabled { opacity: 0.5; cursor: not-allowed; }

.route-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.route-path {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.route-stop {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px;
  background: rgba(255, 255, 255, 0.05);
  border-radius: 10px;
}

.stop-marker {
  width: 28px;
  height: 28px;
  border-radius: 50%;
  background: #22c55e;
  color: white;
  font-size: 12px;
  font-weight: 600;
  display: flex;
  align-items: center;
  justify-content: center;
}

.stop-marker.yellow { background: #eab308; }
.stop-marker.red { background: #ef4444; }
.stop-marker.unknown { background: #6b7280; }

.stop-info {
  flex: 1;
  display: flex;
  flex-direction: column;
}

.stop-name {
  color: white;
  font-size: 13px;
  font-weight: 500;
}

.stop-score {
  color: #64748b;
  font-size: 12px;
}

.remove-stop {
  width: 28px;
  height: 28px;
  border: none;
  background: rgba(239, 68, 68, 0.1);
  border-radius: 6px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s;
}

.remove-stop svg {
  width: 14px;
  height: 14px;
  color: #ef4444;
}

.remove-stop:hover { background: rgba(239, 68, 68, 0.2); }

.route-actions {
  display: flex;
  gap: 8px;
}

.analyze-btn {
  flex: 1;
  padding: 14px;
  background: #3b82f6;
  border: none;
  border-radius: 12px;
  color: white;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
}

.analyze-btn:hover { background: #2563eb; }
.analyze-btn:disabled { background: #374151; cursor: not-allowed; }

.clear-btn {
  padding: 14px 20px;
  background: transparent;
  border: 1px solid rgba(255, 255, 255, 0.2) !important;
  border-radius: 12px;
  color: #94a3b8;
  font-size: 14px;
  cursor: pointer;
  transition: all 0.2s;
}

.clear-btn:hover {
  border-color: rgba(255, 255, 255, 0.4) !important;
  color: white;
}

.route-result {
  background: #1e293b;
  border: 1px solid rgba(168, 85, 247, 0.2) !important;
  border-radius: 12px;
  padding: 16px;
}

.result-header {
  font-size: 12px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 12px;
}

.route-result p {
  font-size: 14px;
  line-height: 1.7;
}

/* Welcome Card */
.welcome-card {
  position: absolute;
  bottom: 120px;
  right: 24px;
  z-index: 1000;
  background: rgba(15, 23, 42, 0.95);
  border-radius: 16px;
  padding: 24px;
  backdrop-filter: blur(16px);
  border: 1px solid rgba(255, 255, 255, 0.1);
  max-width: 280px;
  text-align: center;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.borough-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-height: 300px;
  overflow-y: auto;
  padding-right: 4px;
}

.borough-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px;
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 8px;
  color: #e2e8f0;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s;
  text-align: left;
}

.borough-item:hover {
  background: rgba(255, 255, 255, 0.1);
  border-color: rgba(255, 255, 255, 0.2);
  color: white;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
}

.status-dot.green { background: #22c55e; box-shadow: 0 0 8px rgba(34, 197, 94, 0.4); }
.status-dot.yellow { background: #eab308; box-shadow: 0 0 8px rgba(234, 179, 8, 0.4); }
.status-dot.red { background: #ef4444; box-shadow: 0 0 8px rgba(239, 68, 68, 0.4); }
.status-dot.grey { background: #6b7280; }

.welcome-icon {
  width: 56px;
  height: 56px;
  margin: 0 auto 16px;
  background: linear-gradient(135deg, #3b82f6, #2563eb);
  border-radius: 14px;
  display: flex;
  align-items: center;
  justify-content: center;
  box-shadow: 0 8px 20px rgba(59, 130, 246, 0.3);
}

.welcome-icon svg {
  width: 28px;
  height: 28px;
  color: white;
}

.welcome-card h3 {
  font-size: 18px;
  font-weight: 600;
  color: white;
  margin-bottom: 8px;
}

.welcome-card p {
  font-size: 14px;
  color: #94a3b8;
  line-height: 1.6;
  margin: 0;
}

/* Toast */
.toast {
  position: fixed;
  bottom: 32px;
  left: 50%;
  transform: translateX(-50%);
  z-index: 9999;
  background: rgba(15, 23, 42, 0.95);
  color: white;
  padding: 14px 24px;
  border-radius: 12px;
  font-size: 14px;
  backdrop-filter: blur(12px);
  border: 1px solid rgba(255, 255, 255, 0.1);
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
}

.toast.error {
  background: rgba(239, 68, 68, 0.95);
  border-color: rgba(255, 255, 255, 0.2);
}

/* Transitions */
.slide-enter-active, .slide-leave-active { transition: all 0.3s ease; }
.slide-enter-from, .slide-leave-to { transform: translateX(100%); opacity: 0; }

.fade-enter-active, .fade-leave-active { transition: opacity 0.2s ease; }
.fade-enter-from, .fade-leave-to { opacity: 0; }

.toast-enter-active, .toast-leave-active { transition: all 0.3s ease; }
.toast-enter-from, .toast-leave-to { transform: translateX(-50%) translateY(20px); opacity: 0; }

/* Leaflet Tooltip */
.leaflet-tooltip.custom-tooltip {
  background: rgba(15, 23, 42, 0.95);
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 10px;
  padding: 10px 14px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.4);
  backdrop-filter: blur(12px);
}

.leaflet-tooltip.custom-tooltip::before { display: none; }

.tooltip-content { text-align: center; }

.tooltip-name {
  font-size: 13px;
  font-weight: 600;
  color: white;
  margin-bottom: 4px;
}

.tooltip-score {
  font-size: 12px;
  color: #94a3b8;
}

.tooltip-score strong { color: #22c55e; }

/* Leaflet overrides */
.leaflet-interactive:focus,
.leaflet-container:focus,
path.leaflet-interactive:focus {
  outline: none !important;
}

.leaflet-control-zoom {
  border: none !important;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3) !important;
  border-radius: 12px !important;
  overflow: hidden;
}

.leaflet-control-zoom a {
  background: rgba(15, 23, 42, 0.95) !important;
  color: white !important;
  border: none !important;
  width: 36px !important;
  height: 36px !important;
  line-height: 36px !important;
  font-size: 18px !important;
}

.leaflet-control-zoom a:hover {
  background: rgba(30, 41, 59, 0.95) !important;
}

.leaflet-control-zoom-in {
  border-bottom: 1px solid rgba(255, 255, 255, 0.1) !important;
}

/* Responsive */
@media (max-width: 768px) {
  .top-bar { padding: 12px 16px; }
  .brand-text { display: none; }
  .time-btn span:not(svg) { display: none; }
  .time-btn { padding: 10px 12px; }

  .info-panel {
    width: calc(100% - 32px);
    left: 16px;
    right: 16px;
    top: auto;
    bottom: 16px;
    max-height: 70vh;
    border-radius: 20px;
  }

  .legend {
    bottom: auto;
    top: 80px;
    left: 16px;
  }

  .welcome-card {
    right: 16px;
    left: 16px;
    max-width: none;
  }
}
</style>

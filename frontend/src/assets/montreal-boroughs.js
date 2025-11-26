// Borough name normalization utilities

// Explicit mapping for tricky names
const NAME_ALIASES = {
  'sudouest': 'lesudouest',
  'stleonard': 'stleonard',
  'saintleonard': 'stleonard',
  'montrealest': 'montrealest',
  'rosemontlapetitepatrie': 'rosemontlapetitepatrie'
}

// Normalize string for matching (remove accents, lowercase, remove special chars)
export function normalizeString(str) {
  let normalized = str
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')  // Remove accents
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')        // Remove special chars

  // Normalize common variations
  normalized = normalized
    .replace(/^le/, '')               // Remove leading "Le"
    .replace(/^la/, '')               // Remove leading "La"
    .replace(/saint/g, 'st')          // Normalize Saint -> St

  // Check aliases
  if (NAME_ALIASES[normalized]) {
    normalized = NAME_ALIASES[normalized]
  }

  return normalized
}

// Map from GeoJSON names to likely API borough codes
export function geoNameToApiId(geoName) {
  const cleanName = geoName.replace(/--/g, '-')
  return `BOROUGH#${cleanName}`
}

// Find best match between a GeoJSON borough name and API areas
export function findMatchingArea(geoName, areas) {
  if (!areas || areas.length === 0) return null

  const normalizedGeo = normalizeString(geoName)

  // Try exact normalized match first
  for (const area of areas) {
    const areaName = area.areaName || area.id.replace('BOROUGH#', '')
    if (normalizeString(areaName) === normalizedGeo) {
      return area
    }
  }

  // Try partial match (one contains the other)
  for (const area of areas) {
    const areaName = area.areaName || area.id.replace('BOROUGH#', '')
    const normalizedArea = normalizeString(areaName)
    if (normalizedGeo.includes(normalizedArea) || normalizedArea.includes(normalizedGeo)) {
      return area
    }
  }

  // Try matching significant segments (at least 5 chars)
  for (const area of areas) {
    const areaName = area.areaName || area.id.replace('BOROUGH#', '')
    const normalizedArea = normalizeString(areaName)
    for (let i = 0; i <= normalizedGeo.length - 5; i++) {
      const segment = normalizedGeo.substring(i, i + 5)
      if (segment.length >= 5 && normalizedArea.includes(segment)) {
        return area
      }
    }
  }

  return null
}

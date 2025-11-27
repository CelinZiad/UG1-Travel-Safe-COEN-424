// Borough name normalization utilities

// Explicit mapping for tricky names
const NAME_ALIASES = {
  'lesudouest': 'sudouest',
  'rosemontlapetitepatrie': 'rosemontlapetitepatrie', // Map API's single hyphen/space to double hyphen normalized
  'saintleonard': 'stleonard',
  'montrealest': 'montrealest',
  'lilesoeurs': 'verduniledessoeurs',
  'iledessoeurs': 'verduniledessoeurs',
  'verdun': 'verduniledessoeurs',
  'tetreaultville': 'mercierhochelagamaisonneuve',
  'mercier': 'mercierhochelagamaisonneuve',
  'hochelaga': 'mercierhochelagamaisonneuve',
  'maisonneuve': 'mercierhochelagamaisonneuve',
  'longuepointe': 'mercierhochelagamaisonneuve',
  'nouveaubordeaux': 'ahuntsiccartierville',
  'cartierville': 'ahuntsiccartierville',
  'ahuntsic': 'ahuntsiccartierville',
  'saintmichel': 'villeraysaintmichelparcextension',
  'parcextension': 'villeraysaintmichelparcextension',
  'villeray': 'villeraysaintmichelparcextension',
  'cotedesneiges': 'cotedesneigesnotredamedegrace',
  'notredamedegrace': 'cotedesneigesnotredamedegrace',
  'ndg': 'cotedesneigesnotredamedegrace',
  'rivieredespraries': 'pointeauxtremblesrivieresdesprairies',
  'pointeauxtrembles': 'pointeauxtremblesrivieresdesprairies',
  'rdp': 'pointeauxtremblesrivieresdesprairies',
  'pat': 'pointeauxtremblesrivieresdesprairies',
  'ilebizard': 'lilebizardsaintegenevieve',
  'saintegenevieve': 'lilebizardsaintegenevieve',
  'pierrefonds': 'pierrefondsroxboro',
  'roxboro': 'pierrefondsroxboro'
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
  console.log(`Matching '${geoName}' (normalized: '${normalizedGeo}') against ${areas.length} areas`)

  // Try exact normalized match first
  for (const area of areas) {
    const areaName = area.areaName || area.id.replace('BOROUGH#', '')
    if (normalizeString(areaName) === normalizedGeo) {
      console.log('Exact match found:', areaName)
      return area
    }
  }

  // Try partial match (one contains the other)
  for (const area of areas) {
    const areaName = area.areaName || area.id.replace('BOROUGH#', '')
    const normalizedArea = normalizeString(areaName)
    if (normalizedGeo.includes(normalizedArea) || normalizedArea.includes(normalizedGeo)) {
      console.log('Partial match found:', areaName)
      return area
    }
  }

  // Segment matching removed as it causes false positives (e.g. Montreal-Nord -> Montreal-Est)
  return null
}

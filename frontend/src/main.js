import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import * as pmtiles from 'pmtiles';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const PMTILES_URL =
  'https://pub-6f1e54035ac14471852f4b7a25bf8354.r2.dev/rivers.pmtiles';
const API_BASE = '/forecast'; // proxied to backend via Vite
const PROVIDER = 'geoglows';

// Severity → colour mapping (matches legend)
const SEVERITY_COLORS = {
  0: '#1a9850', // normal / below RP-2
  1: '#fee08b', // RP 2
  2: '#fdae61', // RP 5
  3: '#f46d43', // RP 10
  4: '#d73027', // RP 25
  5: '#a50026', // RP 50
  6: '#67001f', // RP 100
};

const SEVERITY_WIDTHS = {
  0: 1,
  1: 2,
  2: 2.5,
  3: 3,
  4: 3.5,
  5: 4,
  6: 5,
};

// Band labels for info panel
const BAND_LABELS = {
  unknown: 'Unknown',
  below_2: 'Normal',
  '2': '2-year',
  '5': '5-year',
  '10': '10-year',
  '25': '25-year',
  '50': '50-year',
  '100': '100-year',
};

// Zoom → minimum severity threshold and per-request limit
// At global zoom only show the most extreme; as user zooms in, reveal more
const ZOOM_SEVERITY_TIERS = [
  { maxZoom: 3, minSeverity: 6, limit: 10000 },
  { maxZoom: 5, minSeverity: 5, limit: 15000 },
  { maxZoom: 7, minSeverity: 4, limit: 20000 },
  { maxZoom: 9, minSeverity: 3, limit: 30000 },
  { maxZoom: 11, minSeverity: 2, limit: 40000 },
  { maxZoom: Infinity, minSeverity: 1, limit: 50000 },
];

function getTierForZoom(zoom) {
  for (const tier of ZOOM_SEVERITY_TIERS) {
    if (zoom <= tier.maxZoom) return tier;
  }
  return ZOOM_SEVERITY_TIERS[ZOOM_SEVERITY_TIERS.length - 1];
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let forecastIndex = {}; // provider_reach_id → { severity_score, return_period_band, ... }
let currentRunId = null;
let currentTier = null; // track which tier is loaded to avoid redundant fetches
let map;
let loadingAbort = null; // AbortController for in-flight requests

const statusBar = document.getElementById('status-bar');
const infoPanel = document.getElementById('info-panel');
const infoContent = document.getElementById('info-content');

function setStatus(msg) {
  statusBar.textContent = msg;
}

// ---------------------------------------------------------------------------
// Forecast API helpers
// ---------------------------------------------------------------------------
async function fetchJSON(url, signal) {
  const res = await fetch(url, signal ? { signal } : undefined);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function loadRunId() {
  const run = await fetchJSON(
    `${API_BASE}/runs/latest?provider=${PROVIDER}`
  );
  currentRunId = run.run_id;
}

async function loadForecastSummaries(minSeverity, limit, signal) {
  const resp = await fetchJSON(
    `${API_BASE}/map/reaches?provider=${PROVIDER}&run_id=${currentRunId}&flagged_only=true&min_severity_score=${minSeverity}&limit=${limit}`,
    signal
  );
  return resp.data || [];
}

// ---------------------------------------------------------------------------
// Load data for current zoom level
// ---------------------------------------------------------------------------
async function loadDataForZoom(zoom) {
  const tier = getTierForZoom(zoom);

  // Skip if we already have this tier (or a more detailed one) loaded
  if (currentTier && tier.minSeverity >= currentTier.minSeverity) return;

  // Cancel any in-flight request
  if (loadingAbort) loadingAbort.abort();
  loadingAbort = new AbortController();

  setStatus(`Loading severity ≥ ${tier.minSeverity} reaches…`);

  try {
    const reaches = await loadForecastSummaries(
      tier.minSeverity,
      tier.limit,
      loadingAbort.signal
    );

    // Merge new reaches into existing index (don't lose higher-severity data)
    for (const r of reaches) {
      forecastIndex[String(r.provider_reach_id)] = r;
    }

    currentTier = tier;
    setStatus(
      `Run ${currentRunId} – ${Object.keys(forecastIndex).length} reaches loaded (severity ≥ ${tier.minSeverity})`
    );

    // Rebuild the highlighted layer
    updateHighlightedLayer();
  } catch (err) {
    if (err.name === 'AbortError') return; // superseded by a newer request
    console.warn('Could not load forecast summaries:', err);
    setStatus('Error loading forecast data');
  }
}

// ---------------------------------------------------------------------------
// Map setup
// ---------------------------------------------------------------------------
async function initMap() {
  // Register PMTiles protocol
  const protocol = new pmtiles.Protocol();
  maplibregl.addProtocol('pmtiles', protocol.tile);

  // Read PMTiles header for maxzoom
  let riversMaxZoom = 14;
  try {
    const archive = new pmtiles.PMTiles(PMTILES_URL);
    const header = await archive.getHeader();
    riversMaxZoom = header.maxZoom || 14;
  } catch (e) {
    console.warn('Could not read PMTiles header, using default maxzoom=14');
  }

  map = new maplibregl.Map({
    container: 'map',
    style: {
      version: 8,
      sources: {},
      layers: [
        {
          id: 'background',
          type: 'background',
          paint: { 'background-color': '#f0f0f0' },
        },
      ],
      glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
    },
    center: [0, 20],
    zoom: 2,
    maxZoom: 18,
  });

  map.addControl(new maplibregl.NavigationControl(), 'top-left');

  map.on('load', async () => {
    // Add a simple basemap via raster tiles
    map.addSource('osm', {
      type: 'raster',
      tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
      tileSize: 256,
      attribution: '&copy; OpenStreetMap contributors',
    });
    map.addLayer({ id: 'osm-tiles', type: 'raster', source: 'osm' });

    // Add rivers PMTiles source
    map.addSource('rivers', {
      type: 'vector',
      url: `pmtiles://${PMTILES_URL}`,
      maxzoom: riversMaxZoom,
    });

    // Base river layer (all rivers, muted colour)
    map.addLayer({
      id: 'rivers-base',
      type: 'line',
      source: 'rivers',
      'source-layer': 'rivers',
      paint: {
        'line-color': '#4a90d9',
        'line-width': [
          'interpolate',
          ['linear'],
          ['zoom'],
          2, 0.3,
          8, 1,
          14, 1.5,
        ],
        'line-opacity': 0.5,
      },
    });

    // Get the run ID first
    try {
      await loadRunId();
    } catch (err) {
      console.warn('Could not fetch run ID:', err);
      setStatus('Forecast data unavailable – showing rivers only');
      return;
    }

    // Initial data load for current zoom
    await loadDataForZoom(map.getZoom());

    // Reload on zoom changes (debounced)
    let zoomTimer = null;
    map.on('zoomend', () => {
      clearTimeout(zoomTimer);
      zoomTimer = setTimeout(() => loadDataForZoom(map.getZoom()), 300);
    });

    // Debug: log feature properties on click to identify matching field
    map.on('click', 'rivers-base', (e) => {
      if (e.features && e.features.length > 0) {
        const f = e.features[0];
        console.log('Feature ID:', f.id, 'Properties:', JSON.stringify(f.properties));
      }
    });

    // Click handlers
    map.on('click', 'rivers-highlighted', onRiverClick);
    map.on('click', 'rivers-base', onRiverClick);
    map.on('mouseenter', 'rivers-highlighted', () => {
      map.getCanvas().style.cursor = 'pointer';
    });
    map.on('mouseleave', 'rivers-highlighted', () => {
      map.getCanvas().style.cursor = '';
    });
    map.on('mouseenter', 'rivers-base', () => {
      map.getCanvas().style.cursor = 'pointer';
    });
    map.on('mouseleave', 'rivers-base', () => {
      map.getCanvas().style.cursor = '';
    });
  });
}

// ---------------------------------------------------------------------------
// River highlight layer – rebuilt when data changes
// ---------------------------------------------------------------------------
function updateHighlightedLayer() {
  // Remove existing highlighted layer if present
  if (map.getLayer('rivers-highlighted')) {
    map.removeLayer('rivers-highlighted');
  }

  const flaggedIds = Object.keys(forecastIndex).filter(
    (id) => (forecastIndex[id].severity_score || 0) > 0
  );

  if (flaggedIds.length === 0) return;

  // Convert reach IDs to numbers (PMTiles feature IDs are numeric)
  const numericIds = flaggedIds.map((id) => {
    const n = Number(id);
    return isNaN(n) ? id : n;
  });

  // Build color and width match expressions using feature id
  const colorExpr = ['match', ['id']];
  const widthExpr = ['match', ['id']];

  for (const id of numericIds) {
    const strId = String(id);
    const info = forecastIndex[strId];
    const sev = info.severity_score || 0;
    colorExpr.push(id, SEVERITY_COLORS[sev] || SEVERITY_COLORS[0]);
    widthExpr.push(id, SEVERITY_WIDTHS[sev] || 1);
  }
  // Fallback
  colorExpr.push(SEVERITY_COLORS[0]);
  widthExpr.push(1);

  map.addLayer({
    id: 'rivers-highlighted',
    type: 'line',
    source: 'rivers',
    'source-layer': 'rivers',
    filter: ['in', ['id'], ['literal', numericIds]],
    paint: {
      'line-color': colorExpr,
      'line-width': widthExpr,
      'line-opacity': 1,
    },
  });
}

// ---------------------------------------------------------------------------
// Click interaction – show reach detail
// ---------------------------------------------------------------------------
async function onRiverClick(e) {
  if (!e.features || e.features.length === 0) return;

  const feature = e.features[0];
  const reachId = String(feature.id || '');
  const info = forecastIndex[reachId];

  let html = `<h4>Reach ${reachId}</h4><table>`;

  if (info) {
    html += row('Severity', `${info.severity_score} / 6`);
    html += row('Return Period', BAND_LABELS[info.return_period_band] || info.return_period_band);
    if (info.peak_mean_cms != null) html += row('Peak Mean', `${info.peak_mean_cms.toFixed(1)} m³/s`);
    if (info.peak_max_cms != null) html += row('Peak Max', `${info.peak_max_cms.toFixed(1)} m³/s`);
    if (info.peak_time_utc) html += row('Peak Time', new Date(info.peak_time_utc).toUTCString());
  } else {
    html += row('Status', 'No elevated flood risk');
  }

  // Try to fetch detailed timeseries info from API
  if (currentRunId) {
    try {
      const detail = await fetchJSON(
        `${API_BASE}/reaches/${PROVIDER}/${reachId}?run_id=${currentRunId}&timeseries_limit=10`
      );
      if (detail.return_periods) {
        const rp = detail.return_periods;
        html += row('RP-2', rp.rp_2 != null ? `${rp.rp_2.toFixed(1)} m³/s` : '—');
        html += row('RP-10', rp.rp_10 != null ? `${rp.rp_10.toFixed(1)} m³/s` : '—');
        html += row('RP-100', rp.rp_100 != null ? `${rp.rp_100.toFixed(1)} m³/s` : '—');
      }
    } catch {
      // Detail not available – that's fine
    }
  }

  html += '</table>';
  infoContent.innerHTML = html;
  infoPanel.classList.remove('hidden');
}

function row(label, value) {
  return `<tr><td>${label}</td><td>${value}</td></tr>`;
}

// Close info panel
document.getElementById('info-close').addEventListener('click', () => {
  infoPanel.classList.add('hidden');
});

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
initMap();

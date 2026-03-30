import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import * as pmtiles from 'pmtiles';
import { Chart, registerables } from 'chart.js';
import 'chartjs-adapter-date-fns';

Chart.register(...registerables);

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const PMTILES_URL =
  'https://pub-6f1e54035ac14471852f4b7a25bf8354.r2.dev/rivers.pmtiles';
const NE_RIVERS_URL =
  'https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_rivers_lake_centerlines.geojson';
const NE_PMTILES_CROSSOVER_ZOOM = 6; // NE rivers below this zoom, PMTiles above
const API_BASE = '/forecast'; // proxied to backend via Vite
let PROVIDER = 'geoglows';

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
  0: 1.5,
  1: 3,
  2: 3.5,
  3: 4,
  4: 5,
  5: 5.5,
  6: 6.5,
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

// Zoom → minimum severity threshold + max reaches to load
// At global zoom only show the most extreme; as user zooms in, reveal more.
const ZOOM_SEVERITY_TIERS = [
  { maxZoom: 3, minSeverity: 4, limit: null },
  { maxZoom: 5, minSeverity: 3, limit: null },
  { maxZoom: 7, minSeverity: 2, limit: null },
  { maxZoom: Infinity, minSeverity: 1, limit: null },
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
let lastBboxKey = null; // track viewport to detect meaningful pans
let map;
let loadingAbort = null; // AbortController for in-flight requests
let forecastChart = null; // Chart.js instance
let viewportTimer = null; // unified debounce for zoom+pan
const PERF_ENABLED = true;

const perfState = {
  sessionStartedAt: Date.now(),
  longTasks: [],
  interactions: [],
  network: [],
  featureStateWrites: [],
  maxForecastIndexSize: 0,
};

const statusBar = document.getElementById('status-bar');
const infoPanel = document.getElementById('info-panel');
const infoContent = document.getElementById('info-content');

function setStatus(msg) {
  statusBar.textContent = msg;
}

function nowMs() {
  return performance?.now ? performance.now() : Date.now();
}

function recordPerf(bucket, payload) {
  if (!PERF_ENABLED) return;
  const entry = {
    t: new Date().toISOString(),
    ...payload,
  };
  if (Array.isArray(perfState[bucket])) perfState[bucket].push(entry);
}

let lastPerfSnapshot = 0;
function emitPerfSnapshot(reason) {
  if (!PERF_ENABLED) return;
  const now = Date.now();
  // Throttle to at most once per 10 seconds (except provider switches)
  if (reason === 'moveend' && now - lastPerfSnapshot < 10000) return;
  lastPerfSnapshot = now;
  const snapshot = {
    reason,
    uptime_seconds: Math.round((now - perfState.sessionStartedAt) / 1000),
    interactions: perfState.interactions.length,
    network_calls: perfState.network.length,
    feature_state_batches: perfState.featureStateWrites.length,
    long_tasks: perfState.longTasks.length,
    max_forecast_index_size: perfState.maxForecastIndexSize,
  };
  console.info('[perf:snapshot]', snapshot);
}

window.__geoflowsPerf = perfState;

if (typeof PerformanceObserver !== 'undefined') {
  try {
    const longTaskObserver = new PerformanceObserver((list) => {
      list.getEntries().forEach((entry) => {
        recordPerf('longTasks', {
          kind: 'longtask',
          duration_ms: Number(entry.duration.toFixed(2)),
          start_ms: Number(entry.startTime.toFixed(2)),
          name: entry.name || 'longtask',
        });
      });
    });
    longTaskObserver.observe({ entryTypes: ['longtask'] });
  } catch {
    // Ignore if unsupported by browser/runtime.
  }
}

// ---------------------------------------------------------------------------
// Forecast API helpers
// ---------------------------------------------------------------------------
async function fetchJSON(url, signal, perfLabel = 'fetch') {
  const started = nowMs();
  const res = await fetch(url, signal ? { signal } : undefined);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  const cloned = res.clone();
  const text = await cloned.text();
  const payloadBytes = new TextEncoder().encode(text).length;
  const elapsed = nowMs() - started;
  recordPerf('network', {
    label: perfLabel,
    url,
    status: res.status,
    duration_ms: Number(elapsed.toFixed(2)),
    payload_bytes: payloadBytes,
  });
  return JSON.parse(text);
}

async function loadRunId() {
  const started = nowMs();
  const run = await fetchJSON(
    `${API_BASE}/runs/latest?provider=${PROVIDER}`,
    undefined,
    'runs/latest'
  );
  currentRunId = run.run_id;
  recordPerf('interactions', {
    kind: 'run_id_load',
    provider: PROVIDER,
    run_id: currentRunId,
    duration_ms: Number((nowMs() - started).toFixed(2)),
  });
}

async function loadSeverityMap(minSeverity, limit, signal) {
  let url = `${API_BASE}/map/severity?provider=${PROVIDER}&run_id=${currentRunId}&min_severity_score=${minSeverity}`;
  if (limit) url += `&limit=${limit}`;
  const resp = await fetchJSON(url, signal, 'map/severity');
  return resp.severity || {};
}

// ---------------------------------------------------------------------------
// Viewport helpers
// ---------------------------------------------------------------------------

/** Quantise a bounding box to a grid so small pans don't trigger reloads. */
function bboxKey(bounds, step = 2) {
  const snap = (v) => (Math.round(v / step) * step).toFixed(0);
  return `${snap(bounds.getWest())},${snap(bounds.getSouth())},${snap(bounds.getEast())},${snap(bounds.getNorth())}`;
}

// ---------------------------------------------------------------------------
// Load data for current zoom level
// ---------------------------------------------------------------------------
async function loadDataForZoom(zoom) {
  const started = nowMs();
  const tier = getTierForZoom(zoom);

  // Skip if same tier AND viewport hasn't moved significantly
  const newBboxKey = bboxKey(map.getBounds());
  const tierChanged = !currentTier || tier.minSeverity < currentTier.minSeverity;
  const viewportChanged = newBboxKey !== lastBboxKey;

  if (!tierChanged && !viewportChanged) return;

  // Cancel any in-flight request
  if (loadingAbort) loadingAbort.abort();
  loadingAbort = new AbortController();

  setStatus(`Loading severity ≥ ${tier.minSeverity} reaches…`);

  try {
    const severityMap = await loadSeverityMap(
      tier.minSeverity,
      tier.limit,
      loadingAbort.signal
    );

    // Merge new reaches into existing index (don't lose higher-severity data)
    for (const [reachId, score] of Object.entries(severityMap)) {
      forecastIndex[reachId] = { severity_score: score };
    }
    perfState.maxForecastIndexSize = Math.max(
      perfState.maxForecastIndexSize,
      Object.keys(forecastIndex).length
    );

    currentTier = tier;
    lastBboxKey = newBboxKey;
    setStatus(
      `Run ${currentRunId} – ${Object.keys(forecastIndex).length} reaches loaded (severity ≥ ${tier.minSeverity})`
    );

    // Apply feature states only to visible features
    applyVisibleFeatureStates();
    recordPerf('interactions', {
      kind: 'zoom_data_load',
      provider: PROVIDER,
      zoom: Number(zoom.toFixed(2)),
      min_severity: tier.minSeverity,
      limit: tier.limit,
      loaded_reaches: Object.keys(severityMap).length,
      cached_reaches: Object.keys(forecastIndex).length,
      duration_ms: Number((nowMs() - started).toFixed(2)),
    });
  } catch (err) {
    if (err.name === 'AbortError') return; // superseded by a newer request
    console.warn('Could not load forecast summaries:', err);
    setStatus('Error loading forecast data');
  }
}

/** Unified handler for zoom + pan: debounce then load data + apply states. */
function onViewportChange() {
  clearTimeout(viewportTimer);
  viewportTimer = setTimeout(() => {
    if (!currentRunId) return;
    loadDataForZoom(map.getZoom());
  }, 300);
}

// ---------------------------------------------------------------------------
// Map setup
// ---------------------------------------------------------------------------
async function initMap() {
  // Register PMTiles protocol
  const protocol = new pmtiles.Protocol();
  maplibregl.addProtocol('pmtiles', protocol.tile);

  // Read PMTiles header for zoom range
  let riversMaxZoom = 14;
  let riversMinZoom = 0;
  try {
    const archive = new pmtiles.PMTiles(PMTILES_URL);
    const header = await archive.getHeader();
    riversMaxZoom = header.maxZoom || 14;
    riversMinZoom = header.minZoom || 0;
    console.info('[pmtiles] header:', { minZoom: riversMinZoom, maxZoom: riversMaxZoom });
  } catch (e) {
    console.warn('Could not read PMTiles header, using defaults');
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
    zoom: 3,
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
    map.addLayer({
      id: 'osm-tiles',
      type: 'raster',
      source: 'osm',
      paint: { 'raster-fade-duration': 0 },
    });

    // Add rivers PMTiles source (IDs are already baked into the tileset)
    map.addSource('rivers', {
      type: 'vector',
      url: `pmtiles://${PMTILES_URL}`,
      minzoom: riversMinZoom,
      maxzoom: riversMaxZoom,
    });

    // -----------------------------------------------------------------------
    // Natural Earth rivers – low-zoom fallback (z0 to crossover)
    // -----------------------------------------------------------------------
    // NE rivers provide continuous, clean geometry at global zoom where
    // PMTiles has only simplified dots. Loaded once, hidden at higher zoom.
    try {
      const neResp = await fetch(NE_RIVERS_URL);
      const neData = await neResp.json();
      map.addSource('ne-rivers', { type: 'geojson', data: neData });
      map.addLayer({
        id: 'ne-rivers',
        type: 'line',
        source: 'ne-rivers',
        maxzoom: NE_PMTILES_CROSSOVER_ZOOM,
        filter: ['<=', ['get', 'scalerank'], ['interpolate', ['linear'], ['zoom'], 0, 3, 2, 5, 4, 8, 5, 12]],
        layout: { 'line-cap': 'round', 'line-join': 'round' },
        paint: {
          'line-color': '#08519c',
          'line-width': ['interpolate', ['linear'], ['zoom'], 0, 0.8, 3, 1.5, 5, 2],
          'line-opacity': 0.7,
        },
      });
      console.info('[ne-rivers] loaded', neData.features?.length, 'features');
    } catch (e) {
      console.warn('Could not load Natural Earth rivers:', e);
    }

    // -----------------------------------------------------------------------
    // PMTiles river layers – progressive reveal by strmOrder + minzoom
    // -----------------------------------------------------------------------
    // Visible base layers:
    //   rivers-major:  strmOrder >= 7, no minzoom (dots at low zoom, lines at high)
    //   rivers-medium: strmOrder 4–6, visible from z5
    //   rivers-minor:  strmOrder < 4, visible from z8
    // Ghost query layers (invisible, allow queryRenderedFeatures at all zooms)

    const RIVER_TIERS = [
      { id: 'rivers-major',  filter: ['>=', ['get', 'strmOrder'], 7], minzoom: 0,  width: [2, 2.5, 5, 3, 8, 3.5, 12, 4], opacity: 0.9, color: '#08519c' },
      { id: 'rivers-medium', filter: ['all', ['>=', ['get', 'strmOrder'], 4], ['<', ['get', 'strmOrder'], 7]], minzoom: 5,  width: [5, 1.5, 7, 2, 9, 2.5, 12, 3], opacity: 0.7, color: '#2171b5' },
      { id: 'rivers-minor',  filter: ['<', ['get', 'strmOrder'], 4], minzoom: 8,  width: [8, 0.6, 10, 1, 12, 1.5, 14, 2], opacity: 0.5, color: '#4a90d9' },
    ];

    for (const tier of RIVER_TIERS) {
      // Visible layer
      map.addLayer({
        id: tier.id,
        type: 'line',
        source: 'rivers',
        'source-layer': 'rivers',
        minzoom: tier.minzoom,
        filter: tier.filter,
        layout: {
          'line-cap': 'round',
          'line-join': 'round',
        },
        paint: {
          'line-color': tier.color,
          'line-width': ['interpolate', ['linear'], ['zoom'], ...tier.width],
          'line-opacity': tier.opacity,
        },
      });

      // Ghost query layer – transparent, no minzoom, for click/query at all zooms
      map.addLayer({
        id: `${tier.id}-query`,
        type: 'line',
        source: 'rivers',
        'source-layer': 'rivers',
        minzoom: 0,
        filter: tier.filter,
        paint: {
          'line-color': 'transparent',
          'line-width': 6, // generous hit area
          'line-opacity': 0,
        },
      });
    }

    // All layer IDs for event binding and feature queries
    const RIVER_LAYER_IDS = RIVER_TIERS.flatMap(t => [t.id, `${t.id}-query`]);

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

    // Unified viewport handler – fires on both zoom and pan
    map.on('zoomend', () => {
      recordPerf('interactions', {
        kind: 'zoomend',
        provider: PROVIDER,
        zoom: Number(map.getZoom().toFixed(2)),
      });
      onViewportChange();
    });
    map.on('moveend', () => {
      const c = map.getCenter();
      recordPerf('interactions', {
        kind: 'moveend',
        provider: PROVIDER,
        zoom: Number(map.getZoom().toFixed(2)),
        center_lng: Number(c.lng.toFixed(4)),
        center_lat: Number(c.lat.toFixed(4)),
      });
      emitPerfSnapshot('moveend');
      onViewportChange();
      // Also apply feature states for newly-visible tiles after pan
      applyVisibleFeatureStates();
    });

    // Re-apply feature states as new tiles stream in (pan/zoom)
    map.on('sourcedata', onSourceData);

    // Click / cursor handlers for all river layers (highlight layers bound later in addHighlightLayer)
    for (const layerId of RIVER_LAYER_IDS) {
      map.on('click', layerId, onRiverClick);
      map.on('mouseenter', layerId, () => { map.getCanvas().style.cursor = 'pointer'; });
      map.on('mouseleave', layerId, () => { map.getCanvas().style.cursor = ''; });
    }
  });
}

// ---------------------------------------------------------------------------
// River highlight layers – one per tier, feature-state driven styling
// ---------------------------------------------------------------------------
const HIGHLIGHT_LAYER_IDS = [];
let highlightLayersAdded = false;

const HIGHLIGHT_PAINT = {
  'line-color': [
    'match',
    ['coalesce', ['feature-state', 'severity'], 0],
    1, SEVERITY_COLORS[1],
    2, SEVERITY_COLORS[2],
    3, SEVERITY_COLORS[3],
    4, SEVERITY_COLORS[4],
    5, SEVERITY_COLORS[5],
    6, SEVERITY_COLORS[6],
    'transparent',
  ],
  'line-width': [
    'match',
    ['coalesce', ['feature-state', 'severity'], 0],
    1, SEVERITY_WIDTHS[1],
    2, SEVERITY_WIDTHS[2],
    3, SEVERITY_WIDTHS[3],
    4, SEVERITY_WIDTHS[4],
    5, SEVERITY_WIDTHS[5],
    6, SEVERITY_WIDTHS[6],
    0,
  ],
  'line-opacity': 1,
};

function addHighlightLayer() {
  if (highlightLayersAdded) return;

  // Create a highlight layer for each tier so visibility matches base layers
  const tiers = [
    { id: 'rivers-highlight-major',  filter: ['>=', ['get', 'strmOrder'], 7], minzoom: 0 },
    { id: 'rivers-highlight-medium', filter: ['all', ['>=', ['get', 'strmOrder'], 4], ['<', ['get', 'strmOrder'], 7]], minzoom: 5 },
    { id: 'rivers-highlight-minor',  filter: ['<', ['get', 'strmOrder'], 4], minzoom: 8 },
  ];

  for (const tier of tiers) {
    map.addLayer({
      id: tier.id,
      type: 'line',
      source: 'rivers',
      'source-layer': 'rivers',
      minzoom: tier.minzoom,
      filter: tier.filter,
      paint: HIGHLIGHT_PAINT,
    });
    HIGHLIGHT_LAYER_IDS.push(tier.id);
    // Bind click/cursor handlers
    map.on('click', tier.id, onRiverClick);
    map.on('mouseenter', tier.id, () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', tier.id, () => { map.getCanvas().style.cursor = ''; });
  }
  highlightLayersAdded = true;
}

const appliedFeatureStates = new Set();
let rafPending = false;

/**
 * Apply feature states only to reaches currently rendered in the viewport.
 * Uses querySourceFeatures to get the IDs of tiles loaded by MapLibre,
 * then sets severity only for those – avoiding thousands of wasted writes
 * for off-screen features.
 */
function applyVisibleFeatureStates() {
  if (rafPending) return;
  rafPending = true;
  requestAnimationFrame(() => {
    rafPending = false;
    _applyVisibleFeatureStatesBatch();
  });
}

function _applyVisibleFeatureStatesBatch() {
  const started = nowMs();
  addHighlightLayer();

  // Get all river features currently loaded in visible tiles
  let visibleFeatures;
  try {
    visibleFeatures = map.querySourceFeatures('rivers', {
      sourceLayer: 'rivers',
    });
  } catch {
    // Source not loaded yet
    return;
  }

  let writes = 0;
  const seen = new Set();

  for (const feature of visibleFeatures) {
    const reachId = String(feature.id ?? '');
    if (!reachId || seen.has(reachId)) continue;
    seen.add(reachId);

    const info = forecastIndex[reachId];
    if (!info) continue; // no forecast data for this reach – skip

    const severity = info.severity_score || 0;
    if (severity === 0) continue;

    // Skip if we already wrote this exact state
    if (appliedFeatureStates.has(reachId)) continue;

    const numId = Number(reachId);
    if (isNaN(numId)) continue;

    map.setFeatureState(
      { source: 'rivers', sourceLayer: 'rivers', id: numId },
      { severity }
    );
    appliedFeatureStates.add(reachId);
    writes += 1;
  }

  recordPerf('featureStateWrites', {
    kind: 'apply_visible_feature_state_batch',
    writes,
    visible_features: visibleFeatures.length,
    unique_visible: seen.size,
    indexed_reaches: Object.keys(forecastIndex).length,
    duration_ms: Number((nowMs() - started).toFixed(2)),
  });
}

/** Re-apply states when new tiles finish loading (e.g. after pan/zoom). */
function onSourceData(e) {
  if (e.sourceId === 'rivers' && e.isSourceLoaded) {
    applyVisibleFeatureStates();
  }
}

// ---------------------------------------------------------------------------
// Click interaction – show reach detail + hydrograph
// ---------------------------------------------------------------------------

// Return period line colours (subtle, dashed)
const RP_LINE_COLORS = {
  rp_2:   { color: '#91cf60', label: 'RP 2-yr' },
  rp_5:   { color: '#fee08b', label: 'RP 5-yr' },
  rp_10:  { color: '#fdae61', label: 'RP 10-yr' },
  rp_25:  { color: '#f46d43', label: 'RP 25-yr' },
  rp_50:  { color: '#d73027', label: 'RP 50-yr' },
  rp_100: { color: '#67001f', label: 'RP 100-yr' },
};

function buildHydrograph(timeseries, returnPeriods) {
  const canvas = document.getElementById('forecast-chart');

  // Destroy previous chart
  if (forecastChart) {
    forecastChart.destroy();
    forecastChart = null;
  }

  if (!timeseries || timeseries.length === 0) {
    canvas.style.display = 'none';
    return;
  }
  canvas.style.display = 'block';

  // Sort by time
  const sorted = [...timeseries].sort(
    (a, b) => new Date(a.forecast_time_utc) - new Date(b.forecast_time_utc)
  );

  const labels = sorted.map((p) => new Date(p.forecast_time_utc));

  const datasets = [];

  // P25–P75 shaded band (filled area between)
  const hasSpread =
    sorted.some((p) => p.flow_p25_cms != null) &&
    sorted.some((p) => p.flow_p75_cms != null);

  if (hasSpread) {
    // Upper bound (p75) – filled down to p25
    datasets.push({
      label: 'P75',
      data: sorted.map((p) => p.flow_p75_cms),
      borderColor: 'transparent',
      backgroundColor: 'rgba(66,133,244,0.15)',
      fill: '+1', // fill to next dataset (p25)
      pointRadius: 0,
      order: 3,
    });
    // Lower bound (p25)
    datasets.push({
      label: 'P25',
      data: sorted.map((p) => p.flow_p25_cms),
      borderColor: 'transparent',
      backgroundColor: 'transparent',
      fill: false,
      pointRadius: 0,
      order: 3,
    });
  }

  // Max flow line
  if (sorted.some((p) => p.flow_max_cms != null)) {
    datasets.push({
      label: 'Max',
      data: sorted.map((p) => p.flow_max_cms),
      borderColor: 'rgba(213,0,0,0.5)',
      borderWidth: 1,
      borderDash: [4, 3],
      fill: false,
      pointRadius: 0,
      order: 2,
    });
  }

  // Mean flow line (primary)
  datasets.push({
    label: 'Mean',
    data: sorted.map((p) => p.flow_mean_cms),
    borderColor: '#1565c0',
    borderWidth: 2,
    fill: false,
    pointRadius: 0,
    order: 1,
  });

  // Return period threshold lines
  if (returnPeriods) {
    for (const [key, meta] of Object.entries(RP_LINE_COLORS)) {
      const val = returnPeriods[key];
      if (val == null) continue;
      datasets.push({
        label: meta.label,
        data: labels.map(() => val),
        borderColor: meta.color,
        borderWidth: 1.5,
        borderDash: [6, 4],
        fill: false,
        pointRadius: 0,
        order: 4,
      });
    }
  }

  forecastChart = new Chart(canvas, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          display: true,
          position: 'bottom',
          labels: { boxWidth: 14, font: { size: 10 }, padding: 6 },
        },
        tooltip: {
          callbacks: {
            label: (ctx) =>
              `${ctx.dataset.label}: ${ctx.parsed.y != null ? ctx.parsed.y.toFixed(1) : '—'} m³/s`,
          },
        },
      },
      scales: {
        x: {
          type: 'time',
          time: { unit: 'day', tooltipFormat: 'MMM d, HH:mm' },
          title: { display: false },
          ticks: { font: { size: 10 }, maxRotation: 0 },
        },
        y: {
          title: { display: true, text: 'm³/s', font: { size: 11 } },
          beginAtZero: true,
          ticks: { font: { size: 10 } },
        },
      },
    },
  });
}

async function onRiverClick(e) {
  const clickStarted = nowMs();
  if (!e.features || e.features.length === 0) return;

  const feature = e.features[0];
  const reachId = String(feature.id || '');
  const info = forecastIndex[reachId];

  let html = `<h4>Reach ${reachId}</h4><table>`;

  if (info) {
    html += row('Severity', `${info.severity_score} / 6`);
  } else {
    html += row('Status', 'No elevated flood risk');
  }

  // Show panel immediately with what we have
  document.getElementById('info-title').textContent = `Reach ${reachId} — ${PROVIDER.toUpperCase()}`;
  infoContent.innerHTML = html + '</table>';
  // Reset position to default top-right on new click
  infoPanel.style.top = '12px';
  infoPanel.style.right = '12px';
  infoPanel.style.left = 'auto';
  infoPanel.classList.remove('hidden');

  // Fetch full detail (with full timeseries for chart)
  if (currentRunId) {
    try {
      const detail = await fetchJSON(
        `${API_BASE}/reaches/${PROVIDER}/${reachId}?run_id=${currentRunId}&timeseries_limit=500`,
        undefined,
        'reaches/detail'
      );
      if (detail.summary) {
        const s = detail.summary;
        if (s.return_period_band)
          html += row('Return Period', BAND_LABELS[s.return_period_band] || s.return_period_band);
        if (s.peak_mean_cms != null)
          html += row('Peak Mean', `${s.peak_mean_cms.toFixed(1)} m³/s`);
        if (s.peak_max_cms != null)
          html += row('Peak Max', `${s.peak_max_cms.toFixed(1)} m³/s`);
        if (s.peak_time_utc)
          html += row('Peak Time', new Date(s.peak_time_utc).toUTCString());
      }
      html += '</table>';
      infoContent.innerHTML = html;

      // Build hydrograph
      buildHydrograph(detail.timeseries, detail.return_periods);
      recordPerf('interactions', {
        kind: 'river_click_detail',
        provider: PROVIDER,
        reach_id: reachId,
        timeseries_points: Array.isArray(detail.timeseries) ? detail.timeseries.length : 0,
        duration_ms: Number((nowMs() - clickStarted).toFixed(2)),
      });
    } catch {
      html += '</table>';
      infoContent.innerHTML = html;
      buildHydrograph(null, null);
    }
  } else {
    html += '</table>';
    infoContent.innerHTML = html;
    buildHydrograph(null, null);
  }
}

function row(label, value) {
  return `<tr><td>${label}</td><td>${value}</td></tr>`;
}

// Close info panel
document.getElementById('info-close').addEventListener('click', () => {
  infoPanel.classList.add('hidden');
  if (forecastChart) {
    forecastChart.destroy();
    forecastChart = null;
  }
});

// Resize chart when panel is resized
new ResizeObserver(() => {
  if (forecastChart) forecastChart.resize();
}).observe(infoPanel);

// Drag to move info panel
(function initDrag() {
  const titlebar = document.getElementById('info-titlebar');
  let dragging = false;
  let offsetX = 0;
  let offsetY = 0;

  titlebar.addEventListener('mousedown', (e) => {
    if (e.target.id === 'info-close') return;
    dragging = true;
    const rect = infoPanel.getBoundingClientRect();
    offsetX = e.clientX - rect.left;
    offsetY = e.clientY - rect.top;
    // Switch from right-anchored to left-anchored positioning
    infoPanel.style.left = rect.left + 'px';
    infoPanel.style.top = rect.top + 'px';
    infoPanel.style.right = 'auto';
    e.preventDefault();
  });

  document.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    infoPanel.style.left = (e.clientX - offsetX) + 'px';
    infoPanel.style.top = (e.clientY - offsetY) + 'px';
  });

  document.addEventListener('mouseup', () => {
    dragging = false;
  });
})();

// ---------------------------------------------------------------------------
// Provider toggle
// ---------------------------------------------------------------------------
function switchProvider(newProvider) {
  if (newProvider === PROVIDER) return;
  const started = nowMs();
  PROVIDER = newProvider;

  // Update toggle buttons
  document.querySelectorAll('.provider-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.provider === newProvider);
  });

  // Clear existing forecast data
  forecastIndex = {};
  currentTier = null;
  lastBboxKey = null;

  // Clear feature states on the map before clearing the tracking set
  if (map && map.getSource('rivers')) {
    for (const reachId of appliedFeatureStates) {
      const numId = Number(reachId);
      if (isNaN(numId)) continue;
      map.setFeatureState(
        { source: 'rivers', sourceLayer: 'rivers', id: numId },
        { severity: 0 }
      );
    }
  }
  appliedFeatureStates.clear();

  // Close info panel
  infoPanel.classList.add('hidden');
  if (forecastChart) {
    forecastChart.destroy();
    forecastChart = null;
  }

  // Reload for new provider
  (async () => {
    try {
      await loadRunId();
      await loadDataForZoom(map.getZoom());
      recordPerf('interactions', {
        kind: 'switch_provider',
        provider: newProvider,
        duration_ms: Number((nowMs() - started).toFixed(2)),
      });
      emitPerfSnapshot(`switch:${newProvider}`);
    } catch (err) {
      console.warn(`Could not load ${newProvider} data:`, err);
      setStatus(`${newProvider.toUpperCase()} forecast data unavailable`);
    }
  })();
}

document.querySelectorAll('.provider-btn').forEach((btn) => {
  btn.addEventListener('click', () => switchProvider(btn.dataset.provider));
});

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
initMap();

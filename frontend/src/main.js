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
const NE_PMTILES_CROSSOVER_ZOOM = 6; // z6 = crossover where NE fades out and PMTiles takes over
const DEBUG_RIVERS = new URLSearchParams(window.location.search).get('debugRivers') === '1';
const FLOW_TUNING_UI = new URLSearchParams(window.location.search).get('flowTuning') !== '0';
const API_BASE = '/forecast'; // proxied to backend via Vite
const FLOOD_TILE_BUCKET_BASE = 'https://pub-ca427796d1e2457685016e82ce231ce3.r2.dev/tiles';
const FLOOD_TILE_METADATA_URL = `${FLOOD_TILE_BUCKET_BASE}/metadata.json`;
const GAUGE_LAYER_URL =
  'https://services9.arcgis.com/RHVPKKiFTONKtxq3/ArcGIS/rest/services/Live_Stream_Gauges_v1/FeatureServer/0/query';
const GAUGE_LAYER_METADATA_URL =
  'https://services9.arcgis.com/RHVPKKiFTONKtxq3/ArcGIS/rest/services/Live_Stream_Gauges_v1/FeatureServer/0?f=json';
const GAUGE_REFRESH_MS = 5 * 60 * 1000;
const GAUGE_VIEWPORT_BUFFER_DEG = 0.5;
const GAUGE_PAGE_SIZE = 1500;
const GAUGE_MAX_PAGES = 25;
const GAUGE_OUT_FIELDS = [
  'OBJECTID',
  'name',
  'status',
  'location',
  'stage',
  'flow',
  'lastupdate',
  'url',
];
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

const GAUGE_STATUS_COLORS = {
  'Major Flood': '#b50000',
  'Moderate Flood': '#f73500',
  'Minor Flood': '#ff8b00',
  'Action Stage': '#f2ca00',
  'Low Flow': '#c1976f',
  'No Flooding': '#ffffff',
  Unknown: '#72d2e8',
};

const GAUGE_STATUS_PRIORITY = {
  'Major Flood': 'high',
  'Moderate Flood': 'high',
  'Minor Flood': 'high',
  'Action Stage': 'high',
  'Low Flow': 'medium',
  'No Flooding': 'low',
  Unknown: 'low',
};

const GAUGE_STATUS_CANONICAL = {
  'major flood': 'Major Flood',
  'moderate flood': 'Moderate Flood',
  'minor flood': 'Minor Flood',
  'action stage': 'Action Stage',
  'low flow': 'Low Flow',
  'no flooding': 'No Flooding',
  unknown: 'Unknown',
};

const GAUGE_ZOOM_VISIBILITY_POLICY = [
  { maxZoom: 5.5, priorities: ['high'] },
  { maxZoom: 8, priorities: ['high', 'medium'] },
  { maxZoom: Infinity, priorities: ['high', 'medium', 'low'] },
];

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

function getGaugeVisibilityPolicyForZoom(zoom) {
  for (const policy of GAUGE_ZOOM_VISIBILITY_POLICY) {
    if (zoom <= policy.maxZoom) return policy;
  }
  return GAUGE_ZOOM_VISIBILITY_POLICY[GAUGE_ZOOM_VISIBILITY_POLICY.length - 1];
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
let riverLayerIds = [];
let riverFlowAnimator = null;
let gaugesVisible = true;
let gaugesRefreshTimer = null;
let gaugesLoadedOnce = false;
let gaugesAbort = null;
let lastGaugeBboxKey = null;
let floodTileDate = null;
let floodTileLayer = 'flood';
let floodTilesReady = false;
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
const riverDebug = document.getElementById('river-debug');
const riverFlowCanvas = document.getElementById('river-flow-canvas');
const flowControls = document.getElementById('flow-controls');
const gaugeToggle = document.getElementById('gauge-toggle');
const floodDateSelect = document.getElementById('flood-date-select');
const floodLayerSelect = document.getElementById('flood-layer-select');
setFloodControlsEnabled(false);

const flowFxConfig = {
  dashLength: 6,
  gapLength: 12,
  speedMult: 0.3,
  widthMult: 1,
  opacityMult: 0.2,
  pulseMult: 0,
};

function setStatus(msg) {
  statusBar.textContent = msg;
}

function buildFloodTileUrlTemplate(date = floodTileDate, layer = floodTileLayer) {
  return `${FLOOD_TILE_BUCKET_BASE}/${date}/${layer}/{z}/{x}/{y}.png`;
}

function parseDateCandidate(value) {
  const s = String(value ?? '').trim();
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{8}$/.test(s)) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  return null;
}

function setFloodTileSource(date = floodTileDate, layer = floodTileLayer) {
  if (!map || !date || !map.getSource('flood-tiles')) return;
  const source = map.getSource('flood-tiles');
  source.setTiles([buildFloodTileUrlTemplate(date, layer)]);
}

function parseFloodTileDates(metadata, out = new Set()) {
  if (metadata == null) return out;
  if (Array.isArray(metadata)) {
    for (const item of metadata) parseFloodTileDates(item, out);
    return out;
  }
  if (typeof metadata === 'object') {
    for (const [key, value] of Object.entries(metadata)) {
      const keyLc = key.toLowerCase();
      if (keyLc.includes('date') || keyLc.includes('run')) {
        const maybe = parseDateCandidate(value);
        if (maybe) out.add(maybe);
      }
      parseFloodTileDates(value, out);
    }
    return out;
  }
  const maybe = parseDateCandidate(metadata);
  if (maybe) out.add(maybe);
  return out;
}

function setFloodControlsEnabled(enabled) {
  if (floodDateSelect) floodDateSelect.disabled = !enabled;
  if (floodLayerSelect) floodLayerSelect.disabled = !enabled;
}

function ensureFloodTilesLayer() {
  if (!map || !floodTileDate || map.getSource('flood-tiles')) return;
  map.addSource('flood-tiles', {
    type: 'raster',
    tiles: [buildFloodTileUrlTemplate(floodTileDate, floodTileLayer)],
    tileSize: 256,
  });
  map.addLayer({
    id: 'flood-tiles',
    type: 'raster',
    source: 'flood-tiles',
    paint: {
      'raster-opacity': 0.55,
      'raster-fade-duration': 0,
    },
  });
  floodTilesReady = true;
}

async function loadFloodTileMetadata() {
  try {
    const metadata = await fetchJSON(FLOOD_TILE_METADATA_URL, undefined, 'tiles/metadata');
    const dates = [...parseFloodTileDates(metadata)].sort((a, b) => b.localeCompare(a));
    if (dates.length === 0) throw new Error('metadata.json contained no parseable dates');
    floodTileDate = dates[0];
    if (floodDateSelect) {
      floodDateSelect.innerHTML = dates
        .map((d) => `<option value="${d}">${d}</option>`)
        .join('');
      floodDateSelect.value = floodTileDate;
    }
    ensureFloodTilesLayer();
    setFloodTileSource(floodTileDate, floodTileLayer);
    setFloodControlsEnabled(true);
  } catch (err) {
    console.warn('Could not load flood tile metadata:', err);
    if (floodDateSelect) {
      floodDateSelect.innerHTML = '<option value=\"\">Metadata unavailable</option>';
      floodDateSelect.value = '';
    }
    setFloodControlsEnabled(false);
  }
}

function normalizeReachId(value) {
  const s = String(value ?? '').trim();
  if (!s) return '';
  // Provider payloads can occasionally serialize integral IDs as floats (e.g. "760021611.0").
  // Normalize these so forecast index keys match PMTiles reach_id values.
  if (/^\d+\.0+$/.test(s)) return s.replace(/\.0+$/, '');
  return s;
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

function interpLinear(zoom, stops) {
  if (zoom <= stops[0][0]) return stops[0][1];
  if (zoom >= stops[stops.length - 1][0]) return stops[stops.length - 1][1];
  for (let i = 0; i < stops.length - 1; i += 1) {
    const [z0, v0] = stops[i];
    const [z1, v1] = stops[i + 1];
    if (zoom >= z0 && zoom <= z1) {
      const t = (zoom - z0) / (z1 - z0);
      return v0 + t * (v1 - v0);
    }
  }
  return stops[stops.length - 1][1];
}

function getRiverDebugState(zoom) {
  const majorMinArea = interpLinear(zoom, [[0, 120000], [3, 60000], [5, 15000], [6, 0]]);
  const mediumMinArea = interpLinear(zoom, [[6, 20000], [7, 5000], [8, 1000], [9, 100], [10, 0]]);
  const minorMinArea = interpLinear(zoom, [[8, 10000], [9, 2000], [10, 0]]);
  const neActive = zoom < (NE_PMTILES_CROSSOVER_ZOOM + 1);
  const neOpacity = neActive
    ? interpLinear(zoom, [[0, 0.75], [5, 0.7], [6, 0.35], [6.6, 0]])
    : 0;
  const neScalerank = neActive
    ? interpLinear(zoom, [[0, 3], [2, 5], [4, 8], [5, 12]])
    : 0;
  const majorOpacity = interpLinear(zoom, [[0, 0.2], [4.5, 0.35], [6, 0.8], [8, 0.9]]);
  return {
    neActive,
    neOpacity,
    neScalerank,
    major: true,
    medium: zoom >= 6,
    minor: zoom >= 8,
    majorOpacity,
    majorMinArea,
    mediumMinArea,
    minorMinArea,
  };
}

// ---------------------------------------------------------------------------
// Live stream gauges (ArcGIS FeatureServer layer 0)
// ---------------------------------------------------------------------------
function gaugeColorExpression() {
  return [
    'match',
    ['coalesce', ['get', 'status_norm'], ['get', 'status'], 'Unknown'],
    'Major Flood', GAUGE_STATUS_COLORS['Major Flood'],
    'Moderate Flood', GAUGE_STATUS_COLORS['Moderate Flood'],
    'Minor Flood', GAUGE_STATUS_COLORS['Minor Flood'],
    'Action Stage', GAUGE_STATUS_COLORS['Action Stage'],
    'Low Flow', GAUGE_STATUS_COLORS['Low Flow'],
    'No Flooding', GAUGE_STATUS_COLORS['No Flooding'],
    GAUGE_STATUS_COLORS.Unknown,
  ];
}

function gaugeRadiusExpression() {
  const byStatus = [
    'match',
    ['coalesce', ['get', 'status_norm'], ['get', 'status'], 'Unknown'],
    'Major Flood', 7.5,
    'Moderate Flood', 6,
    'Minor Flood', 5.25,
    'Action Stage', 4.5,
    'Low Flow', 4.5,
    'No Flooding', 2.25,
    2.25,
  ];
  return ['interpolate', ['linear'], ['zoom'], 2, byStatus, 6, ['+', byStatus, 2], 10, ['+', byStatus, 5]];
}

function gaugeFilterExpressionByZoom(zoom) {
  const policy = getGaugeVisibilityPolicyForZoom(zoom);
  const allowedStatuses = Object.entries(GAUGE_STATUS_PRIORITY)
    .filter(([, priority]) => policy.priorities.includes(priority))
    .map(([status]) => status);
  return ['in', ['coalesce', ['get', 'status_norm'], ['get', 'status'], 'Unknown'], ['literal', allowedStatuses]];
}

function expandBounds(bounds, bufferDeg = GAUGE_VIEWPORT_BUFFER_DEG) {
  return {
    west: Math.max(-180, bounds.getWest() - bufferDeg),
    south: Math.max(-90, bounds.getSouth() - bufferDeg),
    east: Math.min(180, bounds.getEast() + bufferDeg),
    north: Math.min(90, bounds.getNorth() + bufferDeg),
  };
}

function normalizeGaugeStatus(properties = {}) {
  const raw = properties.status
    ?? properties.Status
    ?? properties.STATUS
    ?? properties.flood_status
    ?? properties.FLOOD_STATUS
    ?? 'Unknown';
  const normalized = String(raw).trim().toLowerCase();
  return GAUGE_STATUS_CANONICAL[normalized] || 'Unknown';
}

function expandBounds(bounds, bufferDeg = GAUGE_VIEWPORT_BUFFER_DEG) {
  return {
    west: Math.max(-180, bounds.getWest() - bufferDeg),
    south: Math.max(-90, bounds.getSouth() - bufferDeg),
    east: Math.min(180, bounds.getEast() + bufferDeg),
    north: Math.min(90, bounds.getNorth() + bufferDeg),
  };
}

function gaugeQueryUrl(bounds, offset = 0, pageSize = GAUGE_PAGE_SIZE) {
  const envelope = expandBounds(bounds);
  const params = new URLSearchParams({
    f: 'geojson',
    where: '1=1',
    outFields: GAUGE_OUT_FIELDS.join(','),
    returnGeometry: 'true',
    outSR: '4326',
    geometry: `${envelope.west},${envelope.south},${envelope.east},${envelope.north}`,
    geometryType: 'esriGeometryEnvelope',
    spatialRel: 'esriSpatialRelIntersects',
    resultOffset: String(offset),
    resultRecordCount: String(pageSize),
  });
  if (gaugeLayerSpec.orderByField) params.set('orderByFields', `${gaugeLayerSpec.orderByField} ASC`);
  return `${GAUGE_LAYER_URL}?${params.toString()}`;
}

async function fetchGaugeFeaturesForViewport(bounds, signal) {
  const pageSize = GAUGE_PAGE_SIZE;
  const merged = [];
  let offset = 0;
  for (let page = 0; page < GAUGE_MAX_PAGES; page += 1) {
    const pageData = await fetchJSON(gaugeQueryUrl(bounds, offset, pageSize), signal, 'gauges/query');
    if (pageData?.error) {
      throw new Error(`Gauge query error: ${pageData.error.message || 'unknown'} (${pageData.error.code || 'n/a'})`);
    }
    const features = Array.isArray(pageData?.features) ? pageData.features : [];
    merged.push(...features);
    if (features.length < pageSize) break;
    offset += pageSize;
  }
  return {
    type: 'FeatureCollection',
    features: merged,
  };
}

function addGaugeLayers() {
  if (!map || map.getSource('stream-gauges')) return;
  map.addSource('stream-gauges', {
    type: 'geojson',
    data: { type: 'FeatureCollection', features: [] },
    cluster: true,
    clusterRadius: GAUGE_CLUSTER_RADIUS,
    clusterMaxZoom: GAUGE_CLUSTER_MAX_ZOOM,
  });

  map.addLayer({
    id: 'stream-gauge-clusters',
    type: 'circle',
    source: 'stream-gauges',
    filter: ['has', 'point_count'],
    paint: {
      'circle-color': '#2d5f9a',
      'circle-radius': ['interpolate', ['linear'], ['get', 'point_count'], 2, 13, 25, 18, 100, 24, 500, 30],
      'circle-stroke-color': '#ffffff',
      'circle-stroke-width': 1.5,
      'circle-opacity': 0.9,
    },
  });

  map.addLayer({
    id: 'stream-gauge-cluster-count',
    type: 'symbol',
    source: 'stream-gauges',
    filter: ['has', 'point_count'],
    layout: {
      'text-field': ['get', 'point_count_abbreviated'],
      'text-size': 12,
      'text-font': ['Open Sans Bold'],
    },
    paint: {
      'text-color': '#ffffff',
      'text-halo-color': 'rgba(0,0,0,0.25)',
      'text-halo-width': 1,
    },
  });

  map.addLayer({
    id: 'stream-gauges',
    type: 'circle',
    source: 'stream-gauges',
    filter: ['!', ['has', 'point_count']],
    paint: {
      'circle-color': gaugeColorExpression(),
      'circle-radius': gaugeRadiusExpression(),
      'circle-stroke-color': 'rgba(30,30,30,0.45)',
      'circle-stroke-width': [
        'match',
        ['coalesce', ['get', 'status'], 'Unknown'],
        'No Flooding', 1,
        2,
      ],
      'circle-opacity': 0.9,
    },
  });

  map.on('click', 'stream-gauge-clusters', (e) => {
    const feature = e.features?.[0];
    const clusterId = feature?.properties?.cluster_id;
    if (clusterId === undefined || clusterId === null) return;
    map.getSource('stream-gauges').getClusterExpansionZoom(clusterId, (err, zoom) => {
      if (err || typeof zoom !== 'number') return;
      map.easeTo({
        center: feature.geometry.coordinates,
        zoom,
        duration: 300,
      });
    });
  });
  map.on('click', 'stream-gauges', onGaugeClick);
  map.on('mouseenter', 'stream-gauge-clusters', () => { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'stream-gauge-clusters', () => { map.getCanvas().style.cursor = ''; });
  map.on('mouseenter', 'stream-gauges', () => { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'stream-gauges', () => { map.getCanvas().style.cursor = ''; });
}

function setGaugeLayerVisibility(visible) {
  if (!map) return;
  const visibility = visible ? 'visible' : 'none';
  ['stream-gauge-clusters', 'stream-gauge-cluster-count', 'stream-gauges'].forEach((layerId) => {
    if (map.getLayer(layerId)) map.setLayoutProperty(layerId, 'visibility', visibility);
  });
}

function applyGaugeVisibilityPolicy() {
  if (!map || !map.getLayer('stream-gauges')) return;
  map.setFilter('stream-gauges', [
    'all',
    ['!', ['has', 'point_count']],
    gaugeFilterExpressionByZoom(map.getZoom()),
  ]);
}

function formatGaugeValue(value, key) {
  if (value === null || value === undefined || value === '') return '—';
  if (key === 'lastupdate') {
    const ts = Number(value);
    if (Number.isFinite(ts)) return new Date(ts).toUTCString();
  }
  if (typeof value === 'number') return Number.isInteger(value) ? String(value) : value.toFixed(2);
  return String(value);
}

function onGaugeClick(e) {
  if (!e.features?.length) return;
  const props = e.features[0].properties || {};
  const rows = Object.entries(props).map(([key, value]) => {
    const display = formatGaugeValue(value, key);
    if (/url$/i.test(key) && display !== '—') {
      return `<tr><td>${key}</td><td><a href="${display}" target="_blank" rel="noopener noreferrer">open</a></td></tr>`;
    }
    return `<tr><td>${key}</td><td>${display}</td></tr>`;
  });
  const html = `<h4>${props.name || 'Flood Gauge'}</h4><table>${rows.join('')}</table>`;
  new maplibregl.Popup({ closeButton: true, maxWidth: '420px' })
    .setLngLat(e.lngLat)
    .setHTML(html)
    .addTo(map);
}

async function refreshGaugeData({ silent = false, force = false } = {}) {
  if (!map || !map.getSource('stream-gauges')) return;
  const started = nowMs();
  const gaugeBboxKey = bboxKey(map.getBounds(), 1);
  if (silent && !force && gaugeBboxKey === lastGaugeBboxKey) return;
  if (gaugesAbort) gaugesAbort.abort();
  gaugesAbort = new AbortController();
  try {
    const fc = await fetchGaugeFeaturesForViewport(map.getBounds(), gaugesAbort.signal);
    lastGaugeBboxKey = gaugeBboxKey;
    map.getSource('stream-gauges').setData(fc);
    gaugesLoadedOnce = true;
    recordPerf('network', {
      kind: 'gauges/query',
      provider: PROVIDER,
      bbox_key: gaugeBboxKey,
      features_raw: fc.features.length,
      duration_ms: Number((nowMs() - started).toFixed(2)),
    });
    if (!silent) {
      setStatus(`Run ${currentRunId} – ${Object.keys(forecastIndex).length} reaches loaded • ${filteredFeatures.length} live gauges`);
    }
  } catch (err) {
    if (err.name === 'AbortError') return;
    console.warn('Could not load live stream gauges:', err);
    if (!gaugesLoadedOnce) {
      setGaugeLayerVisibility(false);
      if (gaugeToggle) gaugeToggle.checked = false;
      gaugesVisible = false;
      setStatus('Live stream gauges unavailable (warning) – forecast layer still active');
    }
  }
}

function updateRiverDebugPanel() {
  if (!DEBUG_RIVERS || !map || !riverDebug) return;
  const z = map.getZoom();
  const s = getRiverDebugState(z);
  riverDebug.classList.remove('hidden');
  riverDebug.innerHTML = `
    <div><strong>River debug</strong> (URL flag: <code>?debugRivers=1</code>)</div>
    <div>zoom: <strong>${z.toFixed(2)}</strong></div>
    <div>NE active: <strong>${s.neActive ? 'yes' : 'no'}</strong>, opacity ~ ${s.neOpacity.toFixed(2)}</div>
    <div>NE filter: <code>scalerank &lt;= ${s.neScalerank.toFixed(2)}</code></div>
    <div>PMTiles major: <strong>on</strong>, opacity ~ ${s.majorOpacity.toFixed(2)}, min DSContArea ~ ${Math.round(s.majorMinArea)}</div>
    <div>PMTiles medium (strmOrder 4–6): <strong>${s.medium ? 'on' : 'off'}</strong>, min DSContArea ~ ${Math.round(s.mediumMinArea)}</div>
    <div>PMTiles minor (strmOrder &lt; 4): <strong>${s.minor ? 'on' : 'off'}</strong>, min DSContArea ~ ${Math.round(s.minorMinArea)}</div>
  `;
}

function initFlowControls() {
  if (!flowControls || !FLOW_TUNING_UI) return;
  flowControls.classList.remove('hidden');
  const controls = [
    ['dashLength', 'flow-dash-length', 'flow-dash-length-val'],
    ['gapLength', 'flow-gap-length', 'flow-gap-length-val'],
    ['speedMult', 'flow-speed-mult', 'flow-speed-mult-val'],
    ['widthMult', 'flow-width-mult', 'flow-width-mult-val'],
    ['opacityMult', 'flow-opacity-mult', 'flow-opacity-mult-val'],
    ['pulseMult', 'flow-pulse-mult', 'flow-pulse-mult-val'],
  ];
  for (const [key, inputId, valueId] of controls) {
    const input = document.getElementById(inputId);
    const valueEl = document.getElementById(valueId);
    if (!input || !valueEl) continue;
    input.value = String(flowFxConfig[key]);
    valueEl.textContent = input.value;
    input.addEventListener('input', () => {
      flowFxConfig[key] = Number(input.value);
      valueEl.textContent = input.value;
      riverFlowAnimator?.triggerRefresh();
    });
  }
  document.getElementById('flow-copy-config')?.addEventListener('click', async () => {
    const snippet = `const FLOW_FX = ${JSON.stringify(flowFxConfig, null, 2)};`;
    try {
      await navigator.clipboard.writeText(snippet);
      setStatus('Flow animation config copied to clipboard');
    } catch {
      setStatus(`Copy failed. Use this config: ${snippet}`);
    }
  });
}

// ---------------------------------------------------------------------------
// Canvas river flow animation
// ---------------------------------------------------------------------------
function severityForFeature(feature) {
  const reachId = normalizeReachId(feature?.id);
  const severity = reachId ? Number(forecastIndex[reachId]?.severity_score || 0) : 0;
  return Number.isFinite(severity) ? Math.max(0, Math.min(6, severity)) : 0;
}

function flowColorForSeverity(severity) {
  if (severity >= 5) return '#a50026';
  if (severity >= 3) return '#f46d43';
  if (severity >= 1) return '#fdae61';
  return '#4aa8ff';
}

function flowWidthForOrder(order, zoom) {
  const base = order >= 9 ? 4.6
    : order >= 8 ? 3.8
    : order >= 7 ? 3.0
    : order >= 6 ? 2.4
    : order >= 5 ? 1.9
    : order >= 4 ? 1.5
    : 1.1;
  const zoomScale = Math.max(0.72, Math.min(2.5, zoom / 4.8));
  return base * zoomScale * flowFxConfig.widthMult;
}

function getDashConfig(zoom) {
  const dashLenBase = interpLinear(zoom, [[0, 4], [3, 6], [5, 10], [7, 12], [10, 14]]);
  const gapLenBase = interpLinear(zoom, [[0, 14], [3, 18], [5, 20], [7, 22], [10, 24]]);
  const dashLen = dashLenBase * (flowFxConfig.dashLength / 10);
  const gapLen = gapLenBase * (flowFxConfig.gapLength / 20);
  return { dashLen, gapLen, period: dashLen + gapLen };
}

function drawDashedSegment(ctx, points, color, lineW, offsetPx, alpha, dashLen, gapLen) {
  if (points.length < 2) return;
  ctx.save();
  ctx.beginPath();
  ctx.moveTo(points[0][0], points[0][1]);
  for (let i = 1; i < points.length; i += 1) ctx.lineTo(points[i][0], points[i][1]);
  ctx.strokeStyle = color;
  ctx.lineWidth = lineW;
  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';
  ctx.globalAlpha = alpha;
  ctx.setLineDash([dashLen, gapLen]);
  ctx.lineDashOffset = -offsetPx;
  ctx.stroke();
  ctx.restore();
}

function createRiverFlowAnimator() {
  if (!riverFlowCanvas) return { triggerRefresh: () => {} };
  const ctx = riverFlowCanvas.getContext('2d');
  const pathOffsets = new Map();
  let cachedPaths = [];
  let refreshTimer = null;

  function syncCanvasSize() {
    const mapEl = document.getElementById('map');
    if (!mapEl) return;
    if (riverFlowCanvas.width !== mapEl.clientWidth) riverFlowCanvas.width = mapEl.clientWidth;
    if (riverFlowCanvas.height !== mapEl.clientHeight) riverFlowCanvas.height = mapEl.clientHeight;
  }

  function triggerRefresh() {
    if (refreshTimer) clearTimeout(refreshTimer);
    refreshTimer = setTimeout(refreshPaths, 90);
  }

  function refreshPaths() {
    if (!map || !map.loaded() || !riverLayerIds.length) return;
    const zoom = map.getZoom();
    let features = [];
    try {
      features = map.querySourceFeatures('rivers', { sourceLayer: 'rivers' });
    } catch {
      features = [];
    }
    const bounds = map.getBounds();
    const minAreaMajor = interpLinear(zoom, [[0, 120000], [3, 60000], [5, 15000], [6, 0]]);
    const minAreaMedium = interpLinear(zoom, [[6, 20000], [7, 5000], [8, 1000], [9, 100], [10, 0]]);
    const minAreaMinor = interpLinear(zoom, [[8, 10000], [9, 2000], [10, 0]]);
    const drawCap = zoom < 4 ? 1400 : zoom < 6 ? 2600 : zoom < 8 ? 4200 : 6500;
    const seen = new Set();
    cachedPaths = [];
    for (const feature of features) {
      const geom = feature.geometry;
      if (!geom) continue;
      const order = Number(feature.properties?.strmOrder ?? 5);
      const dsArea = Number(feature.properties?.DSContArea ?? 0);
      const include = (order >= 7 && dsArea >= minAreaMajor)
        || (zoom >= 6 && order >= 4 && order < 7 && dsArea >= minAreaMedium)
        || (zoom >= 8 && order < 4 && dsArea >= minAreaMinor);
      if (!include) continue;
      const severity = severityForFeature(feature);
      const collect = (coords) => {
        if (!coords || coords.length < 2) return;
        const mid = coords[Math.floor(coords.length / 2)];
        if (!mid || !bounds.contains([mid[0], mid[1]])) return;
        const key = `${normalizeReachId(feature.id)}:${coords[0][0].toFixed(3)},${coords[0][1].toFixed(3)}:${coords.length}`;
        if (seen.has(key)) return;
        seen.add(key);
        if (!pathOffsets.has(key)) pathOffsets.set(key, Math.random());
        cachedPaths.push({
          coords,
          key,
          severity,
          color: flowColorForSeverity(severity),
          width: flowWidthForOrder(order, zoom),
        });
      };
      if (geom.type === 'LineString') collect(geom.coordinates);
      if (geom.type === 'MultiLineString') geom.coordinates.forEach(collect);
      if (cachedPaths.length >= drawCap) break;
    }
  }

  function animate(ts) {
    syncCanvasSize();
    ctx.clearRect(0, 0, riverFlowCanvas.width, riverFlowCanvas.height);
    if (!map || !map.loaded() || !cachedPaths.length) {
      requestAnimationFrame(animate);
      return;
    }
    const zoom = map.getZoom();
    const { dashLen, gapLen, period } = getDashConfig(zoom);
    const baseSpeed = interpLinear(zoom, [[0, 10], [4, 18], [6, 24], [9, 34], [12, 44]]) * flowFxConfig.speedMult;
    const fadeWhenHighlighted = Object.keys(forecastIndex).length > 0 ? 0.88 : 1;
    for (const path of cachedPaths) {
      const pulse = 0.5 + 0.5 * Math.sin((ts / 1000) * (path.severity >= 3 ? 6 : 3) * Math.max(0.2, flowFxConfig.pulseMult));
      const speed = baseSpeed + path.severity * 5;
      const offset = (((pathOffsets.get(path.key) || 0) * period) - (ts / 1000) * speed) % period;
      const projected = path.coords.map(([lng, lat]) => {
        const p = map.project([lng, lat]);
        return [p.x, p.y];
      });
      ctx.save();
      ctx.globalAlpha = fadeWhenHighlighted * flowFxConfig.opacityMult;
      drawDashedSegment(ctx, projected, path.color, path.width * 1.1, 0, 0.16, dashLen * 2.5, gapLen);
      drawDashedSegment(ctx, projected, path.color, path.width * 1.7, offset, 0.62 + pulse * 0.2 * flowFxConfig.pulseMult, dashLen, gapLen);
      if (path.severity >= 3) {
        drawDashedSegment(ctx, projected, path.color, path.width * 3.1, offset, 0.12 + pulse * 0.08 * flowFxConfig.pulseMult, dashLen * 1.2, gapLen);
      }
      ctx.restore();
    }
    requestAnimationFrame(animate);
  }

  requestAnimationFrame(animate);
  return { triggerRefresh };
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
    for (const [rawReachId, score] of Object.entries(severityMap)) {
      const reachId = normalizeReachId(rawReachId);
      if (!reachId) continue;
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
    if (gaugesVisible) refreshGaugeData({ silent: true });
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
  if (DEBUG_RIVERS && riverDebug) {
    riverDebug.classList.remove('hidden');
    riverDebug.textContent = 'River debug enabled. Waiting for map...';
  }

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
    await loadFloodTileMetadata();

    // Add rivers PMTiles source (IDs are already baked into the tileset)
    map.addSource('rivers', {
      type: 'vector',
      url: `pmtiles://${PMTILES_URL}`,
      minzoom: riversMinZoom,
      maxzoom: riversMaxZoom,
      promoteId: 'reach_id',
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
        maxzoom: NE_PMTILES_CROSSOVER_ZOOM + 1,
        filter: ['<=', ['get', 'scalerank'], ['interpolate', ['linear'], ['zoom'], 0, 3, 2, 5, 4, 8, 5, 12]],
        layout: { 'line-cap': 'round', 'line-join': 'round' },
        paint: {
          'line-color': '#08519c',
          'line-width': ['interpolate', ['linear'], ['zoom'], 0, 0.8, 3, 1.5, 5, 2],
          'line-opacity': [
            'interpolate',
            ['linear'],
            ['zoom'],
            0, 0.75,
            NE_PMTILES_CROSSOVER_ZOOM - 1, 0.7,
            NE_PMTILES_CROSSOVER_ZOOM, 0.35,
            NE_PMTILES_CROSSOVER_ZOOM + 0.6, 0,
          ],
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
    //   rivers-medium: strmOrder 4–6, visible from z6
    //   rivers-minor:  strmOrder < 4, visible from z8
    // Ghost query layers (invisible, allow queryRenderedFeatures at all zooms)

    const RIVER_TIERS = [
      {
        id: 'rivers-major',
        filter: [
          'all',
          ['>=', ['get', 'strmOrder'], 7],
          ['>=', ['coalesce', ['get', 'DSContArea'], 0], ['interpolate', ['linear'], ['zoom'], 0, 120000, 3, 60000, 5, 15000, 6, 0]],
        ],
        minzoom: 0,
        // Keep low-zoom major PMTiles visually close to Natural Earth stroke
        // so crossover zooms don't show a jarring double-thickness effect.
        width: [0, 0.8, 3, 1.4, 5, 2.0, 6, 2.5, 8, 3.3, 12, 4.0],
        opacity: ['interpolate', ['linear'], ['zoom'], 0, 0.12, 3, 0.22, 4.5, 0.3, 6, 0.8, 8, 0.9],
        color: '#08519c',
      },
      {
        id: 'rivers-medium',
        filter: [
          'all',
          ['>=', ['get', 'strmOrder'], 4],
          ['<', ['get', 'strmOrder'], 7],
          ['>=', ['coalesce', ['get', 'DSContArea'], 0], ['interpolate', ['linear'], ['zoom'], 6, 20000, 7, 5000, 8, 1000, 9, 100, 10, 0]],
        ],
        minzoom: 6,
        width: [5, 1.3, 7, 1.8, 9, 2.3, 12, 3],
        opacity: ['interpolate', ['linear'], ['zoom'], 6, 0.45, 7, 0.6, 8, 0.7],
        color: '#2171b5',
      },
      {
        id: 'rivers-minor',
        filter: [
          'all',
          ['<', ['get', 'strmOrder'], 4],
          ['>=', ['coalesce', ['get', 'DSContArea'], 0], ['interpolate', ['linear'], ['zoom'], 8, 10000, 9, 2000, 10, 0]],
        ],
        minzoom: 8,
        width: [8, 0.5, 10, 0.9, 12, 1.3, 14, 1.8],
        opacity: ['interpolate', ['linear'], ['zoom'], 8, 0.2, 9, 0.35, 10, 0.5, 12, 0.6],
        color: '#4a90d9',
      },
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
    riverLayerIds = RIVER_LAYER_IDS;
    riverFlowAnimator = createRiverFlowAnimator();
    riverFlowAnimator.triggerRefresh();
    addGaugeLayers();
    setGaugeLayerVisibility(gaugesVisible);
    applyGaugeVisibilityPolicy();
    await refreshGaugeData({ silent: true });
    if (gaugesRefreshTimer) clearInterval(gaugesRefreshTimer);
    gaugesRefreshTimer = setInterval(() => {
      if (!gaugesVisible) return;
      refreshGaugeData({ silent: true, force: true });
    }, GAUGE_REFRESH_MS);

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
    riverFlowAnimator.triggerRefresh();

    // Unified viewport handler – fires on both zoom and pan
    map.on('zoomend', () => {
      recordPerf('interactions', {
        kind: 'zoomend',
        provider: PROVIDER,
        zoom: Number(map.getZoom().toFixed(2)),
      });
      onViewportChange();
      updateRiverDebugPanel();
      riverFlowAnimator.triggerRefresh();
      applyGaugeVisibilityPolicy();
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
      updateRiverDebugPanel();
      riverFlowAnimator.triggerRefresh();
    });

    // Re-apply feature states as new tiles stream in (pan/zoom)
    map.on('sourcedata', onSourceData);
    map.on('idle', () => riverFlowAnimator?.triggerRefresh());

    // Click / cursor handlers for all river layers (highlight layers bound later in addHighlightLayer)
    for (const layerId of RIVER_LAYER_IDS) {
      map.on('click', layerId, onRiverClick);
      map.on('mouseenter', layerId, () => { map.getCanvas().style.cursor = 'pointer'; });
      map.on('mouseleave', layerId, () => { map.getCanvas().style.cursor = ''; });
    }
  });
  map.on('idle', updateRiverDebugPanel);
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
    {
      id: 'rivers-highlight-major',
      filter: [
        'all',
        ['>=', ['get', 'strmOrder'], 7],
        ['>=', ['coalesce', ['get', 'DSContArea'], 0], ['interpolate', ['linear'], ['zoom'], 0, 120000, 3, 60000, 5, 15000, 6, 0]],
      ],
      minzoom: 0,
    },
    {
      id: 'rivers-highlight-medium',
      filter: [
        'all',
        ['>=', ['get', 'strmOrder'], 4],
        ['<', ['get', 'strmOrder'], 7],
        ['>=', ['coalesce', ['get', 'DSContArea'], 0], ['interpolate', ['linear'], ['zoom'], 6, 20000, 7, 5000, 8, 1000, 9, 100, 10, 0]],
      ],
      minzoom: 6,
    },
    {
      id: 'rivers-highlight-minor',
      filter: [
        'all',
        ['<', ['get', 'strmOrder'], 4],
        ['>=', ['coalesce', ['get', 'DSContArea'], 0], ['interpolate', ['linear'], ['zoom'], 8, 10000, 9, 2000, 10, 0]],
      ],
      minzoom: 8,
    },
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
    const reachId = normalizeReachId(feature.id);
    if (!reachId || seen.has(reachId)) continue;
    seen.add(reachId);

    const info = forecastIndex[reachId];
    if (!info) continue; // no forecast data for this reach – skip

    const severity = info.severity_score || 0;
    if (severity === 0) continue;

    // Skip if we already wrote this exact state
    if (appliedFeatureStates.has(reachId)) continue;

    map.setFeatureState(
      { source: 'rivers', sourceLayer: 'rivers', id: reachId },
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
  riverFlowAnimator?.triggerRefresh();
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
  const reachId = normalizeReachId(feature.id);
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
      map.setFeatureState(
        { source: 'rivers', sourceLayer: 'rivers', id: reachId },
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
      riverFlowAnimator?.triggerRefresh();
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

if (gaugeToggle) {
  gaugeToggle.checked = gaugesVisible;
  gaugeToggle.addEventListener('change', async (e) => {
    gaugesVisible = e.target.checked;
    setGaugeLayerVisibility(gaugesVisible);
    if (gaugesVisible) await refreshGaugeData({ silent: true, force: true });
  });
}

if (floodDateSelect) {
  floodDateSelect.addEventListener('change', (e) => {
    floodTileDate = e.target.value;
    if (!floodTilesReady) ensureFloodTilesLayer();
    setFloodTileSource(floodTileDate, floodTileLayer);
  });
}

if (floodLayerSelect) {
  floodLayerSelect.value = floodTileLayer;
  floodLayerSelect.addEventListener('change', (e) => {
    floodTileLayer = e.target.value || 'flood';
    if (!floodTilesReady) ensureFloodTilesLayer();
    setFloodTileSource(floodTileDate, floodTileLayer);
  });
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
initFlowControls();
initMap();

import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import './style.css';
import * as pmtiles from 'pmtiles';
import { Chart, registerables } from 'chart.js';
import 'chartjs-adapter-date-fns';

Chart.register(...registerables);

const PMTILES_URL =
  'https://pub-6f1e54035ac14471852f4b7a25bf8354.r2.dev/rivers.pmtiles';
const NE_RIVERS_URL =
  'https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_rivers_lake_centerlines.geojson';
const NE_PMTILES_CROSSOVER_ZOOM = 6;
const DEBUG_RIVERS = new URLSearchParams(window.location.search).get('debugRivers') === '1';
const API_BASE = '/forecast';
let PROVIDER = 'geoglows';

const SEVERITY_COLORS = { 0: '#1a9850', 1: '#fee08b', 2: '#fdae61', 3: '#f46d43', 4: '#d73027', 5: '#a50026', 6: '#67001f' };
const SEVERITY_WIDTHS = { 0: 1.5, 1: 3, 2: 3.5, 3: 4, 4: 5, 5: 5.5, 6: 6.5 };
const BAND_LABELS = { unknown: 'Unknown', below_2: 'Normal', '2': '2-year', '5': '5-year', '10': '10-year', '25': '25-year', '50': '50-year', '100': '100-year' };
const ZOOM_SEVERITY_TIERS = [
  { maxZoom: 3, minSeverity: 4, limit: null },
  { maxZoom: 5, minSeverity: 3, limit: null },
  { maxZoom: 7, minSeverity: 2, limit: null },
  { maxZoom: Infinity, minSeverity: 1, limit: null },
];

function getTierForZoom(zoom) {
  for (const tier of ZOOM_SEVERITY_TIERS) if (zoom <= tier.maxZoom) return tier;
  return ZOOM_SEVERITY_TIERS[ZOOM_SEVERITY_TIERS.length - 1];
}

let forecastIndex = {};
let currentRunId = null;
let currentTier = null;
let lastBboxKey = null;
let map;
let loadingAbort = null;
let forecastChart = null;
let viewportTimer = null;
let riverAnimationFrame = null;
let riverAnimationStart = 0;
let riverLayerIds = [];

const statusBar = document.getElementById('status-bar');
const infoPanel = document.getElementById('info-panel');
const infoContent = document.getElementById('info-content');
const riverDebug = document.getElementById('river-debug');

const perfState = { interactions: [], network: [], featureStateWrites: [] };

function setStatus(msg) { statusBar.textContent = msg; }
function nowMs() { return performance?.now ? performance.now() : Date.now(); }
function recordPerf(bucket, payload) { if (Array.isArray(perfState[bucket])) perfState[bucket].push({ t: new Date().toISOString(), ...payload }); }
function normalizeReachId(value) { const s = String(value ?? '').trim(); return /^\d+\.0+$/.test(s) ? s.replace(/\.0+$/, '') : s; }

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
    if (zoom >= z0 && zoom <= z1) return v0 + ((zoom - z0) / (z1 - z0)) * (v1 - v0);
  }
  return stops[stops.length - 1][1];
}

function updateRiverDebugPanel() {
  if (!DEBUG_RIVERS || !riverDebug || !map) return;
  const z = map.getZoom();
  riverDebug.style.display = 'block';
  riverDebug.textContent = `zoom ${z.toFixed(2)} · visible river layers: ${riverLayerIds.length}`;
}

function stopRiverAnimation() {
  if (riverAnimationFrame) cancelAnimationFrame(riverAnimationFrame);
  riverAnimationFrame = null;
}

function startRiverAnimation() {
  stopRiverAnimation();
  riverAnimationStart = performance.now();
  const animate = (ts) => {
    if (!map || !map.getLayer('rivers-flow-a') || !map.getLayer('rivers-flow-b')) return;
    const t = (ts - riverAnimationStart) / 1000;
    const pulse = (Math.sin(t * 2.3) + 1) * 0.5;
    map.setPaintProperty('rivers-flow-a', 'line-opacity', 0.32 + pulse * 0.24);
    map.setPaintProperty('rivers-flow-b', 'line-opacity', 0.18 + (1 - pulse) * 0.26);
    map.setPaintProperty('rivers-flow-a', 'line-dasharray', [1 + pulse * 1.6, 2.4, 0.4, 2.1]);
    map.setPaintProperty('rivers-flow-b', 'line-dasharray', [0.4, 2.1, 1 + (1 - pulse) * 1.6, 2.4]);
    riverAnimationFrame = requestAnimationFrame(animate);
  };
  riverAnimationFrame = requestAnimationFrame(animate);
}

async function fetchJSON(url, signal, label = 'fetch') {
  const started = nowMs();
  const res = await fetch(url, signal ? { signal } : undefined);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  const json = await res.json();
  recordPerf('network', { label, url, status: res.status, duration_ms: Number((nowMs() - started).toFixed(2)) });
  return json;
}

async function loadRunId() {
  const run = await fetchJSON(`${API_BASE}/runs/latest?provider=${PROVIDER}`, undefined, 'runs/latest');
  currentRunId = run.run_id;
}

async function loadSeverityMap(minSeverity, limit, signal) {
  let url = `${API_BASE}/map/severity?provider=${PROVIDER}&run_id=${currentRunId}&min_severity_score=${minSeverity}`;
  if (limit) url += `&limit=${limit}`;
  const resp = await fetchJSON(url, signal, 'map/severity');
  return resp.severity || {};
}

async function loadDataForZoom(zoom) {
  const tier = getTierForZoom(zoom);
  const newBboxKey = bboxKey(map.getBounds());
  const tierChanged = !currentTier || tier.minSeverity < currentTier.minSeverity;
  const viewportChanged = newBboxKey !== lastBboxKey;
  if (!tierChanged && !viewportChanged) return;

  if (loadingAbort) loadingAbort.abort();
  loadingAbort = new AbortController();
  setStatus(`Loading severity ≥ ${tier.minSeverity} reaches…`);

  try {
    const severityMap = await loadSeverityMap(tier.minSeverity, tier.limit, loadingAbort.signal);
    for (const [rawReachId, score] of Object.entries(severityMap)) {
      const reachId = normalizeReachId(rawReachId);
      if (!reachId) continue;
      forecastIndex[reachId] = { severity_score: score };
    }
    currentTier = tier;
    lastBboxKey = newBboxKey;
    setStatus(`Run ${currentRunId} – ${Object.keys(forecastIndex).length} reaches loaded (severity ≥ ${tier.minSeverity})`);
    applyVisibleFeatureStates();
  } catch (err) {
    if (err.name === 'AbortError') return;
    console.warn('Could not load forecast summaries:', err);
    setStatus('Error loading forecast data');
  }
}

function onViewportChange() {
  clearTimeout(viewportTimer);
  viewportTimer = setTimeout(() => {
    if (!currentRunId) return;
    loadDataForZoom(map.getZoom());
  }, 300);
}

const appliedFeatureStates = new Set();
let rafPending = false;
let highlightLayersAdded = false;

function addHighlightLayer() {
  if (highlightLayersAdded) return;
  const tiers = [
    { id: 'rivers-highlight-major', minzoom: 0, filter: ['all', ['>=', ['get', 'strmOrder'], 7]] },
    { id: 'rivers-highlight-medium', minzoom: 6, filter: ['all', ['>=', ['get', 'strmOrder'], 4], ['<', ['get', 'strmOrder'], 7]] },
    { id: 'rivers-highlight-minor', minzoom: 8, filter: ['all', ['<', ['get', 'strmOrder'], 4]] },
  ];
  const paint = {
    'line-color': ['match', ['coalesce', ['feature-state', 'severity'], 0], 1, SEVERITY_COLORS[1], 2, SEVERITY_COLORS[2], 3, SEVERITY_COLORS[3], 4, SEVERITY_COLORS[4], 5, SEVERITY_COLORS[5], 6, SEVERITY_COLORS[6], 'transparent'],
    'line-width': ['match', ['coalesce', ['feature-state', 'severity'], 0], 1, SEVERITY_WIDTHS[1], 2, SEVERITY_WIDTHS[2], 3, SEVERITY_WIDTHS[3], 4, SEVERITY_WIDTHS[4], 5, SEVERITY_WIDTHS[5], 6, SEVERITY_WIDTHS[6], 0],
    'line-opacity': 1,
  };

  for (const tier of tiers) {
    map.addLayer({ id: tier.id, type: 'line', source: 'rivers', 'source-layer': 'rivers', minzoom: tier.minzoom, filter: tier.filter, paint });
    map.on('click', tier.id, onRiverClick);
    map.on('mouseenter', tier.id, () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', tier.id, () => { map.getCanvas().style.cursor = ''; });
  }
  highlightLayersAdded = true;
}

function applyVisibleFeatureStates() {
  if (rafPending) return;
  rafPending = true;
  requestAnimationFrame(() => {
    rafPending = false;
    _applyVisibleFeatureStatesBatch();
  });
}

function _applyVisibleFeatureStatesBatch() {
  addHighlightLayer();
  let visibleFeatures = [];
  try { visibleFeatures = map.querySourceFeatures('rivers', { sourceLayer: 'rivers' }); } catch { return; }

  let writes = 0;
  const seenStateIds = new Set();
  for (const feature of visibleFeatures) {
    const stateId = feature.id;
    if (stateId == null) continue;
    const stateKey = String(stateId);
    if (seenStateIds.has(stateKey)) continue;
    seenStateIds.add(stateKey);

    const props = feature.properties || {};
    const candidates = [
      normalizeReachId(feature.id),
      normalizeReachId(props.reach_id),
      normalizeReachId(props.provider_reach_id),
      normalizeReachId(props.LINKNO),
    ].filter(Boolean);

    let info = null;
    for (const candidate of candidates) {
      if (forecastIndex[candidate]) {
        info = forecastIndex[candidate];
        break;
      }
    }
    if (!info || appliedFeatureStates.has(stateKey)) continue;

    map.setFeatureState({ source: 'rivers', sourceLayer: 'rivers', id: stateId }, { severity: info.severity_score || 0 });
    appliedFeatureStates.add(stateKey);
    writes += 1;
  }
  recordPerf('featureStateWrites', { kind: 'visible_batch', writes, visible_features: visibleFeatures.length });
  if (writes > 0) setStatus(`Run ${currentRunId} – ${Object.keys(forecastIndex).length} reaches loaded · styled ${writes} visible`);
}

function onSourceData(e) {
  if (e.sourceId === 'rivers' && e.isSourceLoaded) applyVisibleFeatureStates();
}

async function initMap() {
  const protocol = new pmtiles.Protocol();
  maplibregl.addProtocol('pmtiles', protocol.tile);

  let riversMaxZoom = 14;
  let riversMinZoom = 0;
  try {
    const archive = new pmtiles.PMTiles(PMTILES_URL);
    const header = await archive.getHeader();
    riversMaxZoom = header.maxZoom || 14;
    riversMinZoom = header.minZoom || 0;
  } catch {
    console.warn('Could not read PMTiles header, using defaults');
  }

  map = new maplibregl.Map({
    container: 'map',
    style: { version: 8, sources: {}, layers: [{ id: 'background', type: 'background', paint: { 'background-color': '#f0f0f0' } }], glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf' },
    center: [0, 20],
    zoom: 3,
    maxZoom: 18,
  });

  map.addControl(new maplibregl.NavigationControl(), 'top-left');

  map.on('load', async () => {
    map.addSource('osm', { type: 'raster', tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'], tileSize: 256, attribution: '&copy; OpenStreetMap contributors' });
    map.addLayer({ id: 'osm-tiles', type: 'raster', source: 'osm', paint: { 'raster-fade-duration': 0 } });

    map.addSource('rivers', {
      type: 'vector',
      url: `pmtiles://${PMTILES_URL}`,
      minzoom: riversMinZoom,
      maxzoom: riversMaxZoom,
      promoteId: 'reach_id',
    });

    try {
      const neResp = await fetch(NE_RIVERS_URL);
      const neData = await neResp.json();
      map.addSource('ne-rivers', { type: 'geojson', data: neData });
      map.addLayer({
        id: 'ne-rivers',
        type: 'line',
        source: 'ne-rivers',
        maxzoom: NE_PMTILES_CROSSOVER_ZOOM + 1,
        layout: { 'line-cap': 'round', 'line-join': 'round' },
        paint: {
          'line-color': '#08519c',
          'line-width': ['interpolate', ['linear'], ['zoom'], 0, 0.8, 3, 1.5, 5, 2],
          'line-opacity': ['interpolate', ['linear'], ['zoom'], 0, 0.75, NE_PMTILES_CROSSOVER_ZOOM, 0.35, NE_PMTILES_CROSSOVER_ZOOM + 0.6, 0],
        },
      });
    } catch (e) {
      console.warn('Could not load Natural Earth rivers:', e);
    }

    const RIVER_TIERS = [
      { id: 'rivers-major', minzoom: 0, color: '#08519c', width: [0, 0.8, 3, 1.4, 5, 2.0, 6, 2.5, 8, 3.3], filter: ['all', ['>=', ['get', 'strmOrder'], 7], ['>=', ['coalesce', ['get', 'DSContArea'], 0], ['interpolate', ['linear'], ['zoom'], 0, 120000, 3, 60000, 5, 15000, 6, 0]]], opacity: ['interpolate', ['linear'], ['zoom'], 0, 0.12, 6, 0.8, 8, 0.9] },
      { id: 'rivers-medium', minzoom: 6, color: '#2171b5', width: [5, 1.3, 7, 1.8, 9, 2.3, 12, 3], filter: ['all', ['>=', ['get', 'strmOrder'], 4], ['<', ['get', 'strmOrder'], 7], ['>=', ['coalesce', ['get', 'DSContArea'], 0], ['interpolate', ['linear'], ['zoom'], 6, 20000, 7, 5000, 8, 1000, 9, 100, 10, 0]]], opacity: ['interpolate', ['linear'], ['zoom'], 6, 0.45, 8, 0.7] },
      { id: 'rivers-minor', minzoom: 8, color: '#4a90d9', width: [8, 0.5, 10, 0.9, 12, 1.3, 14, 1.8], filter: ['all', ['<', ['get', 'strmOrder'], 4], ['>=', ['coalesce', ['get', 'DSContArea'], 0], ['interpolate', ['linear'], ['zoom'], 8, 10000, 9, 2000, 10, 0]]], opacity: ['interpolate', ['linear'], ['zoom'], 8, 0.2, 12, 0.6] },
    ];

    for (const tier of RIVER_TIERS) {
      map.addLayer({ id: tier.id, type: 'line', source: 'rivers', 'source-layer': 'rivers', minzoom: tier.minzoom, filter: tier.filter, layout: { 'line-cap': 'round', 'line-join': 'round' }, paint: { 'line-color': tier.color, 'line-width': ['interpolate', ['linear'], ['zoom'], ...tier.width], 'line-opacity': tier.opacity } });
      map.addLayer({ id: `${tier.id}-query`, type: 'line', source: 'rivers', 'source-layer': 'rivers', minzoom: 0, filter: tier.filter, paint: { 'line-color': 'transparent', 'line-width': 6, 'line-opacity': 0 } });
    }

    riverLayerIds = RIVER_TIERS.flatMap((t) => [t.id, `${t.id}-query`]);

    map.addLayer({
      id: 'rivers-flow-a',
      type: 'line',
      source: 'rivers',
      'source-layer': 'rivers',
      layout: { 'line-cap': 'round', 'line-join': 'round' },
      paint: {
        'line-color': '#2f7de1',
        'line-width': ['interpolate', ['linear'], ['zoom'], 2, 0.5, 8, 1.3, 14, 2.2],
        'line-opacity': 0.42,
        'line-dasharray': [1.3, 2.4, 0.4, 2.1],
      },
    });
    map.addLayer({
      id: 'rivers-flow-b',
      type: 'line',
      source: 'rivers',
      'source-layer': 'rivers',
      paint: {
        'line-color': '#8cc9ff',
        'line-width': ['interpolate', ['linear'], ['zoom'], 2, 0.25, 8, 0.9, 14, 1.5],
        'line-opacity': 0.22,
        'line-dasharray': [0.4, 2.1, 1.3, 2.4],
      },
    });
    startRiverAnimation();

    try {
      await loadRunId();
    } catch (err) {
      console.warn('Could not fetch run ID:', err);
      setStatus('Forecast data unavailable – showing rivers only');
      return;
    }

    await loadDataForZoom(map.getZoom());

    map.on('zoomend', () => { onViewportChange(); updateRiverDebugPanel(); });
    map.on('moveend', () => { onViewportChange(); applyVisibleFeatureStates(); updateRiverDebugPanel(); });
    map.on('sourcedata', onSourceData);

    for (const layerId of riverLayerIds) {
      map.on('click', layerId, onRiverClick);
      map.on('mouseenter', layerId, () => { map.getCanvas().style.cursor = 'pointer'; });
      map.on('mouseleave', layerId, () => { map.getCanvas().style.cursor = ''; });
    }
  });

  map.on('idle', updateRiverDebugPanel);
  map.on('remove', stopRiverAnimation);
}

const RP_LINE_COLORS = {
  rp_2: { color: '#91cf60', label: 'RP 2-yr' },
  rp_5: { color: '#fee08b', label: 'RP 5-yr' },
  rp_10: { color: '#fdae61', label: 'RP 10-yr' },
  rp_25: { color: '#f46d43', label: 'RP 25-yr' },
  rp_50: { color: '#d73027', label: 'RP 50-yr' },
  rp_100: { color: '#67001f', label: 'RP 100-yr' },
};

function buildHydrograph(timeseries, returnPeriods) {
  const canvas = document.getElementById('forecast-chart');
  if (forecastChart) { forecastChart.destroy(); forecastChart = null; }
  if (!timeseries || timeseries.length === 0) { canvas.style.display = 'none'; return; }
  canvas.style.display = 'block';
  const sorted = [...timeseries].sort((a, b) => new Date(a.forecast_time_utc) - new Date(b.forecast_time_utc));
  const labels = sorted.map((p) => new Date(p.forecast_time_utc));
  const datasets = [];

  const hasSpread = sorted.some((p) => p.flow_p25_cms != null) && sorted.some((p) => p.flow_p75_cms != null);
  if (hasSpread) {
    datasets.push({ label: 'P75', data: sorted.map((p) => p.flow_p75_cms), borderColor: 'transparent', backgroundColor: 'rgba(66,133,244,0.15)', fill: '+1', pointRadius: 0, order: 3 });
    datasets.push({ label: 'P25', data: sorted.map((p) => p.flow_p25_cms), borderColor: 'transparent', backgroundColor: 'transparent', fill: false, pointRadius: 0, order: 3 });
  }
  if (sorted.some((p) => p.flow_max_cms != null)) datasets.push({ label: 'Max', data: sorted.map((p) => p.flow_max_cms), borderColor: 'rgba(213,0,0,0.5)', borderWidth: 1, borderDash: [4, 3], fill: false, pointRadius: 0, order: 2 });
  datasets.push({ label: 'Mean', data: sorted.map((p) => p.flow_mean_cms), borderColor: '#1565c0', borderWidth: 2, fill: false, pointRadius: 0, order: 1 });
  if (returnPeriods) {
    for (const [key, meta] of Object.entries(RP_LINE_COLORS)) {
      const val = returnPeriods[key];
      if (val == null) continue;
      datasets.push({ label: meta.label, data: labels.map(() => val), borderColor: meta.color, borderWidth: 1.5, borderDash: [6, 4], fill: false, pointRadius: 0, order: 4 });
    }
  }

  forecastChart = new Chart(canvas, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { display: true, position: 'bottom', labels: { boxWidth: 14, font: { size: 10 }, padding: 6 } } },
      scales: { x: { type: 'time', time: { unit: 'day', tooltipFormat: 'MMM d, HH:mm' } }, y: { title: { display: true, text: 'm³/s', font: { size: 11 } }, beginAtZero: true } },
    },
  });
}

async function onRiverClick(e) {
  if (!e.features || e.features.length === 0) return;
  const feature = e.features[0];
  const reachId = normalizeReachId(feature.id);
  const info = forecastIndex[reachId];
  let html = `<h4>Reach ${reachId}</h4><table>`;
  html += info ? row('Severity', `${info.severity_score} / 6`) : row('Status', 'No elevated flood risk');
  document.getElementById('info-title').textContent = `Reach ${reachId} — ${PROVIDER.toUpperCase()}`;
  infoContent.innerHTML = html + '</table>';
  infoPanel.style.top = '12px'; infoPanel.style.right = '12px'; infoPanel.style.left = 'auto'; infoPanel.classList.remove('hidden');

  if (currentRunId) {
    try {
      const detail = await fetchJSON(`${API_BASE}/reaches/${PROVIDER}/${reachId}?run_id=${currentRunId}&timeseries_limit=500`, undefined, 'reaches/detail');
      if (detail.summary) {
        const s = detail.summary;
        if (s.return_period_band) html += row('Return Period', BAND_LABELS[s.return_period_band] || s.return_period_band);
        if (s.peak_mean_cms != null) html += row('Peak Mean', `${s.peak_mean_cms.toFixed(1)} m³/s`);
        if (s.peak_max_cms != null) html += row('Peak Max', `${s.peak_max_cms.toFixed(1)} m³/s`);
        if (s.peak_time_utc) html += row('Peak Time', new Date(s.peak_time_utc).toUTCString());
      }
      html += '</table>';
      infoContent.innerHTML = html;
      buildHydrograph(detail.timeseries, detail.return_periods);
      recordPerf('interactions', { kind: 'river_click_detail', provider: PROVIDER, reach_id: reachId });
    } catch {
      html += '</table>'; infoContent.innerHTML = html; buildHydrograph(null, null);
    }
  } else {
    html += '</table>'; infoContent.innerHTML = html; buildHydrograph(null, null);
  }
}

function row(label, value) { return `<tr><td>${label}</td><td>${value}</td></tr>`; }

document.getElementById('info-close').addEventListener('click', () => {
  infoPanel.classList.add('hidden');
  if (forecastChart) { forecastChart.destroy(); forecastChart = null; }
});

new ResizeObserver(() => { if (forecastChart) forecastChart.resize(); }).observe(infoPanel);

(function initDrag() {
  const titlebar = document.getElementById('info-titlebar');
  let dragging = false; let offsetX = 0; let offsetY = 0;
  titlebar.addEventListener('mousedown', (e) => {
    if (e.target.id === 'info-close') return;
    dragging = true;
    const rect = infoPanel.getBoundingClientRect();
    offsetX = e.clientX - rect.left;
    offsetY = e.clientY - rect.top;
    infoPanel.style.left = `${rect.left}px`; infoPanel.style.top = `${rect.top}px`; infoPanel.style.right = 'auto';
    e.preventDefault();
  });
  document.addEventListener('mousemove', (e) => { if (dragging) { infoPanel.style.left = `${e.clientX - offsetX}px`; infoPanel.style.top = `${e.clientY - offsetY}px`; } });
  document.addEventListener('mouseup', () => { dragging = false; });
})();

function switchProvider(newProvider) {
  if (newProvider === PROVIDER) return;
  PROVIDER = newProvider;
  document.querySelectorAll('.provider-btn').forEach((btn) => btn.classList.toggle('active', btn.dataset.provider === newProvider));

  forecastIndex = {};
  currentTier = null;
  lastBboxKey = null;

  if (map && map.getSource('rivers')) {
    for (const stateKey of appliedFeatureStates) {
      const numeric = Number(stateKey);
      const targetId = Number.isFinite(numeric) ? numeric : stateKey;
      map.setFeatureState({ source: 'rivers', sourceLayer: 'rivers', id: targetId }, { severity: 0 });
    }
  }
  appliedFeatureStates.clear();

  infoPanel.classList.add('hidden');
  if (forecastChart) { forecastChart.destroy(); forecastChart = null; }

  (async () => {
    try {
      await loadRunId();
      await loadDataForZoom(map.getZoom());
    } catch (err) {
      console.warn(`Could not load ${newProvider} data:`, err);
      setStatus(`${newProvider.toUpperCase()} forecast data unavailable`);
    }
  })();
}

document.querySelectorAll('.provider-btn').forEach((btn) => btn.addEventListener('click', () => switchProvider(btn.dataset.provider)));

window.__geoflowsPerf = perfState;
initMap();

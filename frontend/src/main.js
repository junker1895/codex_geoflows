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

// Zoom → minimum severity threshold + max reaches to load
// At global zoom only show the most extreme; as user zooms in, reveal more.
const ZOOM_SEVERITY_TIERS = [
  { maxZoom: 3, minSeverity: 4, limit: 5000 },
  { maxZoom: 5, minSeverity: 3, limit: 20000 },
  { maxZoom: 7, minSeverity: 2, limit: 50000 },
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
let map;
let loadingAbort = null; // AbortController for in-flight requests
let forecastChart = null; // Chart.js instance
let riverAnimationFrame = null;
let riverAnimationStart = 0;

const statusBar = document.getElementById('status-bar');
const infoPanel = document.getElementById('info-panel');
const infoContent = document.getElementById('info-content');

function setStatus(msg) {
  statusBar.textContent = msg;
}

function stopRiverAnimation() {
  if (riverAnimationFrame) {
    cancelAnimationFrame(riverAnimationFrame);
    riverAnimationFrame = null;
  }
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
    map.setPaintProperty('rivers-flow-a', 'line-dasharray', [1.0 + pulse * 1.6, 2.4, 0.4, 2.1]);
    map.setPaintProperty('rivers-flow-b', 'line-dasharray', [0.4, 2.1, 1.0 + (1 - pulse) * 1.6, 2.4]);

    riverAnimationFrame = requestAnimationFrame(animate);
  };

  riverAnimationFrame = requestAnimationFrame(animate);
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

async function loadSeverityMap(minSeverity, limit, signal) {
  let url = `${API_BASE}/map/severity?provider=${PROVIDER}&run_id=${currentRunId}&min_severity_score=${minSeverity}`;
  if (limit) url += `&limit=${limit}`;
  const resp = await fetchJSON(url, signal);
  return resp.severity || {};
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
    const severityMap = await loadSeverityMap(
      tier.minSeverity,
      tier.limit,
      loadingAbort.signal
    );

    // Merge new reaches into existing index (don't lose higher-severity data)
    for (const [reachId, score] of Object.entries(severityMap)) {
      forecastIndex[reachId] = { severity_score: score };
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
      layout: { 'line-cap': 'round', 'line-join': 'round' },
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

    map.addLayer({
      id: 'rivers-flow-a',
      type: 'line',
      source: 'rivers',
      'source-layer': 'rivers',
      layout: { 'line-cap': 'round', 'line-join': 'round' },
      paint: {
        'line-color': '#2f7de1',
        'line-width': [
          'interpolate',
          ['linear'],
          ['zoom'],
          2, 0.5,
          8, 1.3,
          14, 2.2,
        ],
        'line-opacity': 0.42,
        'line-dasharray': [1.3, 2.4, 0.4, 2.1],
      },
    });

    map.addLayer({
      id: 'rivers-flow-b',
      type: 'line',
      source: 'rivers',
      'source-layer': 'rivers',
      layout: { 'line-cap': 'round', 'line-join': 'round' },
      paint: {
        'line-color': '#8cc9ff',
        'line-width': [
          'interpolate',
          ['linear'],
          ['zoom'],
          2, 0.25,
          8, 0.9,
          14, 1.5,
        ],
        'line-opacity': 0.22,
        'line-dasharray': [0.4, 2.1, 1.3, 2.4],
      },
    });
    startRiverAnimation();

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

  map.on('remove', stopRiverAnimation);
}

// ---------------------------------------------------------------------------
// River highlight layer – uses feature-state for scalable styling
// ---------------------------------------------------------------------------
let highlightLayerAdded = false;

function addHighlightLayer() {
  if (highlightLayerAdded) return;

  // Use feature-state driven styling – works with any number of features
  map.addLayer({
    id: 'rivers-highlighted',
    type: 'line',
    source: 'rivers',
    'source-layer': 'rivers',
    paint: {
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
    },
  });
  highlightLayerAdded = true;
}

const appliedFeatureStates = new Set();

function updateHighlightedLayer() {
  addHighlightLayer();

  // Set feature state for each reach in the forecast index
  for (const [reachId, info] of Object.entries(forecastIndex)) {
    if (appliedFeatureStates.has(reachId)) continue;
    const numId = Number(reachId);
    if (isNaN(numId)) continue;
    map.setFeatureState(
      { source: 'rivers', sourceLayer: 'rivers', id: numId },
      { severity: info.severity_score || 0 }
    );
    appliedFeatureStates.add(reachId);
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
        `${API_BASE}/reaches/${PROVIDER}/${reachId}?run_id=${currentRunId}&timeseries_limit=500`
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
  PROVIDER = newProvider;

  // Update toggle buttons
  document.querySelectorAll('.provider-btn').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.provider === newProvider);
  });

  // Clear existing forecast data
  forecastIndex = {};
  currentTier = null;

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

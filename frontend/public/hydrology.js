(function(root,factory){if(typeof module==='object'&&module.exports){module.exports=factory();}else{root.Hydrology=factory();}})(typeof globalThis!=='undefined'?globalThis:window,function(){
const DEFAULT_TTL_MS=30000;
const DEFAULT_FORECAST_PROVIDER='geoglows';
function serializeBbox(b){return [b.minLon,b.minLat,b.maxLon,b.maxLat].map(Number).join(',');}
function buildUrl(base,path,params={}){const u=new URL(path,base);Object.entries(params).forEach(([k,v])=>{if(v!==undefined&&v!==null&&v!=='')u.searchParams.set(k,String(v));});return u.toString();}
async function fetchJson(url,{signal}={}){const r=await fetch(url,{signal});if(!r.ok)throw new Error(`Hydrology API ${r.status}`);return r.json();}
function getFreshnessLabel(v){return v||'status unavailable';}
function formatObservationValue(row){if(row?.value_canonical!=null&&row?.unit_canonical)return `${Number(row.value_canonical).toFixed(2)} ${row.unit_canonical}`;if(row?.value_native!=null&&row?.unit_native)return `${Number(row.value_native).toFixed(2)} ${row.unit_native}`;return '—';}
function getStationBadges(r){return ['is_forecast','is_provisional','is_estimated','is_missing','is_flagged'].filter(k=>r&&r[k]).map(k=>k.replace('is_',''));}
function getReachBadges(r){return getStationBadges(r);}
function stationVisualState(r){if(!r)return 'normal';if(r.is_missing)return 'missing';if(r.is_flagged)return 'flagged';if(r.warning_summary?.has_warning)return 'warning';if(r.is_forecast)return 'forecast';if(r.freshness_status==='stale')return 'stale';if(r.freshness_status==='old')return 'old';return 'fresh';}
function reachVisualState(r){if(!r)return 'normal';if(r.is_missing)return 'missing';if(r.warning_summary?.has_warning)return 'warning';if(r.is_forecast||r.quality_code==='forecast')return 'forecast';if(r.freshness_status==='stale')return 'stale';if(r.freshness_status==='old')return 'old';return 'fresh';}
function getWarningStyle(sev){const m={severe:{fill:'#ff3b30',line:'#ff8b84'},flood:{fill:'#ff8c42',line:'#ffc08f'},warning:{fill:'#f5c542',line:'#f9de8a'},watch:{fill:'#4aa8ff',line:'#92c8ff'}};return m[sev]||{fill:'#b06bff',line:'#d1a9ff'};}
function parseEnvelope(j){return {data:Array.isArray(j?.data)?j.data:[],meta:j?.meta||{}};}

function parseForecastMapEnvelope(json){
 if(Array.isArray(json?.data))return parseEnvelope(json);
 const list=Array.isArray(json?.reaches)?json.reaches:Array.isArray(json)?json:[];
 const meta=json?.meta||{};
 return {data:list,meta};
}

function forecastSeverityState(summary){
 if(!summary)return 'normal';
 if(summary.is_flagged)return 'flagged';
 const band=String(summary.return_period_band||'').toLowerCase();
 if(band.includes('extreme')||band.includes('200'))return 'extreme';
 if(band.includes('very_high')||band.includes('100'))return 'very-high';
 if(band.includes('high')||band.includes('50')||band.includes('25'))return 'high';
 if(band.includes('medium')||band.includes('10')||band.includes('5'))return 'medium';
 const score=Number(summary.severity_score);
 if(Number.isFinite(score)){
  if(score>=5)return 'extreme';
  if(score>=3)return 'high';
  if(score>=1)return 'medium';
  if(score>=0.85)return 'extreme';
  if(score>=0.7)return 'very-high';
  if(score>=0.5)return 'high';
  if(score>=0.3)return 'medium';
 }
 return 'normal';
}

function getForecastBadges(detail){
 if(!detail)return [];
 const summary=detail.summary||{};
 const badges=[];
 if(summary.is_flagged)badges.push('flagged');
 if(summary.return_period_band)badges.push(`rp:${String(summary.return_period_band).toLowerCase().replace(/\s+/g,'_')}`);
 const state=forecastSeverityState(summary);
 if(state!=='normal')badges.push(`severity:${state}`);
 if(detail.run)badges.push('latest-run');
 return badges;
}

function formatForecastPeak(detail){
 const summary=detail?.summary;
 if(!summary)return '—';
 const peak=summary.peak_mean_cms??summary.peak_median_cms??summary.peak_max_cms;
 if(peak==null)return '—';
 const when=summary.peak_time_utc;
 const val=`${Number(peak).toFixed(2)} m³/s`;
 return when?`${val} @ ${when}`:val;
}

function formatReturnPeriods(returnPeriods){
 if(!returnPeriods)return [];
 const order=['rp_2','rp_5','rp_10','rp_25','rp_50','rp_100'];
 return order.map(key=>{
  const value=returnPeriods[key];
  if(value==null)return null;
  const label=key.replace('rp_','RP');
  return `${label}: ${Number(value).toFixed(2)} m³/s`;
 }).filter(Boolean);
}

function createForecastState(currentForecastProvider=DEFAULT_FORECAST_PROVIDER){
 return {
  forecastReaches:[],
  selectedForecastReach:null,
  latestForecastRun:null,
  currentForecastProvider
 };
}

function createClient(baseUrl,{ttlMs=DEFAULT_TTL_MS}={}){const cache=new Map();
 const call=(path,params={},signal,parser=parseEnvelope)=>{const key=`${path}?${JSON.stringify(params)}`;const now=Date.now();const old=cache.get(key);if(old&&now-old.ts<ttlMs)return old.value;const p=fetchJson(buildUrl(baseUrl,path,params),{signal}).then(parser);cache.set(key,{ts:now,value:p});return p;};
 return {
  fetchStationsMap:(params={},signal)=>call('/v1/stations/map',params,signal),
  fetchReachesMap:(params={},signal)=>call('/v1/reaches/map',params,signal),
  fetchActiveWarnings:(params={},signal)=>call('/v1/warnings/active',params,signal),
  fetchStationTimeseries:(stationId,params={},signal)=>call(`/v1/stations/${encodeURIComponent(stationId)}/timeseries`,params,signal),
  fetchReachTimeseries:(reachId,params={},signal)=>call(`/v1/reaches/${encodeURIComponent(reachId)}/timeseries`,params,signal),
  fetchStationThresholds:(stationId,params={},signal)=>call(`/v1/stations/${encodeURIComponent(stationId)}/thresholds`,params,signal),
  fetchReachThresholds:(reachId,params={},signal)=>call(`/v1/reaches/${encodeURIComponent(reachId)}/thresholds`,params,signal),
  fetchForecastHealth:(params={},signal)=>call('/forecast/health',{provider:DEFAULT_FORECAST_PROVIDER,...params},signal),
  fetchForecastMap:(params={},signal)=>call('/forecast/map/reaches',params,signal,parseForecastMapEnvelope),
  fetchForecastReachDetail:(provider,reachId,params={},signal)=>call(`/forecast/reaches/${encodeURIComponent(provider||DEFAULT_FORECAST_PROVIDER)}/${encodeURIComponent(reachId)}`,params,signal)
 };
}
return {serializeBbox,createClient,stationVisualState,reachVisualState,getWarningStyle,formatObservationValue,getFreshnessLabel,getStationBadges,getReachBadges,forecastSeverityState,getForecastBadges,formatForecastPeak,formatReturnPeriods,createForecastState};
});

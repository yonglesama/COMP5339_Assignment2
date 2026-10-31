/* D's presentation layer. MQTT parsing, durable state and SQL stay in A/B/C. */
"use strict";
const $ = (id) => document.getElementById(id);
const fmt = (value, digits=0) => value == null ? "—" : Number(value).toLocaleString("en-AU", {minimumFractionDigits:digits, maximumFractionDigits:digits});
const esc = (value) => String(value ?? "Not provided").replace(/[&<>"']/g, c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const clock = (value) => value ? String(value).slice(11,16) : "—";
const app = {snapshot:null, selectedStation:null, selectedSession:null, sessionPinned:false, measure:"energy_kwh", markers:new Map(), busy:false, detailSequence:0, tileErrors:0};
const map = L.map("map", {zoomControl:false, center:[-33.88,151.14], zoom:11, minZoom:5, maxZoom:18});
L.control.zoom({position:"topleft"}).addTo(map);
L.control.scale({position:"bottomleft",imperial:false}).addTo(map);
// A lightweight grid remains underneath the external tiles if internet is unavailable.
const Grid = L.GridLayer.extend({createTile(coords){
  const canvas=document.createElement("canvas");canvas.width=256;canvas.height=256;
  const ctx=canvas.getContext("2d");ctx.fillStyle="#eaf0ec";ctx.fillRect(0,0,256,256);
  ctx.strokeStyle="#dce6df";ctx.lineWidth=1;
  for(let n=0;n<=256;n+=64){ctx.beginPath();ctx.moveTo(n,0);ctx.lineTo(n,256);ctx.moveTo(0,n);ctx.lineTo(256,n);ctx.stroke();}
  return canvas;
}});
new Grid({tileSize:256}).addTo(map);
const tiles=L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom:19, attribution:'&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap</a> contributors',
  updateWhenIdle:true, keepBuffer:1,
}).addTo(map);
tiles.on("tileerror",()=>{if(++app.tileErrors>=3)$('tile-notice').hidden=false;});
tiles.on("tileload",()=>{$('tile-notice').hidden=true;app.tileErrors=0;});
const markerLayer=L.layerGroup().addTo(map);
let selectedPrice=null;

async function api(path, options={}) {
  const response=await fetch(path, {cache:"no-store",...options});
  const data=await response.json();
  if(!response.ok)throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}
function notify(message){$('toast').textContent=message;$('toast').hidden=false;clearTimeout(app.toastTimer);app.toastTimer=setTimeout(()=>$('toast').hidden=true,5000);}
function stationColor(s){return s.occupied_connector_count>0 ? "#16886c" : s.price_per_kwh == null ? "#aab7bd" : "#617a88";}
function popup(s){return `<div class="popup-network">${esc(s.operator_names || "Network not provided")}</div><div>${esc(s.display_name)}</div>${s.station_name&&s.address?`<div>${esc(s.address)}</div>`:""}<div class="popup-price">${s.price_per_kwh==null?"Price not provided":"$"+fmt(s.price_per_kwh,2)+" / kWh"}</div><div class="popup-meta">${esc(s.plug_types || "Plug types not provided")}<br>${s.occupied_connector_count} of ${s.connector_count} connectors in use</div>`;}

function matchingStations(){
  if(!app.snapshot)return [];
  const query=$('station-search').value.toLowerCase().trim();
  const operator=$('operator-filter').value;
  return app.snapshot.stations.filter(s=>(!query || [s.display_name,s.address,s.operator_names,s.station_id,s.sa4_name].some(x=>String(x||"").toLowerCase().includes(query))) && (!operator || s.operator_names===operator) && (!$('active-only').checked || s.occupied_connector_count>0));
}
function syncMarkers(){
  const filtered=matchingStations();
  const ids=new Set(filtered.map(s=>s.station_id));
  for(const [sid, marker] of app.markers){if(!ids.has(sid)){markerLayer.removeLayer(marker);app.markers.delete(sid);}}
  for(const s of filtered){
    let marker=app.markers.get(s.station_id);
    if(!marker){
      marker=L.circleMarker([s.latitude,s.longitude],{radius:6,weight:2,color:"#fff",fillOpacity:.95,fillColor:stationColor(s)}).addTo(markerLayer);
      marker.on("click",()=>selectStation(s.station_id,true));
      const el=marker.getElement();
      if(el){el.setAttribute("tabindex","0");el.setAttribute("role","button");el.addEventListener("keydown",e=>{if(e.key==="Enter"||e.key===" "){e.preventDefault();selectStation(s.station_id,true);}});}
      app.markers.set(s.station_id,marker);
    }
    marker.setStyle({fillColor:stationColor(s),radius:s.station_id===app.selectedStation?8:6});
    marker.bindTooltip(`${s.price_per_kwh==null?"Price unavailable":"$"+fmt(s.price_per_kwh,2)+"/kWh"} · ${s.occupied_connector_count} in use`,{direction:"top"});
    if(marker.isPopupOpen())marker.setPopupContent(popup(s));
    else marker.bindPopup(popup(s),{maxWidth:250});
    const el=marker.getElement();
    if(el)el.setAttribute("aria-label",`${s.display_name}, ${s.price_per_kwh==null?"price unavailable":"AUD "+fmt(s.price_per_kwh,2)+" per kWh"}, ${s.occupied_connector_count} connectors in use`);
  }
  // Keep occupied and selected stations clickable when nearby sites overlap.
  for(const s of filtered){if(s.occupied_connector_count>0)app.markers.get(s.station_id)?.bringToFront();}
  app.markers.get(app.selectedStation)?.bringToFront();
  $('shown-count').textContent=`${fmt(filtered.length)} shown`;
  $('map-empty').hidden=filtered.length>0;
  updatePriceFlag();
}
function updatePriceFlag(){
  if(selectedPrice){map.removeLayer(selectedPrice);selectedPrice=null;}
  const s=app.snapshot?.stations.find(x=>x.station_id===app.selectedStation);
  if(s&&app.markers.has(s.station_id)){
    selectedPrice=L.tooltip({permanent:true,direction:"top",offset:[0,-11],className:"price-flag"})
      .setLatLng([s.latitude,s.longitude]).setContent(s.price_per_kwh==null?"Price unavailable":`$${fmt(s.price_per_kwh,2)} / kWh`).addTo(map);
  }
}
function selectStation(sid, openPopup=false){
  if(sid!==app.selectedStation){app.selectedSession=null;app.sessionPinned=false;}
  app.selectedStation=sid;
  const s=app.snapshot?.stations.find(x=>x.station_id===sid);
  if(s){map.panTo([s.latitude,s.longitude]);if(openPopup)app.markers.get(sid)?.openPopup();}
  syncMarkers();loadDetail();
}
async function loadDetail(){
  if(!app.selectedStation)return;
  const sid=app.selectedStation;
  const sequence=++app.detailSequence;
  const suffix=app.sessionPinned&&app.selectedSession?`?session_id=${encodeURIComponent(app.selectedSession)}`:"";
  try{
    const data=await api(`/api/stations/${encodeURIComponent(sid)}${suffix}`);
    if(sequence!==app.detailSequence||sid!==app.selectedStation)return;
    app.selectedSession=data.session_id;
    renderDetail(data);
  }catch(error){
    if(sequence!==app.detailSequence)return;
    if(app.sessionPinned&&app.selectedSession){app.selectedSession=null;app.sessionPinned=false;return loadDetail();}
    notify(error.message);
  }
}
function renderDetail(data){
  const s=data.station, live=data.state;
  $('station-network').textContent=s.operator_names||"NETWORK NOT PROVIDED";
  $('station-title').textContent=s.station_name||s.address||s.station_id;
  $('station-address').textContent=s.address||"Address not provided";
  $('station-address').hidden=!s.station_name;
  $('station-id').textContent=s.station_id;
  $('station-availability').textContent=live.occupied_connector_count>0?"In use":"Available";
  $('station-availability').classList.toggle("idle",live.occupied_connector_count===0);
  $('station-price').textContent=live.price_per_kwh==null?"—":"$"+fmt(live.price_per_kwh,2);
  $('station-plugs').textContent=s.plug_types||"Not provided";
  $('station-connectors').textContent=`${live.occupied_connector_count} / ${s.connector_count}`;
  $('station-power').textContent=fmt(s.max_rating_kw)+" kW";
  $('station-region').textContent=s.sa4_name||"Not provided";
  const select=$('session-select');
  const options=data.available_sessions.map(x=>({value:x.session_id,label:`${x.is_active?"Live":"History"} · Connector ${x.connector_id} · ${String(x.start_time).slice(5,16).replace("T"," ")}`}));
  if(!options.length)options.push({value:"",label:"No recorded session"});
  const key=JSON.stringify(options);
  if(select.dataset.options!==key){select.replaceChildren(...options.map(x=>new Option(x.label,x.value)));select.dataset.options=key;}
  select.value=data.session_id||"";
  $('curve-status').textContent=data.curve_status==="active"?"LIVE":data.curve_status==="completed"?"COMPLETED":"NO SESSION";
  $('curve-status').classList.toggle("completed",data.curve_status!=="active");
  $('curve-explanation').textContent=data.curve_status==="active"?"Live session: the curve updates as new meter events arrive.":data.curve_status==="completed"?"Historical session: recorded meter readings from a completed charge.":"No charging readings are stored for this station in this database.";
  app.lastDetail=data;
  renderChart(data);
}
function renderChart(data){
  const units={energy_kwh:{unit:"kWh delivered",short:"kWh",digits:2},power_kw:{unit:"kW charging power",short:"kW",digits:1},soc_pct:{unit:"% battery",short:"%",digits:1}};
  const m=units[app.measure];
  const points=data.curve.filter(x=>Number.isFinite(x[app.measure]));
  const latest=points.at(-1);
  $('curve-value').textContent=latest?fmt(latest[app.measure],m.digits):"—";
  $('curve-unit').textContent=m.unit;
  $('curve-points').textContent=`${points.length} meter reading${points.length===1?"":"s"}`;
  $('curve-time').textContent=latest?`Sim. ${clock(latest.event_time)}`:"";
  const container=$('curve-chart');
  if(!points.length){container.innerHTML=`<p class="chart-empty">${data.curve_status==="active"?"Waiting for the first meter reading…":"No recorded charging session at this station."}</p>`;return;}
  const width=310,height=144,left=34,right=12,top=12,bottom=25;
  const plotW=width-left-right,plotH=height-top-bottom;
  const times=points.map(x=>Date.parse(x.event_time));
  const t0=times[0],span=times.at(-1)-t0;
  const max=app.measure==="soc_pct"?100:Math.max(1,Math.ceil(Math.max(...points.map(x=>x[app.measure]))*1.15/5)*5);
  const x=(i)=>left+(span?(times[i]-t0)/span:.5)*plotW;
  const y=(v)=>top+plotH-v/max*plotH;
  const coordinates=points.map((p,i)=>`${x(i).toFixed(2)},${y(p[app.measure]).toFixed(2)}`);
  let grid="";
  for(let i=0;i<=4;i++){const val=max*i/4;const yy=y(val);grid+=`<line x1="${left}" x2="${width-right}" y1="${yy}" y2="${yy}" stroke="#edf2ef"/><text x="${left-7}" y="${yy+3}" text-anchor="end" fill="#9ba9ae" font-size="8">${fmt(val,val%1?1:0)}</text>`;}
  const labels=[0,Math.floor((points.length-1)/2),points.length-1].filter((v,i,a)=>a.indexOf(v)===i);
  const timesSvg=labels.map(i=>`<text x="${x(i)}" y="${height-7}" text-anchor="middle" fill="#9ba9ae" font-size="8">${clock(points[i].event_time)}</text>`).join("");
  const area=`${left},${top+plotH} ${coordinates.join(" ")} ${x(points.length-1)},${top+plotH}`;
  container.innerHTML=`<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(m.short)} curve with ${points.length} meter readings"><title>Charging ${esc(m.short)} from ${clock(points[0].event_time)} to ${clock(latest.event_time)}</title>${grid}<polygon points="${area}" fill="#16886c" opacity=".07"/><polyline points="${coordinates.join(" ")}" fill="none" stroke="#16886c" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>${points.map((p,i)=>`<circle cx="${x(i)}" cy="${y(p[app.measure])}" r="${i===points.length-1?3.3:2}" fill="#16886c" stroke="white" stroke-width="1"><title>${clock(p.event_time)} · ${fmt(p[app.measure],m.digits)} ${m.short}</title></circle>`).join("")}${timesSvg}</svg>`;
}
function renderActivity(data){
  const active=data.stations.filter(x=>x.active_session_count>0);
  const box=$('active-stations');box.replaceChildren();
  if(!active.length){const span=document.createElement('span');span.className='muted';span.textContent="No active sessions. Completed sessions remain in historical totals.";box.append(span);return;}
  for(const s of active){const button=document.createElement('button');button.className='active-station';button.innerHTML=`<span class="tiny-dot"></span><span><strong>${esc(s.operator_names)} · ${esc(s.address)}</strong><small>${s.active_session_count} active · $${fmt(s.price_per_kwh,2)}/kWh</small></span>`;button.addEventListener('click',()=>selectStation(s.station_id,true));box.append(button);}
}
function renderSnapshot(data){
  const first=!app.snapshot;
  app.snapshot=data;
  $('connection').className=`connection ${data.status}`;
  $('connection-text').textContent=({connected:"Live connection",historical:"Read-only history",reconnecting:"Reconnecting",error:"Connection error"})[data.status]||"Connecting";
  $('data-label').textContent=data.data_label;
  $('station-count').textContent=fmt(data.stations.length);
  $('metric-active').textContent=fmt(data.current_metrics.active_session_count);
  $('metric-active-sub').textContent=`${fmt(data.current_metrics.current_power_kw,1)} kW across ${fmt(data.current_metrics.active_station_count)} station${data.current_metrics.active_station_count===1?"":"s"}`;
  $('metric-completed').textContent=fmt(data.historical_metrics.completed_session_count);
  $('metric-energy').textContent=fmt(data.historical_metrics.total_energy_kwh,1);
  $('metric-revenue').textContent=fmt(data.historical_metrics.total_revenue_aud,2);
  $('demo-controls').hidden=!["demo","unified"].includes(data.mode);
  $('replay-context').hidden=!data.replay_context;
  if(data.replay_context){const c=data.replay_context;$('replay-context').textContent=`C database history + A MQTT replay. ${c.selected_session_count} sessions at ${c.station_ids.length} stations are reserved for replay in this private copy: ${fmt(c.baseline_historical_metrics.completed_session_count)} completed before replay, ${fmt(c.target_historical_metrics.completed_session_count)} after. The original database is unchanged.`;}
  $('run-demo').disabled=data.replay.running||data.replay.finished||data.status!=="connected";
  $('run-demo').innerHTML=data.replay.running?'<span aria-hidden="true">◌</span> Replaying…':data.replay.finished?'<span aria-hidden="true">✓</span> Replay complete':'<span aria-hidden="true">▶</span> Run demo';
  $('reset-demo').disabled=data.replay.running;
  const error=data.error||data.replay.error;
  $('notice').hidden=!error;
  if(error)$('notice').textContent=error;
  if(first){
    const operators=[...new Set(data.stations.map(s=>s.operator_names).filter(Boolean))].sort();
    $('operator-filter').append(...operators.map(x=>new Option(x,x)));
  }
  if(!app.selectedStation){const requested=new URLSearchParams(window.location.search).get('station')||data.replay_context?.station_ids[0];app.selectedStation=(data.stations.find(s=>s.station_id===requested)||data.stations.find(s=>s.active_session_count>0)||data.stations[0])?.station_id;const s=data.stations.find(s=>s.station_id===app.selectedStation);if(s)map.setView([s.latitude,s.longitude],11);}
  syncMarkers();renderActivity(data);loadDetail();
  $('last-update').textContent=`Updated ${new Date().toLocaleTimeString("en-AU",{hour12:false})}`;
}
async function refresh(){
  if(app.busy)return;
  app.busy=true;
  try{renderSnapshot(await api('/api/snapshot'));}
  catch(error){$('notice').hidden=false;$('notice').textContent=`Dashboard unavailable. ${error.message}`;$('connection').className='connection error';$('connection-text').textContent='Disconnected';}
  finally{app.busy=false;}
}
for(const id of ['station-search','operator-filter','active-only'])$(id).addEventListener(id==='station-search'?'input':'change',syncMarkers);
$('fit-stations').addEventListener('click',()=>{const stations=matchingStations();if(stations.length)map.fitBounds(L.latLngBounds(stations.map(s=>[s.latitude,s.longitude])),{padding:[35,35],maxZoom:13});});
$('focus-active').addEventListener('click',()=>{const active=app.snapshot?.stations.find(s=>s.active_session_count>0);if(active){selectStation(active.station_id,true);map.setView([active.latitude,active.longitude],13);}else notify('No active charging sessions right now.');});
$('session-select').addEventListener('change',e=>{app.selectedSession=e.target.value||null;app.sessionPinned=!!app.selectedSession;loadDetail();});
for(const button of document.querySelectorAll('[data-measure]'))button.addEventListener('click',()=>{app.measure=button.dataset.measure;for(const other of document.querySelectorAll('[data-measure]')){const selected=other===button;other.classList.toggle('active',selected);other.setAttribute('aria-pressed',String(selected));}if(app.lastDetail)renderChart(app.lastDetail);});
$('run-demo').addEventListener('click',async()=>{try{$('run-demo').disabled=true;await api('/api/demo/replay',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});notify('Demo started. Watch prices, sessions and the live curve.');await refresh();}catch(error){notify(error.message);}});
$('reset-demo').addEventListener('click',async()=>{try{$('reset-demo').disabled=true;await api('/api/demo/reset',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});app.selectedStation=null;app.selectedSession=null;app.sessionPinned=false;await refresh();notify('Demo restored to its starting state.');}catch(error){notify(error.message);}});
refresh();setInterval(refresh,1000);

/* Switch across three live stations, then reconcile C history in the same UI. */
const {chromium}=require('playwright');
const fs=require('node:fs');
const path=require('node:path');
const assert=require('node:assert/strict');
(async()=>{
 const base=process.env.DASHBOARD_BASE_URL||'http://127.0.0.1:8050';
 const out=process.env.DASHBOARD_SCREENSHOTS||path.resolve('A2/docs/images');
 fs.mkdirSync(out,{recursive:true});
 const browser=await chromium.launch({headless:true,executablePath:process.env.DASHBOARD_BROWSER_PATH||'C:/Program Files/Google/Chrome/Application/chrome.exe'});
 const page=await browser.newPage({viewport:{width:1440,height:1120},deviceScaleFactor:1});
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 try{
  await page.goto(base,{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>document.querySelector('#metric-completed')?.textContent==='5,081');
  const initial=await (await page.request.get(base+'/api/snapshot')).json();
  const context=initial.replay_context;
  assert.equal(initial.mode,'unified');
  assert.equal(await page.locator('#demo-controls').isVisible(),true);
  assert.equal(await page.locator('.leaflet-interactive').count(),414);
  assert.equal(await page.locator('#metric-active').innerText(),'0');
  assert.match(await page.locator('#replay-context').innerText(),/3 sessions at 3 stations/);
  assert.equal(await page.locator('#station-id').innerText(),context.station_ids[0]);
  await page.screenshot({path:path.join(out,'unified_initial.png'),fullPage:true});
  // Ordinary, non-replayed stations must expose history when clicked on the map.
  const historicalStations=initial.stations.filter(s=>!context.station_ids.includes(s.station_id)).slice(0,3);
  const observedHistory=[];
  for(const station of historicalStations){
   await page.locator('#station-search').fill(station.station_id);
   await page.waitForFunction(()=>document.querySelectorAll('.leaflet-interactive').length===1);
   await page.locator('#fit-stations').click();
   await page.locator('.leaflet-interactive').click({force:true});
   await page.waitForFunction(sid=>document.querySelector('#station-id').textContent===sid&&document.querySelector('#curve-status').textContent==='COMPLETED',station.station_id);
   assert.equal(await page.locator('#curve-chart svg').count(),1);
   assert.match(await page.locator('#curve-explanation').innerText(),/Historical session/);
   const detail=await (await page.request.get(base+'/api/stations/'+station.station_id)).json();
   const older=detail.available_sessions.at(-1).session_id;
   await page.locator('#session-select').selectOption(older);
   await page.waitForTimeout(1400); // A refresh must preserve a manually selected history.
   assert.equal(await page.locator('#session-select').inputValue(),older);
   const selected=await (await page.request.get(base+'/api/stations/'+station.station_id+'?session_id='+encodeURIComponent(older))).json();
   assert.ok(selected.curve.length>0);
   assert.ok(selected.curve.every(p=>p.station_id===station.station_id&&p.session_id===older));
   observedHistory.push({station_id:station.station_id,selected_session_id:older,meter_points:selected.curve.length});
  }
  await page.screenshot({path:path.join(out,'unified_other_station_history.png'),fullPage:true});
  // Selecting a new station clears the manual pin; its new live session wins.
  await page.locator('#station-search').fill(context.station_ids[0]);
  await page.waitForFunction(()=>document.querySelectorAll('.leaflet-interactive').length===1);
  await page.locator('#fit-stations').click();
  await page.locator('.leaflet-interactive').click({force:true});
  await page.waitForFunction(sid=>document.querySelector('#station-id').textContent===sid,context.station_ids[0]);
  await page.locator('#station-search').fill('');
  await page.locator('#run-demo').click();
  await page.waitForFunction(()=>document.querySelector('#metric-active').textContent==='3'&&document.querySelector('#curve-status').textContent==='LIVE'&&parseInt(document.querySelector('#curve-points').textContent)>=3,{},{timeout:20000});
  const observed=[];
  for(let i=0;i<context.station_ids.length;i++){
   const station=context.station_ids[i];
   const record=initial.stations.find(s=>s.station_id===station);
   await page.locator('.active-station').filter({hasText:record.address}).click();
   await page.waitForFunction(sid=>document.querySelector('#station-id').textContent===sid&&document.querySelector('#curve-status').textContent==='LIVE'&&parseInt(document.querySelector('#curve-points').textContent)>0,station);
   const detail=await (await page.request.get(base+'/api/stations/'+station)).json();
   assert.equal(detail.session_id,context.session_ids[i]);
   assert.ok(detail.curve.every(p=>p.station_id===station));
   assert.equal(await page.locator('#curve-chart svg').count(),1);
   observed.push({station_id:station,session_id:detail.session_id,curve_points:detail.curve.length});
   await page.locator('#toast').waitFor({state:'hidden'});
   await page.screenshot({path:path.join(out,`unified_live_station_${i+1}.png`),fullPage:true});
  }
  await page.waitForFunction(()=>document.querySelector('#run-demo').textContent.includes('Replay complete'),{},{timeout:60000});
  assert.equal(await page.locator('#metric-active').innerText(),'0');
  assert.equal(await page.locator('#metric-completed').innerText(),'5,084');
  assert.equal(await page.locator('#metric-energy').innerText(),'129,422.2');
  assert.equal(await page.locator('#metric-revenue').innerText(),'88,908.69');
  assert.equal(await page.locator('#curve-status').innerText(),'COMPLETED');
  await page.screenshot({path:path.join(out,'unified_completed.png'),fullPage:true});
  const final=await (await page.request.get(base+'/api/snapshot')).json();
  await page.locator('#reset-demo').click();
  await page.waitForFunction(()=>document.querySelector('#metric-completed').textContent==='5,081'&&document.querySelector('#metric-active').textContent==='0');
  const reset=await (await page.request.get(base+'/api/snapshot')).json();
  assert.deepEqual(errors,[]);
  const result={passed:true,real_mqtt:true,mode:'unified',sample_events:context.event_count,
    provenance:context,initial_historical:initial.historical_metrics,stations_observed_live:observed,
    non_replay_stations_with_history:observedHistory,manual_history_selection_persists:true,history_switches_automatically_to_new_live_session:true,
    final_historical:final.historical_metrics,final_current:final.current_metrics,final_replay:final.replay,
    reset_historical:reset.historical_metrics,page_errors:errors,
    scope:'Three unchanged A sessions at three stations; C baseline and live replay in one page. Not a full-stream paced MQTT run.'};
  fs.writeFileSync(path.join(out,'unified_browser_validation.json'),JSON.stringify(result,null,2));
  console.log(JSON.stringify(result,null,2));
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});

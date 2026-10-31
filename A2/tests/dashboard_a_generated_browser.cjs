/* Acceptance of A's unmodified generated 36-event session through D's UI. */
const {chromium}=require('playwright');
const fs=require('node:fs');
const path=require('node:path');
const assert=require('node:assert/strict');
(async()=>{
 const base=process.env.DASHBOARD_BASE_URL||'http://127.0.0.1:8051';
 const history=process.env.DASHBOARD_HISTORY_URL||'http://127.0.0.1:8052';
 const out=process.env.DASHBOARD_SCREENSHOTS||path.resolve('A2/docs/images');
 fs.mkdirSync(out,{recursive:true});
 const browser=await chromium.launch({headless:true,executablePath:process.env.DASHBOARD_BROWSER_PATH||'C:/Program Files/Google/Chrome/Application/chrome.exe'});
 const page=await browser.newPage({viewport:{width:1440,height:1120},deviceScaleFactor:1});
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 try{
  await page.goto(base+'/?station=ST4D056616A3',{waitUntil:'domcontentloaded',timeout:15000});
  await page.waitForFunction(()=>document.querySelector('#station-title')?.textContent.includes('73 Belmore'));
  assert.equal(await page.locator('#metric-active').innerText(),'0');
  assert.equal(await page.locator('#metric-completed').innerText(),'0');
  assert.equal(await page.locator('#station-price').innerText(),'—');
  assert.equal(await page.locator('.leaflet-interactive').count(),414);
  await page.screenshot({path:path.join(out,'a_generated_initial.png'),fullPage:true});
  await page.locator('#run-demo').click();
  await page.waitForFunction(()=>parseInt(document.querySelector('#curve-points').textContent)>=8,{},{timeout:20000});
  await page.locator('.leaflet-interactive[aria-label^="73 Belmore Road"]').click();
  await page.waitForTimeout(400);
  assert.equal(await page.locator('#station-price').innerText(),'$0.74');
  assert.equal(await page.locator('#metric-active').innerText(),'1');
  assert.equal(await page.locator('#metric-completed').innerText(),'0');
  assert.equal(await page.locator('#station-plugs').innerText(),'Not provided');
  await page.locator('#toast').waitFor({state:'hidden'});
  await page.screenshot({path:path.join(out,'a_generated_live.png'),fullPage:true});
  const during=await (await page.request.get(base+'/api/snapshot')).json();
  const liveCurve=await (await page.request.get(base+'/api/stations/ST4D056616A3')).json();
  await page.waitForFunction(()=>document.querySelector('#run-demo').textContent.includes('Replay complete'),{},{timeout:40000});
  assert.equal(await page.locator('#metric-active').innerText(),'0');
  assert.equal(await page.locator('#metric-completed').innerText(),'1');
  assert.equal(await page.locator('#metric-energy').innerText(),'22.6');
  assert.equal(await page.locator('#metric-revenue').innerText(),'16.72');
  assert.equal(await page.locator('#curve-status').innerText(),'COMPLETED');
  assert.equal(await page.locator('#curve-points').innerText(),'32 meter readings');
  assert.equal(await page.locator('#curve-value').innerText(),'22.60');
  await page.screenshot({path:path.join(out,'a_generated_completed.png'),fullPage:true});
  const final=await (await page.request.get(base+'/api/snapshot')).json();
  await page.locator('#reset-demo').click();
  await page.waitForFunction(()=>document.querySelector('#metric-completed').textContent==='0'&&document.querySelector('#station-price').textContent==='—');
  const reset=await (await page.request.get(base+'/api/snapshot')).json();
  assert.equal(reset.current_metrics.active_session_count,0);
  await page.goto(history,{waitUntil:'domcontentloaded',timeout:15000});
  await page.waitForFunction(()=>document.querySelector('#metric-completed').textContent==='5,084');
  assert.equal(await page.locator('#metric-energy').innerText(),'129,422.2');
  assert.equal(await page.locator('#metric-revenue').innerText(),'88,908.69');
  assert.equal(await page.locator('#connection-text').innerText(),'Read-only history');
  assert.equal(await page.locator('#demo-controls').isVisible(),false);
  await page.locator('#fit-stations').click();
  try{await page.waitForFunction(()=>[...document.querySelectorAll('.leaflet-tile')].every(x=>x.complete),{},{timeout:6000});}catch{}
  await page.screenshot({path:path.join(out,'a_generated_formal_history.png'),fullPage:true});
  const formal=await (await page.request.get(history+'/api/snapshot')).json();
  assert.deepEqual(errors,[]);
  const report={passed:true,source:'Unmodified Member A generated event_stream.jsonl',sample_events:36,
   real_mqtt:true,during_current:during.current_metrics,during_historical:during.historical_metrics,
   during_curve_points:liveCurve.curve.length,final_current:final.current_metrics,final_historical:final.historical_metrics,
   final_replay:final.replay,final_curve_points:32,reset_current:reset.current_metrics,reset_historical:reset.historical_metrics,
   formal_current:formal.current_metrics,formal_historical:formal.historical_metrics,page_errors:errors,
   scope:'36-event real MQTT/UI replay plus full-database read-only view; not full-stream paced MQTT replay'};
  fs.writeFileSync(path.join(out,'a_generated_browser_validation.json'),JSON.stringify(report,null,2));
  console.log(JSON.stringify(report,null,2));
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});

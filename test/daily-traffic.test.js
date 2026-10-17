import test from 'node:test';
import assert from 'node:assert/strict';
import { Miniflare } from 'miniflare';
import { sumDailyTraffic, trafficDay, attachDailyTraffic } from '../src/database/dailyTraffic.js';
import { buildHistoryId } from '../src/database/indexOptimization.js';
const now = Date.UTC(2026,9,18,1), start=trafficDay(now).start;
test('daily observed counters exclude yesterday, preserve zero, handle resets and independent missing fields',()=>{
 const rows=[{timestamp:start-1000,net_rx:9999,net_tx:9999},{timestamp:start+1000,net_rx:100,net_tx:200},{timestamp:start+2000,net_rx:160,net_tx:null},{timestamp:start+3000,net_rx:10,net_tx:300}];
 const value=sumDailyTraffic(rows,{timestamp:now,net_rx:20,net_tx:320},now);
 assert.equal(value.net_rx_daily,80);assert.equal(value.net_tx_daily,120);
 assert.equal(value.traffic_daily_day,'2026-10-18');assert.equal(value.traffic_daily_partial,true);assert.equal(value.traffic_daily_since,start+1000);
 assert.equal(sumDailyTraffic([{timestamp:now,net_rx:0}],null,now).net_rx_daily,0);
 assert.equal(sumDailyTraffic([],null,now).net_rx_daily,undefined);
 assert.equal(trafficDay(start-1).day,'2026-10-17');
});
test('D1 daily range merges weekly tables, caches history, and includes a fresh counter without rereading D1',async()=>{
 const mf=new Miniflare({modules:true,script:'export default {fetch(){return new Response("ok")}}',compatibilityDate:'2024-12-01',d1Databases:['DB']});
 try {
  const db=await mf.getD1Database('DB');
  await db.exec('CREATE TABLE metrics_history(id INTEGER PRIMARY KEY,server_id TEXT,timestamp INTEGER,net_rx REAL,net_tx REAL)');
  await db.exec('CREATE TABLE metrics_history_old(id INTEGER PRIMARY KEY,server_id TEXT,timestamp INTEGER,net_rx REAL,net_tx REAL)');
  const times=[start+1000,Date.UTC(2026,9,18,0,0,1)];
  for (let i=0;i<2;i++) await db.prepare(`INSERT INTO ${i?'metrics_history':'metrics_history_old'} VALUES (?,?,?,?,?)`).bind(buildHistoryId(1,times[i]),'one',times[i],100+i*50,200+i*60).run();
  const server={id:'one',history_partition_id:1,timestamp:start-86400000,last_updated:now,net_rx:200,net_tx:300};
  await attachDailyTraffic(db,[server],now);
  assert.equal(server.net_rx_daily,100);assert.equal(server.net_tx_daily,100);
  await db.exec('DROP TABLE metrics_history');await db.exec('DROP TABLE metrics_history_old');
  const newer={...server,last_updated:now+1000,net_rx:220,net_tx:310};
  await attachDailyTraffic(db,[newer],now+1000);
  assert.equal(newer.net_rx_daily,120);assert.equal(newer.net_tx_daily,110);
 } finally { await mf.dispose(); }
});

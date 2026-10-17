import { getServerHistoryInfo, getHistoryIdRange } from './indexOptimization.js';

const DAY = 86400000, OFFSET = 8 * 3600000, TTL = 300000;
const caches = new WeakMap();
const number = value => value !== null && value !== undefined && value !== false && value !== '' && Number.isFinite(Number(value)) && Number(value) >= 0 ? Number(value) : null;
const time = value => Number(value) < 1e12 ? Number(value) * 1000 : Number(value);
export function trafficDay(now) {
  const start = Math.floor((now + OFFSET) / DAY) * DAY - OFFSET;
  const date = new Date(start + OFFSET);
  return { start, end: start + DAY, day: `${date.getUTCFullYear()}-${String(date.getUTCMonth()+1).padStart(2,'0')}-${String(date.getUTCDate()).padStart(2,'0')}` };
}
// Counter deltas are measured between real snapshots. Do not attribute the
// unobserved interval before the first daily snapshot to today's usage.
export function sumDailyTraffic(rows, latest, now = Date.now()) {
  const { start, end, day } = trafficDay(now);
  const points = new Map();
  for (const row of [...rows, latest]) {
    if (!row) continue;
    const timestamp = time(row.last_updated ?? row.timestamp);
    if (timestamp >= start && timestamp <= now) points.set(timestamp, { ...row, timestamp });
  }
  const ordered = [...points.values()].sort((a,b)=>a.timestamp-b.timestamp);
  const result = { traffic_daily_day: day, traffic_daily_until: end, traffic_daily_timezone: 'Asia/Shanghai', traffic_daily_partial: true };
  for (const direction of ['rx','tx']) {
    let previous = null, total = 0, since = null;
    for (const row of ordered) {
      const value = number(row[`net_${direction}`]);
      if (value === null) continue;
      since ??= row.timestamp;
      if (previous !== null) total += value >= previous ? value - previous : value;
      previous = value;
    }
    if (since !== null) { result[`net_${direction}_daily`] = total; result.traffic_daily_since = Math.max(result.traffic_daily_since || 0, since); }
  }
  result.traffic_daily_at = ordered.at(-1)?.timestamp;
  return result;
}
async function readDay(db, server, now) {
  const { start } = trafficDay(now);
  const info = await getServerHistoryInfo(db, server.id, server);
  const begin = Math.max(start, info.startTimestamp);
  const range = info.partitionId ? getHistoryIdRange(info.partitionId, begin, now) : null;
  const tables = ['metrics_history'];
  const sunday = new Date(now); sunday.setUTCHours(0,0,0,0); sunday.setUTCDate(sunday.getUTCDate()-sunday.getUTCDay());
  if (begin < sunday.getTime() && await db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='metrics_history_old'").first()) tables.push('metrics_history_old');
  const queries = tables.map(table => range
    ? db.prepare(`SELECT timestamp,net_rx,net_tx FROM ${table} WHERE id BETWEEN ? AND ? AND server_id = ? AND timestamp >= ? AND timestamp <= ? ORDER BY id`).bind(range.startId,range.endId,server.id,begin,now)
    : db.prepare(`SELECT timestamp,net_rx,net_tx FROM ${table} WHERE server_id = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp`).bind(server.id,begin,now));
  const results = await db.batch(queries);
  return results.flatMap(result=>result.results || []);
}
export async function attachDailyTraffic(db, servers, now = Date.now()) {
  let cache = caches.get(db); if (!cache) { cache = new Map(); caches.set(db,cache); }
  // Three bounded workers; cache the in-flight promise as well as the result.
  let cursor = 0;
  await Promise.all(Array.from({length:Math.min(3,servers.length)},async()=>{
    while (cursor < servers.length) {
      const server = servers[cursor++], key = `${server.id}:${server.timestamp || 0}:${trafficDay(now).day}`;
      try {
        let entry = cache.get(key);
        if (!entry || now-entry.at >= TTL) {
          entry = { at:now, rows:readDay(db,server,now) }; cache.delete(key); cache.set(key,entry);
          if (cache.size > 256) cache.delete(cache.keys().next().value);
        }
        Object.assign(server,sumDailyTraffic(await entry.rows,server,now));
      } catch (error) { cache.delete(key); console.warn('[DailyTraffic] History unavailable:',error?.message || error); }
    }
  }));
}

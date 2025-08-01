// /api/index.js
import 'dotenv/config';
import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const EXCLUDED_KEYWORDS = ['wrapped', 'staked', 'WETH'];
const ONE_DAY_MS      = 24 * 60 * 60 * 1000;
const FIVE_YEARS_MS   = 5 * 365 * ONE_DAY_MS;  // approx

async function fetchKlines(symbol, startTime, endTime) {
  const all = [];
  let cursor = startTime;
  while (cursor < endTime) {
    const chunkEnd = Math.min(endTime, cursor + 1000 * ONE_DAY_MS);
    const url = new URL('https://api.binance.com/api/v3/klines');
    url.searchParams.set('symbol', symbol);
    url.searchParams.set('interval', '1d');
    url.searchParams.set('startTime', String(cursor));
    url.searchParams.set('endTime', String(chunkEnd));
    url.searchParams.set('limit', '1000');

    console.log(`  → [${symbol}] fetching ${new Date(cursor).toISOString()} → ${new Date(chunkEnd).toISOString()}`);
    const resp = await fetch(url);
    const klines = await resp.json();
    if (!Array.isArray(klines)) {
      console.warn(`    ! Binance error chunk for ${symbol}:`, klines);
      break;
    }
    all.push(...klines);
    if (klines.length < 1000) break;
    cursor = klines[klines.length - 1][0] + ONE_DAY_MS;
  }
  return all;
}

export default async function handler(_, res) {
  try {
    console.log('=== START BINANCE OHLCV SYNC ===');

    // 1) load exchangeInfo once
    const exchangeInfo = await fetch('https://api.binance.com/api/v3/exchangeInfo')
      .then(r => r.json());
    const validPairs = new Set(exchangeInfo.symbols.map(s => s.symbol));

    // 2) load top 1000 snapshots by market_cap
    const { data: snaps, error: snapErr } = await supabase
      .from('snapshot')
      .select('symbol, name')
      .order('market_cap', { ascending: false })
      .limit(1000);
    if (snapErr) throw snapErr;
    console.log(`Fetched top ${snaps.length} coins by market_cap.`);

    // 3) build & filter target pairs
    const pairs = snaps
      .filter(s => !EXCLUDED_KEYWORDS.some(k => s.name.toLowerCase().includes(k)))
      .map(s => s.symbol.toUpperCase() + 'USDT')
      .filter(sym => validPairs.has(sym));
    console.log(`→ ${pairs.length} valid USDT pairs after filtering.`);

    const now = Date.now();
    const globalStart = now - FIVE_YEARS_MS;

    for (const symbol of pairs) {
      console.log(`\n-- Processing ${symbol} --`);

      // A) get existing range in our DB
      const { data: range, error: rangeErr } = await supabase
        .from('binance_ohlcv_1d')
        .select('min(open_time) as earliest, max(open_time) as latest')
        .eq('symbol', symbol)
        .single();
      if (rangeErr) {
        console.warn(`  ! Could not get existing range for ${symbol}:`, rangeErr);
        continue;
      }

      const earliestMs = range.earliest ? new Date(range.earliest).getTime() : null;
      const latestMs   = range.latest   ? new Date(range.latest).getTime()   : null;

      // B) decide backfill & update windows
      const tasks = [];

      if (!earliestMs) {
        tasks.push({ from: globalStart, to: now });
        console.log(`  • No existing data; will fetch full 5y→now.`);
      } else if (earliestMs > globalStart + ONE_DAY_MS) {
        tasks.push({ from: globalStart, to: earliestMs - ONE_DAY_MS });
        console.log(`  • Backfilling: ${new Date(globalStart).toISOString()} → ${new Date(earliestMs - ONE_DAY_MS).toISOString()}`);
      } else {
        console.log(`  • No backfill needed.`);
      }

      if (latestMs && latestMs + ONE_DAY_MS < now) {
        tasks.push({ from: latestMs + ONE_DAY_MS, to: now });
        console.log(`  • Updating: ${new Date(latestMs + ONE_DAY_MS).toISOString()} → ${new Date(now).toISOString()}`);
      } else {
        console.log(`  • No update needed.`);
      }

      // C) run fetch/upsert for each task
      for (const { from, to } of tasks) {
        const klines = await fetchKlines(symbol, from, to);
        if (!klines.length) {
          console.log(`    – No data returned for segment.`);
          continue;
        }
        const records = klines.map(k => ({
          symbol,
          open_time:     new Date(k[0]).toISOString(),
          open:          k[1], high: k[2], low: k[3], close: k[4],
          volume:        k[5],
          close_time:    new Date(k[6]).toISOString(),
          quote_asset_volume: k[7],
          number_of_trades:   k[8],
          taker_buy_base_volume:  k[9],
          taker_buy_quote_volume: k[10],
        }));
        const { error: upErr } = await supabase
          .from('binance_ohlcv_1d')
          .upsert(records);
        if (upErr) {
          console.error(`    ✖ Upsert failed:`, upErr);
        } else {
          console.log(`    ✔ Upserted ${records.length} rows.`);
        }
      }
    }

    console.log('\n=== SYNC COMPLETE ===');
    return res.status(200).json({ status: 'ok', processed: pairs.length });
  } catch (err) {
    console.error('Fatal error:', err);
    return res.status(500).json({ error: err.message });
  }
}

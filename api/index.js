// /api/index.js
import 'dotenv/config';                        // loads .env locally
import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';
import { readFile } from 'fs/promises';
import { join } from 'path';

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const EXCLUDED_KEYWORDS = ['wrapped', 'staked', 'WETH'];
const ONE_DAY_MS      = 24 * 60 * 60 * 1000;
const FIVE_YEARS_MS   = 5 * 365 * ONE_DAY_MS;  // approx

// helper: fetch klines in [start, end), chunking into <=1000 days
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
    try {
      const resp = await fetch(url);
      if (!resp.ok) {
        console.warn(`    ! Failed to fetch klines for ${symbol}: ${resp.status} ${resp.statusText}`);
        break;
      }
      const klines = await resp.json();
      if (!Array.isArray(klines)) {
        console.warn(`    ! Unexpected klines response for ${symbol}:`, klines);
        break;
      }
      all.push(...klines);
      if (klines.length < 1000) break; // no more data
      cursor = klines[klines.length - 1][0] + ONE_DAY_MS;
    } catch (err) {
      console.warn(`    ! Error fetching klines for ${symbol}: ${err.message}`);
      break;
    }
  }
  return all;
}

export default async function handler(_, res) {
  console.log('=== START BINANCE OHLCV SYNC ===');

  // 1) load and validate exchangeInfo once
  let validPairs = null;
  try {
    const respInfo = await fetch('https://api.binance.com/api/v3/exchangeInfo');
    if (!respInfo.ok) {
      console.warn(`Binance exchangeInfo fetch failed: ${respInfo.status} ${respInfo.statusText}`);
      throw new Error('Use local fallback');
    }
    const exchangeInfo = await respInfo.json();
    validPairs = new Set(exchangeInfo.symbols.map(s => s.symbol));
    console.log('Loaded validPairs from Binance API');
  } catch {
    console.warn('Falling back to local exchangeInfo.json');
    try {
      const file = await readFile(join(process.cwd(), 'exchangeInfo.json'), 'utf8');
      const exchangeInfo = JSON.parse(file);
      validPairs = new Set(exchangeInfo.symbols.map(s => s.symbol));
      console.log('Loaded validPairs from local exchangeInfo.json');
    } catch (e) {
      console.warn('Failed to load local exchangeInfo.json:', e.message);
    }
  }

  try {
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
      .filter(sym => validPairs ? validPairs.has(sym) : true);
    console.log(`→ ${pairs.length} target pairs after filtering.`);

    const now = Date.now();
    const globalStart = now - FIVE_YEARS_MS;

    for (const symbol of pairs) {
      console.log(`\n-- Processing ${symbol} --`);

      // A) get earliest & latest from DB
      let earliestMs = null;
      let latestMs = null;
      const { data: early, error: earlyErr } = await supabase
        .from('binance_ohlcv_1d')
        .select('open_time')
        .eq('symbol', symbol)
        .order('open_time', { ascending: true })
        .limit(1)
        .single();
      if (!earlyErr && early?.open_time) earliestMs = new Date(early.open_time).getTime();

      const { data: late, error: lateErr } = await supabase
        .from('binance_ohlcv_1d')
        .select('open_time')
        .eq('symbol', symbol)
        .order('open_time', { ascending: false })
        .limit(1)
        .single();
      if (!lateErr && late?.open_time) latestMs = new Date(late.open_time).getTime();

      // B) determine backfill & update windows
      const tasks = [];
      if (!earliestMs) {
        tasks.push({ from: globalStart, to: now });
        console.log('  • No existing data; full fetch.');
      } else if (earliestMs > globalStart + ONE_DAY_MS) {
        tasks.push({ from: globalStart, to: earliestMs - ONE_DAY_MS });
        console.log('  • Backfill window:', new Date(globalStart).toISOString(), '→', new Date(earliestMs - ONE_DAY_MS).toISOString());
      }
      if (latestMs && latestMs + ONE_DAY_MS < now) {
        tasks.push({ from: latestMs + ONE_DAY_MS, to: now });
        console.log('  • Update window:', new Date(latestMs + ONE_DAY_MS).toISOString(), '→', new Date(now).toISOString());
      }

      // C) fetch & upsert
      for (const { from, to } of tasks) {
        const klines = await fetchKlines(symbol, from, to);
        if (!klines.length) {
          console.log('    – No data for segment.');
          continue;
        }
        const records = klines.map(k => ({
          symbol,
          open_time: new Date(k[0]).toISOString(),
          open: k[1], high: k[2], low: k[3], close: k[4],
          volume: k[5],
          close_time: new Date(k[6]).toISOString(),
          quote_asset_volume: k[7],
          number_of_trades: k[8],
          taker_buy_base_volume: k[9],
          taker_buy_quote_volume: k[10],
        }));
        const { error: upErr } = await supabase
          .from('binance_ohlcv_1d')
          .upsert(records);
        if (upErr) console.error('    ✖ Upsert failed:', upErr);
        else console.log(`    ✔ Upserted ${records.length} rows.`);
      }
    }

    console.log('\n=== SYNC COMPLETE ===');
    return res.status(200).json({ status: 'ok', processed: pairs.length });
  } catch (err) {
    console.error('Fatal error:', err);
    return res.status(500).json({ error: err.message });
  }
}

// ----- Mock runner for standalone use -----
if (import.meta.url === `file://${process.argv[1]}`) {
  // Simple fake req/res
  const fakeReq = { method: 'GET' };
  const fakeRes = {
    status(code) {
      this._status = code;
      return this;
    },
    json(payload) {
      console.log(`\n[MockRunner] ${this._status} →`, payload);
    },
    setHeader() {/* noop */}
  };

  // Run it
  handler(fakeReq, fakeRes)
    .catch(err => console.error('[MockRunner] Fatal error:', err));
}

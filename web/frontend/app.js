// market-stream demo frontend.
//
// THE most important behaviour here is graceful degradation: the page must never look
// broken to a recruiter, even with no backend. So the boot order is:
//   1. render immediately from the bundled recorded sample (always available on the CDN);
//   2. probe the live API in parallel (short timeout);
//   3. if healthy → switch to live polling, and a watchdog reverts to the looping sample
//      on repeated failures (and keeps retrying live in the background);
//   4. if unreachable → stay on the looping sample, silently. No spinners, no errors.
(function () {
  "use strict";

  const cfg = window.MARKET_STREAM_CONFIG || {};
  const API = (cfg.API_BASE_URL || "").replace(/\/$/, "");

  const el = {
    badge: document.getElementById("badge"),
    symbol: document.getElementById("symbol"),
    price: document.getElementById("price"),
    chart: document.getElementById("chart"),
    tape: document.getElementById("tape"),
    ohlcvBody: document.querySelector("#ohlcv tbody"),
  };

  let symbol = (cfg.SYMBOLS && cfg.SYMBOLS[0]) || "AAPL";
  let mode = "sample"; // "sample" | "live"
  let sample = null;
  let sampleTimer = null;
  let liveTimers = [];
  let liveFailures = 0;
  let lastPrice = null;

  // ── rendering ────────────────────────────────────────────────────────────────
  const fmt = (n, d = 2) =>
    n == null ? "—" : Number(n).toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d });

  function setBadge(text, live) {
    el.badge.textContent = text;
    el.badge.className = "badge " + (live ? "live" : "sample");
  }

  function setPrice(p) {
    el.price.textContent = fmt(p);
    lastPrice = p;
  }

  function renderChart(rows) {
    window.drawChart(el.chart, (rows || []).map((r) => Number(r.close)));
  }

  function renderOhlcv(rows) {
    const recent = (rows || []).slice(-12).reverse();
    el.ohlcvBody.innerHTML = recent
      .map((r) => {
        const t = new Date(r.window_start_ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
        return `<tr><td>${t}</td><td>${fmt(r.open)}</td><td>${fmt(r.high)}</td><td>${fmt(r.low)}</td>` +
          `<td>${fmt(r.close)}</td><td>${fmt(r.vwap)}</td><td>${fmt(r.volume, 3)}</td><td>${r.trade_count}</td></tr>`;
      })
      .join("");
  }

  function renderTape(ticks) {
    const rows = (ticks || []).slice(0, 40);
    el.tape.innerHTML = rows
      .map((t, i) => {
        // Stocks have no maker/taker side, so colour by price direction: a tick is "up"
        // if its price is >= the next (older) tick's price.
        const prev = rows[i + 1];
        const side = !prev || Number(t.price) >= Number(prev.price) ? "up" : "down";
        return `<li><span class="px ${side}">${fmt(t.price)}</span><span class="qty">${fmt(t.quantity, 0)}</span></li>`;
      })
      .join("");
  }

  // ── sample mode (always works, no backend) ─────────────────────────────────────
  function startSample() {
    stopLive();
    mode = "sample";
    setBadge("Sample feed", false);

    renderOhlcv(sample.ohlcv[symbol] || []);
    renderChart(sample.ohlcv[symbol] || []);

    const ticks = sample.ticks[symbol] || [];
    const tape = [];
    let i = 0;
    clearInterval(sampleTimer);
    sampleTimer = setInterval(() => {
      if (!ticks.length) return;
      const src = ticks[i % ticks.length];
      i++;
      tape.unshift({ ...src });
      if (tape.length > 40) tape.pop();
      renderTape(tape);
      setPrice(src.price);
    }, 400); // loop the recorded ticks so the tape keeps moving — a frozen page looks broken
  }

  // ── live mode ──────────────────────────────────────────────────────────────────
  function stopSample() {
    clearInterval(sampleTimer);
    sampleTimer = null;
  }
  function stopLive() {
    liveTimers.forEach(clearInterval);
    liveTimers = [];
  }

  async function fetchJSON(path, timeoutMs) {
    const ctrl = new AbortController();
    const to = setTimeout(() => ctrl.abort(), timeoutMs || 4000);
    try {
      const res = await fetch(API + path, { signal: ctrl.signal, mode: "cors" });
      if (!res.ok) throw new Error("HTTP " + res.status);
      return await res.json();
    } finally {
      clearTimeout(to);
    }
  }

  function onLiveOk() {
    liveFailures = 0;
  }
  function onLiveError() {
    liveFailures += 1;
    if (mode === "live" && liveFailures >= 3) {
      // Watchdog: the backend went away mid-session → fall back, keep probing in background.
      startSample();
      scheduleProbe();
    }
  }

  async function goLive() {
    stopSample();
    mode = "live";
    liveFailures = 0;
    setBadge("Live", true);

    const pollOhlcv = async () => {
      try {
        const d = await fetchJSON(`/api/ohlcv?symbol=${symbol}&limit=120`);
        onLiveOk();
        renderOhlcv(d.rows);
        renderChart(d.rows);
      } catch (_) {
        onLiveError();
      }
    };
    const pollTicks = async () => {
      try {
        const d = await fetchJSON(`/api/ticks?symbol=${symbol}&limit=40`);
        onLiveOk();
        renderTape(d.rows);
        if (d.rows && d.rows[0]) setPrice(d.rows[0].price);
      } catch (_) {
        onLiveError();
      }
    };

    await pollOhlcv();
    await pollTicks();
    if (mode !== "live") return; // a failure during the first poll may have reverted us
    liveTimers.push(setInterval(pollOhlcv, cfg.OHLCV_POLL_MS || 5000));
    liveTimers.push(setInterval(pollTicks, cfg.TICK_POLL_MS || 2000));
  }

  async function probe() {
    if (!API) return false; // sample-only mode
    try {
      await fetchJSON("/healthz", cfg.HEALTH_TIMEOUT_MS || 1500);
      return true;
    } catch (_) {
      return false;
    }
  }

  let probeScheduled = false;
  function scheduleProbe() {
    if (probeScheduled) return;
    probeScheduled = true;
    setTimeout(async () => {
      probeScheduled = false;
      if (mode === "sample" && (await probe())) goLive();
      else if (mode === "sample") scheduleProbe();
    }, 8000);
  }

  // ── controls ─────────────────────────────────────────────────────────────────
  function populateSymbols(list) {
    el.symbol.innerHTML = list.map((s) => `<option value="${s}">${s}</option>`).join("");
    el.symbol.value = symbol;
    el.symbol.addEventListener("change", () => {
      symbol = el.symbol.value;
      mode === "live" ? goLive() : startSample();
    });
  }

  // ── boot ─────────────────────────────────────────────────────────────────────
  async function boot() {
    try {
      sample = await (await fetch("./sample/replay.json")).json();
    } catch (_) {
      sample = { symbols: cfg.SYMBOLS || [symbol], ticks: {}, ohlcv: {} };
    }
    populateSymbols(sample.symbols && sample.symbols.length ? sample.symbols : cfg.SYMBOLS || [symbol]);

    startSample(); // render instantly — page is never blank
    if (await probe()) goLive();
    else scheduleProbe();
  }

  boot();
})();

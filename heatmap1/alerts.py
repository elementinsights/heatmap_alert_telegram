#!/usr/bin/env python3
"""
Coinglass Model-2 heatmap alert runner (Telegram + Google Sheets with milestones).

Focus: longer-term moves (≈5 minutes → a few hours)

Key updates vs earlier versions:
- Timeframes narrowed to 24h and 72h (macro context).
- Default price windows widened to ±6–7% (heavier liquidity bands, fewer/noisier micro signals).
- Crossing thresholds raised to ±40/50/60%.
- Shock levels raised to Δ≥40/50/60%.
- Shock windows lengthened (24h: 180m, 72h: 480m) with moderate cooldowns (20/30m).
- Persistence gate for both crossing and shock alerts to avoid one-print spikes.
- Single API fetch per coin×timeframe; reuse for all windows (efficiency).
- Same Google Sheets milestone logging + Telegram messages as before.

ENV expected:
  COINGLASS_API_KEY
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID (or TELEGRAM_CHANNEL)
  ALERT_TAG (optional, prepended to every Telegram message)
  GOOGLE_SHEETS_ID
  GOOGLE_SHEETS_TAB (optional; default 'Alerts')
  GOOGLE_SA_JSON (path to service account json)
  GSHEET_WEBHOOK_URL (optional fallback for appends)
  CROSSING_ENABLED=true/false (optional; default true)
  SHOCK_ENABLED=true/false (optional; default true)
"""

import os, json, math, time, argparse, requests, datetime as dt
from collections import defaultdict
from dotenv import load_dotenv

# --- load .env EARLY so env vars exist for the feature flags
load_dotenv(override=True)

# Feature toggles (env)
SHOCK_ENABLED = os.getenv("SHOCK_ENABLED", "true").strip().lower() in ("1","true","yes","on")
CROSSING_ENABLED = os.getenv("CROSSING_ENABLED", "true").strip().lower() in ("1","true","yes","on")

# ========== CONFIG ==========
API_HOST = "https://open-api-v4.coinglass.com"
COIN_ENDPOINT = "/api/futures/liquidation/aggregated-heatmap/model2"
PAIR_ENDPOINT = "/api/futures/liquidation/heatmap/model2"

WATCH_COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE", "LINK"]
TIMEFRAMES  = ["24h", "72h"]  # macro focus

# Windows around price to sum liq levels (wider = fewer but heavier signals)
DEFAULT_WINDOW_PCTS = [6.0, 7.0]

# Crossing thresholds (abs(imbalance) >= threshold)
CROSSING_THRESHOLDS = [0.40, 0.50, 0.60]

# Shock: change in imbalance within a lookback window (minutes)
# Minutes/cooldowns per timeframe (longer windows for regime shifts; moderate cooldowns for re-alerts)
SHOCK_RULES = {
    "24h": {"minutes": 180, "cooldown_min": 20},
    "72h": {"minutes": 480, "cooldown_min": 30},
}

# Multi-level shock deltas evaluated simultaneously (Δ≥level)
SHOCK_LEVELS = [0.40, 0.50, 0.60]

DEFAULT_INTERVAL_S  = 60          # seconds between loops
STATE_FILE          = "alerts_state.json"
PER_REQUEST_PAUSE   = 0.5         # pause between API calls (politeness)

# Require condition to persist before alerting (minutes)
PERSISTENCE_MIN = {
    "24h": 5,
    "72h": 10,
}

# How much history to retain (minutes)—keep above the largest shock window
MAX_HISTORY_MINUTES = max(v["minutes"] for v in SHOCK_RULES.values()) * 3

# Milestones schedule:
# 1m: 1..5 | 5m: 10..30 step 5 | 15m: 45..360 step 15 | 30m: 390..720 step 30 | 60m: 780..4320 step 60
def build_milestones_option_a():
    mins = []
    mins += [1, 2, 3, 4, 5]
    mins += list(range(10, 30 + 1, 5))
    mins += list(range(45, 6*60 + 1, 15))
    mins += list(range(6*60 + 30, 12*60 + 1, 30))
    mins += list(range(13*60, 72*60 + 1, 60))
    return mins

DELTA_MILESTONES_MIN = build_milestones_option_a()

# ======== Helpers ========
def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def get_api_key() -> str:
    load_dotenv()
    k = os.getenv("COINGLASS_API_KEY")
    if not k:
        raise RuntimeError("Missing COINGLASS_API_KEY in .env")
    return k

def timeframe_variants(tf: str):
    tf = tf.strip().lower()
    variants = {tf}
    if tf.endswith("h"):
        n = int(tf[:-1])
        variants |= {f"{n}hour", f"{n} hours"}
        if n % 24 == 0:
            d = n // 24
            variants |= {f"{d}d", f"{d}day", f"{d} days"}
    order = ["12h","24h","72h","1d","3d","12hour","24hour","72hour",
             "12 hours","24 hours","72 hours","1day","3day"]
    return [v for v in order if v in variants]

def try_fetch(url, headers, params):
    r = requests.get(url, headers=headers, params=params, timeout=25)
    try:
        j = r.json()
    except Exception:
        j = {}
    return r, j

def fetch_coin_model2_raw(currency: str, timeframe: str):
    headers = {"CG-API-KEY": get_api_key(), "accept": "application/json"}
    url = f"{API_HOST}{COIN_ENDPOINT}"
    param_options = []
    for v in timeframe_variants(timeframe):
        param_options += [
            {"currency": currency, "range": v},
            {"symbol":  currency,  "range": v},
            {"coin":    currency,  "range": v},
            {"currency": currency, "interval": v},
        ]
    last = None
    for params in param_options:
        r, j = try_fetch(url, headers, params)
        if r.status_code == 200 and str(j.get("code")) == "0" and "data" in j:
            return j["data"], params, "coin"
        last = f"coin fail: http={r.status_code}, code={j.get('code')}, msg={j.get('msg')}, params={params}"
    raise RuntimeError(last or "coin endpoint failed")

def fetch_pair_model2_raw(symbol_pair: str, timeframe: str):
    headers = {"CG-API-KEY": get_api_key(), "accept": "application/json"}
    url = f"{API_HOST}{PAIR_ENDPOINT}"
    param_options = []
    for v in timeframe_variants(timeframe):
        param_options += [
            {"symbol": symbol_pair, "range": v},
            {"symbol": symbol_pair, "interval": v},
        ]
    last = None
    for params in param_options:
        r, j = try_fetch(url, headers, params)
        if r.status_code == 200 and str(j.get("code")) == "0" and "data" in j:
            return j["data"], params, "pair"
        last = f"pair fail: http={r.status_code}, code={j.get('code')}, msg={j.get('msg')}, params={params}"
    raise RuntimeError(last or "pair endpoint failed")

def fetch_any(currency: str, timeframe: str):
    try:
        return fetch_coin_model2_raw(currency, timeframe)
    except Exception:
        pair = currency.upper().strip() + "USDT"
        return fetch_pair_model2_raw(pair, timeframe)

# -------- formatting helpers --------
def format_direction(prev_sign, cur_sign):
    def lab(s): return "Above" if s > 0 else ("Below" if s < 0 else "Neutral")
    return f"{lab(prev_sign)}→{lab(cur_sign)}"

def format_crossing_detail(imb, prev_sign, cur_sign, threshold):
    return f"{imb:.2%} ({format_direction(prev_sign, cur_sign)}) ≥ {threshold:.0%}"

def format_shock_detail(prev_imb, now_imb, dt_min):
    d = abs(now_imb - prev_imb)
    return f"{prev_imb:.2%} → {now_imb:.2%} (Δ{d:.2%} in {dt_min} min)"

def window_display(window_pct):
    return f"{float(window_pct):.0f}%"

# ========== Data shaping ==========
def last_close(price_candles):
    if not price_candles or len(price_candles[-1]) < 5:
        raise RuntimeError("price_candlesticks missing/short")
    return float(price_candles[-1][4])

def aggregate_totals_by_level(y_axis, triples):
    levels = []
    for v in (y_axis or []):
        try:
            levels.append(float(v))
        except Exception:
            levels.append(float("nan"))
    sums = defaultdict(float)
    for row in (triples or []):
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        _, yi, amt = row[:3]
        try:
            yi = int(yi); amt = float(amt)
        except Exception:
            continue
        if 0 <= yi < len(levels) and math.isfinite(amt):
            sums[yi] += amt
    return [(lvl, sums.get(i, 0.0)) for i, lvl in enumerate(levels) if math.isfinite(lvl)]

def split_window(levels, price, pct):
    tol = pct / 100.0
    inwin = [(lvl, amt) for (lvl, amt) in levels if price and abs(lvl - price)/price <= tol]
    below = sorted([(lvl, amt) for (lvl, amt) in inwin if lvl < price], key=lambda x: price - x[0])
    above = sorted([(lvl, amt) for (lvl, amt) in inwin if lvl > price], key=lambda x: x[0] - price)
    return below, above

def imbalance(below, above):
    tb = sum(amt for _, amt in below)
    ta = sum(amt for _, amt in above)
    denom = ta + tb
    return ((ta - tb)/denom if denom else 0.0), ta, tb

# ========== State I/O ==========
def load_state(reset=False):
    if reset:
        return {"_pending": {}, "_next_row": 2}
    try:
        with open(STATE_FILE, "r") as f:
            s = json.load(f)
            s.setdefault("_pending", {})
            s.setdefault("_next_row", 2)
            return s
    except Exception:
        return {"_pending": {}, "_next_row": 2}

def save_state(state: dict):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass

def new_alert_id(coin, tf, window_pct, ts, thr=None, shock_level=None):
    base = f"{coin}|{tf}|{int(float(window_pct))}|{int(ts)}"
    if thr is not None:
        return f"{base}|thr{int(round(thr*100))}"
    if shock_level is not None:
        return f"{base}|shock{int(round(shock_level*100))}"
    return base

def record_alert_pending(state, alert_id, sheet_row, base_price, ts):
    state.setdefault("_pending", {})[alert_id] = {
        "row": int(sheet_row),
        "base_price": float(base_price),
        "ts": float(ts),
        "done": []
    }

def mark_done(state, alert_id, minute_bucket):
    rec = state.get("_pending", {}).get(alert_id)
    if not rec:
        return
    if int(minute_bucket) not in rec["done"]:
        rec["done"].append(int(minute_bucket))
    if set(rec["done"]) >= set(DELTA_MILESTONES_MIN):
        del state["_pending"][alert_id]

# ========== Telegram ==========
def send_telegram(text: str) -> bool:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("TELEGRAM_CHANNEL")
    tag = os.getenv("ALERT_TAG", "").strip()

    if tag:
        text = f"{tag} {text}"  # prepend tag to every message

    if not (token and chat_id):
        print("[warn] Telegram not sent: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID", flush=True)
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=15)
        ok = (r.status_code == 200 and r.json().get("ok") is True)
        if not ok:
            print("[error] Telegram send failed:", r.text, flush=True)
        return ok
    except Exception as e:
        print("[error] Telegram exception:", e, flush=True)
        return False

# ========== Google Sheets ==========
HEADER_ROW = 1
SHEET_HEADERS = ["Date","Time","Coin","Price","Timeframe","Window","Alert"] + \
                [f"%Δ {m}m" for m in DELTA_MILESTONES_MIN]

def _get_gspread_client():
    load_dotenv()
    creds_path = os.getenv("GOOGLE_SA_JSON")
    if not creds_path:
        raise RuntimeError("Missing GOOGLE_SA_JSON")
    if not os.path.exists(creds_path):
        raise RuntimeError(f"GOOGLE_SA_JSON not found: {creds_path}")
    try:
        from google.oauth2.service_account import Credentials
        import gspread
    except Exception as e:
        raise RuntimeError("gspread/google-auth not installed. Run: pip install gspread google-auth") from e
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)

def _get_sheet_handles():
    load_dotenv()
    sheet_id = os.getenv("GOOGLE_SHEETS_ID")
    tab_name = os.getenv("GOOGLE_SHEETS_TAB", "Alerts")
    if not sheet_id:
        print("[warn] Sheets not configured: missing GOOGLE_SHEETS_ID", flush=True)
        return None, None, None
    try:
        gc = _get_gspread_client()
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet(tab_name)
        except Exception:
            ws = sh.add_worksheet(title=tab_name, rows=4000, cols=max(30, len(SHEET_HEADERS)+2))
        return gc, sh, ws
    except Exception as e:
        print("[error] Sheets open failed:", e, flush=True)
        return None, None, None

def ensure_headers_and_get_map(ws):
    values = ws.get_values(f"A{HEADER_ROW}:ZZ{HEADER_ROW}")
    headers = (values[0] if values else [])
    if not headers or headers[:len(SHEET_HEADERS)] != SHEET_HEADERS:
        ws.update(range_name=f"A{HEADER_ROW}", values=[SHEET_HEADERS])
        headers = SHEET_HEADERS
    return {h: i+1 for i, h in enumerate(headers)}

def a1_col(n):
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def rowcol_to_a1(row, col):
    return f"{a1_col(col)}{row}"

def get_last_data_row(ws):
    colA = ws.col_values(1)
    return len(colA) if colA else HEADER_ROW

def append_rows_to_sheet(rows) -> bool:
    gc, sh, ws = _get_sheet_handles()
    if not ws:
        return False
    try:
        ensure_headers_and_get_map(ws)
        if rows:
            ws.append_rows(rows, value_input_option="RAW")
        return True
    except Exception as e:
        print("[error] Sheets append failed:", e, flush=True)
        return False

def append_rows_via_webhook(rows) -> bool:
    load_dotenv()
    url = os.getenv("GSHEET_WEBHOOK_URL")
    if not url:
        return False
    try:
        r = requests.post(url, json={"rows": rows}, timeout=20)
        ok = (r.status_code == 200 and isinstance(r.json(), dict) and r.json().get("ok") is True)
        if not ok:
            print("[error] Webhook append failed:", r.text, flush=True)
        return ok
    except Exception as e:
        print("[error] Webhook exception:", e, flush=True)
        return False

def append_rows(rows) -> bool:
    return append_rows_to_sheet(rows) or append_rows_via_webhook(rows)

def batch_fill_cells(updates):
    gc, sh, ws = _get_sheet_handles()
    if not ws:
        return False
    if not updates:
        return True
    try:
        ws.batch_update(updates, value_input_option='RAW')
        return True
    except Exception as e:
        print("[error] Sheets batch_update failed:", e, flush=True)
        return False

# ---- one-time next-row initialization ----
def ensure_next_row_initialized(state):
    if state.get("_next_row", 2) > 2:
        return state["_next_row"]
    gc, sh, ws = _get_sheet_handles()
    if not ws:
        state["_next_row"] = 2
        return 2
    ensure_headers_and_get_map(ws)
    last = get_last_data_row(ws)
    state["_next_row"] = max(last + 1, 2)
    return state["_next_row"]

# ========== Persistence Gate ==========
def persisted(hist, now_ts, tf, cond_fn):
    """Return True if cond_fn(value) holds continuously for PERSISTENCE_MIN[tf] minutes."""
    need_sec = int(PERSISTENCE_MIN.get(tf, 0)) * 60
    if need_sec <= 0:
        return True
    cutoff = now_ts - need_sec
    saw_any = False
    for ts, val in reversed(hist):
        if ts < cutoff:
            break
        saw_any = True
        if not cond_fn(val):
            return False
    return saw_any  # ensure we actually covered the window

# ========== Shock detection (multi-level) ==========
def prune_history(hist, now_ts):
    keep_seconds = MAX_HISTORY_MINUTES * 60
    return [pt for pt in hist if now_ts - pt[0] <= keep_seconds][-500:]

def detect_shock(tf, hist, now_imb, now_ts, levels):
    """Return (level, prev_imb, dt_min) for the HIGHEST level satisfied within the tf window, else (None, None, None)."""
    rule = SHOCK_RULES.get(tf)
    if not rule:
        return (None, None, None)
    window = rule["minutes"] * 60
    best = (None, None, None)
    for ts, prev_imb in reversed(hist):
        dt_sec = now_ts - ts
        if dt_sec > window:
            break
        diff = abs(now_imb - prev_imb)
        lvl = max((L for L in levels if diff >= L), default=None)
        if lvl is not None and (best[0] is None or lvl > best[0]):
            best = (lvl, prev_imb, round(dt_sec / 60, 1))
    return best

def cooldown_ok(last_ts_str, cooldown_min, now_ts):
    if not last_ts_str:
        return True
    try:
        last_ts = dt.datetime.fromisoformat(last_ts_str).timestamp()
    except Exception:
        return True
    return (now_ts - last_ts) >= cooldown_min * 60

# ========== Run loop ==========
def run_once(windows, thresholds, startup_mode, state):
    lines = []
    staged_rows = []
    staged_meta = []

    utc_now_dt = now_utc()
    utc_now = utc_now_dt.strftime("%Y-%m-%d %H:%M:%S")
    now_ts = utc_now_dt.timestamp()

    current_prices = {}

    for coin in WATCH_COINS:
        for tf in TIMEFRAMES:
            # ----- One fetch per coin×tf -----
            try:
                data, params, source = fetch_any(coin, tf)
                time.sleep(PER_REQUEST_PAUSE)
                price = last_close(data.get("price_candlesticks", []))
                current_prices[coin] = price
                levels = aggregate_totals_by_level(
                    data.get("y_axis", []),
                    data.get("liquidation_leverage_data", [])
                )
            except Exception as e:
                line = f"{utc_now} | {coin:<5} {tf:<4} | FETCH ERROR: {e}"
                print(line, flush=True); lines.append(line)
                continue

            for window_pct in windows:
                try:
                    below, above = split_window(levels, price, float(window_pct))
                    imb, ta, tb = imbalance(below, above)
                except Exception as e:
                    line = f"{utc_now} | {coin:<5} {tf:<4} | win=±{window_pct}% | SHAPE ERROR: {e}"
                    print(line, flush=True); lines.append(line)
                    continue

                # ---------- MULTI-LEVEL SHOCK (one state per coin×tf×window) ----------
                if SHOCK_ENABLED:
                    shock_key = f"{coin}:{tf}:{window_pct}:shock"
                    shock_rec = state.get(shock_key, {})
                    shock_hist = shock_rec.get("hist", [])
                    shock_hist.append([now_ts, float(imb)])
                    shock_hist = prune_history(shock_hist, now_ts)

                    level, prev_imb, dt_min = detect_shock(tf, shock_hist[:-1], imb, now_ts, SHOCK_LEVELS)

                    # Optional: persistence gate for shocks (delta-based)
                    if level is not None:
                        def cond_fn(prev_v): return abs(imb - prev_v) >= level
                        if not persisted(shock_hist, now_ts, tf, cond_fn):
                            level = None

                    rule = SHOCK_RULES[tf]
                    last_level = float(shock_rec.get("last_level", 0.0))
                    last_any_utc = shock_rec.get("last_any_utc")

                    should_alert = False
                    if level is not None:
                        if level > last_level:
                            should_alert = True  # escalate immediately
                        elif cooldown_ok(last_any_utc, rule["cooldown_min"], now_ts):
                            should_alert = True  # same-level re-alert after cooldown

                    if level is not None and should_alert:
                        parts = [f"<b>Heatmap Alert</b> {coin} • {tf} • ±{float(window_pct):.0f}% window"]
                        parts.append("⚡ Shock Δ≥" + f"{int(level*100)}%: " + format_shock_detail(prev_imb, imb, dt_min))
                        parts += [
                            f"Above: ${ta:,.0f} • Below: ${tb:,.0f}",
                            f"Current price: ${price:,.2f}",
                            f"Source: {source}",
                            f"Params: {params}",
                            f"UTC: {utc_now}",
                        ]
                        send_telegram("\n".join(parts))

                        # Sheet row for shock (include level)
                        date_str = utc_now_dt.strftime("%Y-%m-%d")
                        time_str = utc_now_dt.strftime("%H:%M:%S")
                        row_values = [date_str, time_str, coin, round(price, 4), tf,
                                      window_display(window_pct),
                                      f"shock Δ≥{int(level*100)}%: " + format_shock_detail(prev_imb, imb, dt_min)] + \
                                     [""] * len(DELTA_MILESTONES_MIN)
                        staged_rows.append(row_values)

                        aid = new_alert_id(coin, tf, window_pct, now_ts, shock_level=level)
                        staged_meta.append({"aid": aid, "coin": coin, "base_price": price, "ts": now_ts})

                        shock_rec["last_level"] = float(level)
                        shock_rec["last_any_utc"] = now_utc().isoformat()

                    # Persist shock hist every loop
                    shock_rec.update({"hist": shock_hist, "last_checked_utc": now_utc().isoformat()})
                    state[shock_key] = shock_rec

                # ---------- MULTI-THRESHOLD CROSSINGS ----------
                if CROSSING_ENABLED:
                    for thr in thresholds:
                        key = f"{coin}:{tf}:{window_pct}:thr={int(round(thr*100))}"
                        rec = state.get(key, {})
                        hist = rec.get("hist", [])
                        hist.append([now_ts, float(imb)])
                        hist = prune_history(hist, now_ts)

                        prev_active = bool(rec.get("active"))
                        prev_sign = int(rec.get("sign", 0))
                        active_now = abs(imb) >= thr
                        sign_now = 1 if imb > 0 else (-1 if imb < 0 else 0)

                        # Persistence gate for crossings
                        def _sign(v): return 1 if v > 0 else (-1 if v < 0 else 0)
                        def cond_fn(v): return abs(v) >= thr and _sign(v) == sign_now
                        persist_ok = persisted(hist, now_ts, tf, cond_fn)

                        crossed = False; reason = ""
                        if "active" not in rec:
                            if startup_mode == "all":
                                crossed = True; reason = "initial(all)"
                            elif startup_mode == "over" and active_now and persist_ok:
                                crossed = True; reason = "initial(over+persist)"
                        else:
                            crossed = active_now and persist_ok and (not prev_active or prev_sign != sign_now)
                            if crossed: reason = "crossing+persist"

                        if crossed:
                            parts = [f"<b>Heatmap Alert</b> {coin} • {tf} • ±{float(window_pct):.0f}% window • thr=±{int(thr*100)}%"]
                            parts.append("Crossing: <b>" + format_crossing_detail(imb, prev_sign, sign_now, thr) + "</b>")
                            parts += [
                                f"Above: ${ta:,.0f} • Below: ${tb:,.0f}",
                                f"Current price: ${price:,.2f}",
                                f"Source: {source}",
                                f"Params: {params}",
                                f"UTC: {utc_now}",
                                f"Note: {reason}" if reason else "",
                            ]
                            send_telegram("\n".join(parts))

                            date_str = utc_now_dt.strftime("%Y-%m-%d")
                            time_str = utc_now_dt.strftime("%H:%M:%S")
                            alert_label = "crossing: " + format_crossing_detail(imb, prev_sign, sign_now, thr)
                            row_values = [date_str, time_str, coin, round(price, 4), tf,
                                          window_display(window_pct),
                                          f"thr=±{int(thr*100)}% • {alert_label}"] + \
                                         [""] * len(DELTA_MILESTONES_MIN)
                            staged_rows.append(row_values)

                            aid = new_alert_id(coin, tf, window_pct, now_ts, thr=thr)
                            staged_meta.append({"aid": aid, "coin": coin, "base_price": price, "ts": now_ts})

                        # Per-threshold status line
                        line = (
                            f"{utc_now} | {coin:<5} {tf:<4} | win=±{float(window_pct):.0f}% | thr=±{int(thr*100)}% | "
                            f"imb={imb:>+6.2%} | above=${ta:>12,.0f} | below=${tb:>12,.0f}"
                        )
                        print(line, flush=True); lines.append(line)

                        rec.update({
                            "active": active_now,
                            "sign": sign_now,
                            "last_imbalance": float(imb),
                            "last_checked_utc": now_utc().isoformat(),
                            "hist": hist,
                        })
                        state[key] = rec

    # ===== Append staged rows & register milestones =====
    if staged_rows:
        start_row = ensure_next_row_initialized(state)
        ok = append_rows(staged_rows)
        if not ok:
            err = f"{utc_now} | Sheets append failed for {len(staged_rows)} row(s)."
            print(err, flush=True); lines.append(err)
        else:
            for i, meta in enumerate(staged_meta):
                row_idx = start_row + i
                record_alert_pending(state, meta["aid"], row_idx, meta["base_price"], meta["ts"])
            state["_next_row"] = start_row + len(staged_rows)

    # ===== Milestone fills (single batch) =====
    pending = state.get("_pending", {})
    if pending:
        gc, sh, ws = _get_sheet_handles()
        updates = []
        mark_after_success = []
        if ws:
            col_map = ensure_headers_and_get_map(ws)
            now_ts2 = now_utc().timestamp()
            milestone_col = {}
            for m in DELTA_MILESTONES_MIN:
                header = f"%Δ {m}m"
                col = col_map.get(header)
                if col: milestone_col[m] = col

            for aid, rec in list(pending.items()):
                try:
                    parts = aid.split("|")
                    coin_i = parts[0]
                except Exception:
                    continue
                base = rec["base_price"]
                row = rec["row"]
                done_set = set(rec.get("done", []))
                price_now = current_prices.get(coin_i)
                if price_now is None:
                    continue
                elapsed_min = int((now_ts2 - float(rec["ts"])) // 60)

                for m in DELTA_MILESTONES_MIN:
                    if m in done_set or elapsed_min < m:
                        continue
                    col = milestone_col.get(m)
                    if not col:
                        continue
                    pct = (price_now / base) - 1.0
                    a1 = rowcol_to_a1(row, col)
                    updates.append({"range": a1, "values": [[round(pct, 6)]]})
                    mark_after_success.append((aid, m))

        if updates:
            ok2 = batch_fill_cells(updates)
            msg = (f"{len(updates)} milestone cell(s) updated." if ok2
                   else f"Sheets milestone batch failed ({len(updates)} cells).")
            print(f"{utc_now} | {msg}", flush=True); lines.append(f"{utc_now} | {msg}")
            if ok2:
                for aid, m in mark_after_success:
                    mark_done(state, aid, m)

    return lines, state

# ========== CLI ==========
def parse_windows(args):
    if args.windows:
        out = []
        for w in args.windows.split(","):
            w = w.strip()
            if not w: continue
            try: out.append(float(w))
            except ValueError: pass
        return out or DEFAULT_WINDOW_PCTS
    if args.window is not None:
        return [float(args.window)]
    return DEFAULT_WINDOW_PCTS

def parse_thresholds(args):
    if getattr(args, "thresholds", None):
        out = []
        for t in args.thresholds.split(","):
            t = t.strip()
            if not t: continue
            try: out.append(float(t))
            except ValueError: pass
        return out or CROSSING_THRESHOLDS
    if getattr(args, "threshold", None) is not None:
        return [float(args.threshold)]
    return CROSSING_THRESHOLDS

def parse_shock_levels(args):
    if getattr(args, "shock_levels", None):
        out = []
        for s in args.shock_levels.split(","):
            s = s.strip()
            if not s: continue
            try: out.append(float(s))
            except ValueError: pass
        return out or SHOCK_LEVELS
    return SHOCK_LEVELS

def main():
    ap = argparse.ArgumentParser(
        description="Coinglass Heatmap Alert Runner (Telegram + Shock + Sheets + Milestones; macro-focused: 24h/72h, ±6–7% windows, ±40/50/60% thresholds, Δ≥40/50/60% shocks)"
    )
    ap.add_argument("--windows", type=str, help='comma-separated window percents, e.g. "6,7"')
    ap.add_argument("--window", type=float, help="single window percent (convenience)")
    ap.add_argument("--thresholds", type=str, help='comma-separated crossing thresholds as fractions, e.g. "0.40,0.50,0.60"')
    ap.add_argument("--threshold", type=float, default=None, help="single crossing threshold (fraction), e.g. 0.50")
    ap.add_argument("--shock-levels", type=str, help='comma-separated shock deltas as fractions, e.g. "0.40,0.50,0.60"')
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_S, help="seconds between checks")
    ap.add_argument("--startup", choices=["over","all","none"], default="over",
                    help="first run: 'over' (alert if already ≥ thr), 'all' (alert everything once), 'none'")
    ap.add_argument("--reset-state", action="store_true", help="ignore prior alerts_state.json on startup")
    args = ap.parse_args()

    windows = parse_windows(args)
    thresholds = parse_thresholds(args)
    shock_levels = parse_shock_levels(args)

    # allow overriding globals for this run (so detect_shock sees the CLI levels)
    global SHOCK_LEVELS
    SHOCK_LEVELS = shock_levels

    print("=== Coinglass Heatmap Alert Runner (Telegram + Shock + Sheets + Milestones) ===", flush=True)
    print(f"Watchlist: {', '.join(WATCH_COINS)} | TFs: {', '.join(TIMEFRAMES)} | Windows: " +
          ", ".join(f"±{w:.0f}%" for w in windows), flush=True)
    print(f"crossing thr(s)={', '.join(f'±{int(t*100)}%' for t in thresholds)} | "
          f"shock Δ-levels={', '.join(f'≥{int(s*100)}%' for s in shock_levels)}", flush=True)
    print(f"interval={args.interval}s | startup={args.startup} | crossing_enabled={CROSSING_ENABLED} | shock_enabled={SHOCK_ENABLED}", flush=True)
    print("shock windows: " + ", ".join(f"{tf}: ≤{r['minutes']}m (cooldown {r['cooldown_min']}m)"
                                      for tf,r in SHOCK_RULES.items()), flush=True)
    if args.reset_state:
        print("State: RESET", flush=True)
    print("Ctrl-C to stop.\n", flush=True)

    state = load_state(reset=args.reset_state)
    ensure_next_row_initialized(state)
    save_state(state)

    try:
        while True:
            _, state = run_once(windows, thresholds, args.startup, state)
            save_state(state)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped by user.", flush=True)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Coinglass Model-2 SHOCK + CROSSING + SUSTAINED alerts (Telegram + Google Sheets).

What it does:
- Polls Coinglass Model-2 heatmap for a watchlist of coins and timeframes.
- Computes imbalance within ±window% of current price.

Alerts (independent):
1) SHOCK: |imbalance_now - imbalance_before| ≥ level within a TF-specific lookback window.
   - No persistence (fires immediately when condition true).
   - Per-TF cooldown to reduce spam.

2) CROSSING: |imbalance_now| ≥ threshold and stays ≥ threshold with the SAME SIGN
   continuously for TF-specific minutes (12h=1m, 24h=1m, 72h=1m here for testing).
   - No startup-safe arming; if already ≥ threshold and persists, it can fire.
   - Per-TF cooldown (applied per (coin, tf, window, threshold)).
   - Requires full-window coverage (not just one sample).

3) SUSTAINED: Like CROSSING but requires much longer persistence (default 30m), with its
   own thresholds and per-timeframe cooldowns.

ENV expected:
  COINGLASS_API_KEY
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID (or TELEGRAM_CHANNEL)
  ALERT_TAG (optional, prepended to every Telegram message)
  GOOGLE_SHEETS_ID
  GOOGLE_SA_JSON (path to service account json)
  GOOGLE_SHEETS_TAB (optional; default 'Alerts')
  GSHEET_WEBHOOK_URL (optional fallback for appends - not used in this script)
  # Optional sustained thresholds via ENV: SUSTAINED_THRESHOLDS="0.03,0.04,0.05"
"""

import os, time, math, json, argparse, requests, datetime as dt
from collections import defaultdict
from dotenv import load_dotenv

# --- load .env early
load_dotenv(override=True)

# ======== Feature toggles ========
SHOCK_ENABLED      = True   # set False to disable SHOCK alerts entirely
CROSSING_ENABLED   = True   # set False to disable CROSSING alerts entirely
SUSTAINED_ENABLED  = True   # set False to disable SUSTAINED alerts entirely

# ========== CONFIG ==========
API_HOST = "https://open-api-v4.coinglass.com"
COIN_ENDPOINT = "/api/futures/liquidation/aggregated-heatmap/model2"
PAIR_ENDPOINT = "/api/futures/liquidation/heatmap/model2"

WATCH_COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE", "LINK"]
TIMEFRAMES  = ["12h", "24h", "72h"]

# Windows around price to sum liq levels
DEFAULT_WINDOW_PCTS = [6.0, 7.0, 8.0]

### SHOCK RULES

SHOCK_LEVELS = [.40, .50, .60]
SHOCK_RULES = {
    "12h": {"minutes": 60,  "cooldown_min": 0},
    "24h": {"minutes": 180, "cooldown_min": 0},
    "72h": {"minutes": 480, "cooldown_min": 0},
}
PERSISTENCE_MIN_SHOCK = {"12h": 0, "24h": 0, "72h": 0}

### CROSSING RULES

CROSSING_THRESHOLDS = [.40, .50, .60]
CROSSING_RULES = {
    "12h": {"cooldown_min": 5},
    "24h": {"cooldown_min": 15},
    "72h": {"cooldown_min": 30},
}
PERSISTENCE_MIN_CROSSING = {"12h": 3, "24h": 5, "72h": 12}


### SUSTAINED RULES
SUSTAINED_THRESHOLDS = [.35, .425, .50]

SUSTAINED_RULES = {
    "12h": {"cooldown_min": 30},
    "24h": {"cooldown_min": 60},
    "72h": {"cooldown_min": 120},
}

PERSISTENCE_MIN_SUSTAINED = {"12h": 30, "24h": 60, "72h": 120}

_env_thr = os.getenv("SUSTAINED_THRESHOLDS")
if _env_thr:
    try:
        SUSTAINED_THRESHOLDS = [float(x.strip()) for x in _env_thr.split(",") if x.strip()]
    except ValueError:
        pass


DEFAULT_INTERVAL_S  = 60       # seconds between loops
STATE_FILE          = "alerts_state.json"
PER_REQUEST_PAUSE   = 0.5      # pause between API calls (politeness)

# ======== Price delta milestone columns ========
# 1m: 1..5 | 5m: 10..30 step 5 | 15m: 45..360 step 15 | 30m: 390..720 step 30 | 60m: 780..4320 step 60

def _build_milestones():
    mins = []
    mins += [1,2,3,4,5]
    mins += list(range(10, 30+1, 5))
    mins += list(range(45, 6*60 + 1, 15))
    mins += list(range(6*60 + 30, 12*60 + 1, 30))
    mins += list(range(13*60, 72*60 + 1, 60))
    return mins

DELTA_MILESTONES_MIN = _build_milestones()

# ======== Helpers ========
def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def get_api_key() -> str:
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

def cooldown_ok(last_ts_str, cooldown_min, now_ts):
    if not last_ts_str:
        return True
    try:
        last_ts = dt.datetime.fromisoformat(last_ts_str).timestamp()
    except Exception:
        return True
    return (now_ts - last_ts) >= cooldown_min * 60

# ======== State I/O ==========
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

# ======== Telegram ==========
def send_telegram(text: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("TELEGRAM_CHANNEL")
    tag = os.getenv("ALERT_TAG", "").strip()

    if tag:
        text = f"{tag} {text}"

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

# ======== Google Sheets ==========
HEADER_ROW = 1
SHEET_HEADERS = [
    "Date","Time","Coin","Price","Timeframe","Window","Alert"
] + [f"%Δ {m}m" for m in DELTA_MILESTONES_MIN]


def _get_gspread_client():
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


def append_rows_with_row_index(rows):
    gc, sh, ws = _get_sheet_handles()
    if not ws:
        return False, None
    try:
        ensure_headers_and_get_map(ws)
        start_row = max(get_last_data_row(ws) + 1, 2)
        if rows:
            ws.append_rows(rows, value_input_option="RAW")
        return True, start_row
    except Exception as e:
        print("[error] Sheets append failed:", e, flush=True)
        return False, None


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


def append_rows(rows) -> bool:
    ok, _ = append_rows_with_row_index(rows)
    return ok

# ======== Milestone tracking state ========
# state["_pending"][alert_id] = {"row": int, "base_price": float, "ts": float, "done": [mins,...]}

def new_alert_id(coin, tf, window_pct, ts, kind, thr=None, sign=None):
    base = f"{coin}|{tf}|{int(float(window_pct))}|{int(ts)}|{kind}"
    if thr is not None:
        base += f"|thr{int(round(thr*100))}"
    if sign is not None:
        base += f"|{'abv' if sign>0 else ('blw' if sign<0 else 'neu')}"
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

# ======== Small helpers ========

def sign_label(x):  # "Above"/"Below"/"Neutral"
    return "Above" if x > 0 else ("Below" if x < 0 else "Neutral")

# --- persistence helpers ---

def persisted_for_minutes(hist, now_ts, mins, thr, sign_now):
    """True if for at least the last `mins` minutes, abs(v) >= thr and sign(v) == sign_now with no breaks."""
    need_sec = int(mins) * 60
    if need_sec <= 0:
        return True
    start = now_ts - need_sec
    streak_start = None
    for ts, v in hist:
        s = 1 if v > 0 else (-1 if v < 0 else 0)
        ok = (s != 0) and (s == sign_now) and (abs(v) >= thr)
        if ok:
            if streak_start is None:
                streak_start = ts
        else:
            streak_start = None
    return streak_start is not None and (now_ts - streak_start) >= need_sec


def persisted_crossing(hist, now_ts, tf, thr, sign_now):
    mins = int(PERSISTENCE_MIN_CROSSING.get(tf, 0))
    return persisted_for_minutes(hist, now_ts, mins, thr, sign_now)

# ========== One loop ==========

def run_once(windows, shock_levels, crossing_thresholds, sustained_thresholds, state):
    utc_now_dt = now_utc()
    utc_now = utc_now_dt.strftime("%Y-%m-%d %H:%M:%S")
    now_ts = utc_now_dt.timestamp()

    current_prices = {}
    staged_rows = []
    staged_register_meta = []  # [{aid, coin, base_price, ts}]

    for coin in WATCH_COINS:
        for tf in TIMEFRAMES:
            # 1) Fetch
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
                print(f"{utc_now} | {coin:<5} {tf:<4} | FETCH ERROR: {e}", flush=True)
                continue

            for window_pct in windows:
                # 2) Imbalance now
                try:
                    below, above = split_window(levels, price, float(window_pct))
                    imb, ta, tb = imbalance(below, above)
                except Exception as e:
                    print(f"{utc_now} | {coin:<5} {tf:<4} | win=±{window_pct}% | SHAPE ERROR: {e}", flush=True)
                    continue

                # ✅ ALWAYS-ON LOGGING (independent of alerts/toggles)
                line = (f"{utc_now} | {coin:<5} {tf:<4} | win=±{int(float(window_pct))}% | "
                        f"imb={imb:+6.2%} | above=${ta:,.0f} | below=${tb:,.0f}")
                print(line, flush=True)

                # ===== SHOCK (independent) =====
                if SHOCK_ENABLED:
                    s_key = f"{coin}:{tf}:{window_pct}:shock"
                    s_rec = state.get(s_key, {})
                    s_hist = s_rec.get("hist", [])
                    # keep only needed history
                    keep_m = SHOCK_RULES[tf]["minutes"]
                    s_hist = [pt for pt in s_hist if now_ts - pt[0] <= keep_m*60][-1000:]

                    level = None; prev_imb = None; dt_min = None
                    # detect within window
                    window = SHOCK_RULES[tf]["minutes"]*60
                    for ts0, prev in reversed(s_hist):
                        dt_sec = now_ts - ts0
                        if dt_sec > window:
                            break
                        diff = abs(imb - prev)
                        lvl = max((L for L in shock_levels if diff >= L), default=None)
                        if lvl is not None and (level is None or lvl > level):
                            level, prev_imb, dt_min = lvl, prev, int(round(dt_sec/60))

                    if level is not None:
                        cd = SHOCK_RULES[tf]["cooldown_min"]
                        last_any_utc = s_rec.get("last_any_utc")
                        if cooldown_ok(last_any_utc, cd, now_ts):
                            delta_pct = abs(imb - prev_imb)
                            msg_lines = [
                                "⚡ <b>SHOCK ALERT</b>",
                                f"Coin: {coin}",
                                f"Threshold: Δ≥{int(level*100)}%: {prev_imb:+.2%} → {imb:+.2%} (Δ{delta_pct:.2%} in {dt_min} min)",
                                f"Window: ±{int(float(window_pct))}%",
                                f"Above: ${ta:,.0f} • Below: ${tb:,.0f}",
                                f"Current price: ${price:,.2f}",
                                f"Timeframe: {tf}",
                                f"UTC: {utc_now}",
                            ]
                            send_telegram("\n".join(msg_lines))

                            # Sheets
                            date_str = utc_now_dt.strftime("%Y-%m-%d")
                            time_str = utc_now_dt.strftime("%H:%M:%S")
                            label = f"shock Δ≥{int(level*100)}%: {prev_imb:+.2%} → {imb:+.2%} (Δ{delta_pct:.2%} in {dt_min}m)"
                            row_values = [date_str, time_str, coin, round(price, 4), tf,
                                          f"±{int(float(window_pct))}%", label] + [""]*len(DELTA_MILESTONES_MIN)
                            staged_rows.append(row_values)
                            aid = new_alert_id(coin, tf, window_pct, now_ts, kind="shock")
                            staged_register_meta.append({"aid": aid, "coin": coin, "base_price": price, "ts": now_ts})

                            s_rec["last_any_utc"] = now_utc().isoformat()

                    # persist shock history (append current point last)
                    s_hist.append([now_ts, float(imb)])
                    s_rec["hist"] = s_hist
                    s_rec["last_checked_utc"] = now_utc().isoformat()
                    state[s_key] = s_rec

                # ===== CROSSING (independent) =====
                if CROSSING_ENABLED:
                    for thr in crossing_thresholds:
                        thr_bps = int(round(thr * 10000))             # precise per-threshold key
                        win_bp  = int(round(float(window_pct) * 100))  # precise per-window key
                        c_key   = f"{coin}:{tf}:win{win_bp}bp:cross:{thr_bps}bps"

                        c_rec = state.get(c_key, {})
                        c_hist = c_rec.get("hist", [])
                        look_min = max(15, PERSISTENCE_MIN_CROSSING.get(tf, 0) * 3)
                        cutoff_sec = look_min * 60
                        c_hist = [pt for pt in c_hist if now_ts - pt[0] <= cutoff_sec][-1000:]

                        c_hist.append([now_ts, float(imb)])

                        prev_persist = bool(c_rec.get("persist_ok", False))
                        prev_sign = int(c_rec.get("sign", 0))

                        sign_now = 1 if imb > 0 else (-1 if imb < 0 else 0)
                        now_persist = False
                        if sign_now != 0:
                            now_persist = persisted_crossing(c_hist, now_ts, tf, thr, sign_now)

                        crossed = False
                        if now_persist and not prev_persist:
                            crossed = True
                        elif now_persist and prev_persist and (prev_sign != sign_now):
                            crossed = True

                        if crossed:
                            cd = CROSSING_RULES.get(tf, {}).get("cooldown_min", 0)
                            last_any_utc = c_rec.get("last_any_utc")
                            if cooldown_ok(last_any_utc, cd, now_ts):
                                mins_req = PERSISTENCE_MIN_CROSSING.get(tf, 0)
                                msg_lines = [
                                    "🚦 <b>CROSSING ALERT</b>",
                                    f"Coin: {coin}",
                                    f"Threshold: ±{int(thr*100)}% • Imbalance: {imb:+.2%} ({sign_label(imb)}) — Persisted {mins_req}m",
                                    f"Window: ±{int(float(window_pct))}%",
                                    f"Above: ${ta:,.0f} • Below: ${tb:,.0f}",
                                    f"Current price: ${price:,.2f}",
                                    f"Timeframe: {tf}",
                                    f"UTC: {utc_now}",
                                ]
                                send_telegram("\n".join(msg_lines))

                                date_str = utc_now_dt.strftime("%Y-%m-%d")
                                time_str = utc_now_dt.strftime("%H:%M:%S")
                                label = f"crossing thr=±{int(thr*100)}% • imb={imb:+.2%} ({sign_label(imb)}) • persisted {mins_req}m"
                                row_values = [date_str, time_str, coin, round(price, 4), tf,
                                              f"±{int(float(window_pct))}%", label] + [""]*len(DELTA_MILESTONES_MIN)
                                staged_rows.append(row_values)
                                aid = new_alert_id(coin, tf, window_pct, now_ts, kind="cross", thr=thr, sign=sign_now)
                                staged_register_meta.append({"aid": aid, "coin": coin, "base_price": price, "ts": now_ts})

                                c_rec["last_any_utc"] = now_utc().isoformat()

                        c_rec.update({
                            "sign": sign_now,
                            "persist_ok": bool(now_persist),
                            "last_checked_utc": now_utc().isoformat(),
                            "hist": c_hist,
                        })
                        state[c_key] = c_rec

                # ===== SUSTAINED (independent; custom thresholds & cooldowns; 30m persistence) =====
                if SUSTAINED_ENABLED:
                    for thr in sustained_thresholds:
                        thr_bps = int(round(thr * 10000))
                        win_bp  = int(round(float(window_pct) * 100))
                        key = f"{coin}:{tf}:win{win_bp}bp:sustain:{thr_bps}bps"

                        rec = state.get(key, {})
                        hist = rec.get("hist", [])
                        # retain enough history to cover the sustained window plus slack
                        look_min = max(PERSISTENCE_MIN_SUSTAINED.get(tf, 30) * 3, 120)
                        cutoff_sec = look_min * 60
                        hist = [pt for pt in hist if now_ts - pt[0] <= cutoff_sec][-2000:]
                        hist.append([now_ts, float(imb)])

                        sign_now = 1 if imb > 0 else (-1 if imb < 0 else 0)
                        persist_ok = False
                        if sign_now != 0:
                            persist_ok = persisted_for_minutes(hist, now_ts, PERSISTENCE_MIN_SUSTAINED.get(tf, 30), thr, sign_now)

                        prev_ok = bool(rec.get("persist_ok", False))
                        prev_sign = int(rec.get("sign", 0))
                        should_alert = persist_ok and (not prev_ok or prev_sign != sign_now)
                        if should_alert:
                            cd = SUSTAINED_RULES.get(tf, {}).get("cooldown_min", 0)
                            last_any_utc = rec.get("last_any_utc")
                            if cooldown_ok(last_any_utc, cd, now_ts):
                                mins_req = PERSISTENCE_MIN_SUSTAINED.get(tf, 30)
                                msg_lines = [
                                    "🕰️ <b>SUSTAINED IMBALANCE</b>",
                                    f"Coin: {coin}",
                                    f"Threshold: ±{int(thr*100)}% • Imbalance: {imb:+.2%} ({sign_label(imb)}) — Persisted {mins_req}m",
                                    f"Window: ±{int(float(window_pct))}%",
                                    f"Above: ${ta:,.0f} • Below: ${tb:,.0f}",
                                    f"Current price: ${price:,.2f}",
                                    f"Timeframe: {tf}",
                                    f"UTC: {utc_now}",
                                ]
                                send_telegram("\n".join(msg_lines))

                                date_str = utc_now_dt.strftime("%Y-%m-%d")
                                time_str = utc_now_dt.strftime("%H:%M:%S")
                                label = f"sustained thr=±{int(thr*100)}% • imb={imb:+.2%} ({sign_label(imb)}) • persisted {mins_req}m"
                                row_values = [date_str, time_str, coin, round(price, 4), tf,
                                              f"±{int(float(window_pct))}%", label] + [""]*len(DELTA_MILESTONES_MIN)
                                staged_rows.append(row_values)
                                aid = new_alert_id(coin, tf, window_pct, now_ts, kind="sustain", thr=thr, sign=sign_now)
                                staged_register_meta.append({"aid": aid, "coin": coin, "base_price": price, "ts": now_ts})

                                rec["last_any_utc"] = now_utc().isoformat()

                        rec.update({
                            "sign": sign_now,
                            "persist_ok": bool(persist_ok),
                            "last_checked_utc": now_utc().isoformat(),
                            "hist": hist,
                        })
                        state[key] = rec

    # ===== Append staged rows & register pending for milestone fills =====
    if staged_rows:
        ok, start_row = append_rows_with_row_index(staged_rows)
        if not ok:
            print(f"{utc_now} | Sheets append failed for {len(staged_rows)} row(s).", flush=True)
        else:
            for i, meta in enumerate(staged_register_meta):
                row_idx = start_row + i
                record_alert_pending(state, meta["aid"], row_idx, meta["base_price"], meta["ts"])

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
            print(f"{utc_now} | {msg}", flush=True)
            if ok2:
                for aid, m in mark_after_success:
                    mark_done(state, aid, m)

    return state

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


def parse_crossing_thresholds(args):
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


def parse_sustained_thresholds(args):
    if getattr(args, "sustained_thresholds", None):
        out = []
        for t in args.sustained_thresholds.split(","):
            t = t.strip()
            if not t: continue
            try: out.append(float(t))
            except ValueError: pass
        return out or SUSTAINED_THRESHOLDS
    if getattr(args, "sustained_threshold", None) is not None:
        return [float(args.sustained_threshold)]
    return SUSTAINED_THRESHOLDS


def main():
    ap = argparse.ArgumentParser(description="Coinglass Model-2 SHOCK + CROSSING + SUSTAINED alert runner (Telegram + Sheets + Milestones)")
    ap.add_argument("--windows", type=str, help='comma-separated window percents, e.g. "6,7,8"')
    ap.add_argument("--window", type=float, help="single window percent")
    ap.add_argument("--shock-levels", type=str, help='comma-separated shock deltas as fractions, e.g. "0.40,0.50,0.60"')
    ap.add_argument("--thresholds", type=str, help='comma-separated crossing thresholds as fractions, e.g. "0.40,0.50,0.60"')
    ap.add_argument("--threshold", type=float, help="single crossing threshold (fraction), e.g. 0.50")
    ap.add_argument("--sustained-thresholds", type=str, help='comma-separated sustained thresholds, e.g. "0.03,0.04,0.05"')
    ap.add_argument("--sustained-threshold", type=float, help="single sustained threshold, e.g. 0.04")
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_S, help="seconds between checks")
    ap.add_argument("--reset-state", action="store_true", help="ignore prior alerts_state.json on startup")
    args = ap.parse_args()

    windows = parse_windows(args)
    shock_levels = parse_shock_levels(args)
    crossing_thresholds = parse_crossing_thresholds(args)
    sustained_thresholds = parse_sustained_thresholds(args)

    print("=== Coinglass Model-2 SHOCK + CROSSING + SUSTAINED Alert Runner (Telegram + Sheets + Milestones) ===", flush=True)
    print(f"Watchlist: {', '.join(WATCH_COINS)} | TFs: {', '.join(TIMEFRAMES)} | Windows: " +
          ", ".join(f"±{w:.0f}%" for w in windows), flush=True)
    print(f"SHOCK Δ-levels={', '.join(f'≥{int(s*100)}%' for s in shock_levels)} | SHOCK_ENABLED={SHOCK_ENABLED}", flush=True)
    print(f"CROSS thr(s)={', '.join(f'±{int(t*100)}%' for t in crossing_thresholds)} | CROSSING_ENABLED={CROSSING_ENABLED}", flush=True)
    print(f"SUSTAINED thr(s)={', '.join(f'±{int(t*100)}%' for t in sustained_thresholds)} | persistence={PERSISTENCE_MIN_SUSTAINED.get('12h',30)}m | SUSTAINED_ENABLED={SUSTAINED_ENABLED}", flush=True)
    print("Shock windows: " + ", ".join(f"{tf}: ≤{r['minutes']}m (cooldown {r['cooldown_min']}m)"
                                      for tf,r in SHOCK_RULES.items()), flush=True)
    print("Crossing persistence: " + ", ".join(f"{tf}: {PERSISTENCE_MIN_CROSSING.get(tf,0)}m" for tf in TIMEFRAMES), flush=True)
    print("Sustained persistence: " + ", ".join(f"{tf}: {PERSISTENCE_MIN_SUSTAINED.get(tf,30)}m" for tf in TIMEFRAMES), flush=True)
    print("Sustained cooldowns: " + ", ".join(f"{tf}: {SUSTAINED_RULES.get(tf, {}).get('cooldown_min', 0)}m" for tf in TIMEFRAMES), flush=True)
    print("Ctrl-C to stop.", flush=True)

    state = load_state(reset=args.reset_state)
    save_state(state)

    try:
        while True:
            state = run_once(windows, shock_levels, crossing_thresholds, sustained_thresholds, state)
            save_state(state)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopped by user.", flush=True)

if __name__ == "__main__":
    main()

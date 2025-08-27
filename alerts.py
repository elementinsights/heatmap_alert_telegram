#!/usr/bin/env python3
"""
Coinglass Model-2 heatmap alert runner (Telegram + Google Sheets with milestones).
- Crossing alerts at multiple thresholds (default ±30%, ±35%, ±40%)
- Shock alerts (|Δimbalance| >= shock_delta within shock_minutes) — ONE per coin×tf×window (not per threshold)
- Stores short imbalance history per coin×timeframe×window×threshold in alerts_state.json
- Appends one row per alert to a Google Sheet (service account or Apps Script webhook)
- Fills %Δ price columns at custom milestones:
    1m,2m,3m,4m,5m,10m,15m, then every 15m (30m,45m,...) up to 24h
- Streams terminal output continuously and caches the next free sheet row for speed.
"""

import os, json, math, time, argparse, requests, datetime as dt
from collections import defaultdict
from dotenv import load_dotenv

# ========== CONFIG ==========
API_HOST = "https://open-api-v4.coinglass.com"
COIN_ENDPOINT = "/api/futures/liquidation/aggregated-heatmap/model2"
PAIR_ENDPOINT = "/api/futures/liquidation/heatmap/model2"

WATCH_COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
TIMEFRAMES  = ["12h", "24h", "72h"]

DEFAULT_WINDOW_PCTS = [5.0]      # alert windows ±5%, ±6%, ±7%
CROSSING_THRESHOLDS = [0.30]   # evaluate all of these
DEFAULT_INTERVAL_S  = 120                  # seconds between loops
STATE_FILE          = "alerts_state.json"
PER_REQUEST_PAUSE   = 0.5                  # pause between API calls

# Shock rules: per-timeframe Δimb must happen within this many minutes
SHOCK_RULES = {
    "12h": {"delta": 0.30, "minutes": 15,  "cooldown_min": 5},
    "24h": {"delta": 0.30, "minutes": 120, "cooldown_min": 10},
    "72h": {"delta": 0.30, "minutes": 360, "cooldown_min": 15},
}
# How much history to retain (minutes). Keep comfortably above the largest window.
MAX_HISTORY_MINUTES = max(v["minutes"] for v in SHOCK_RULES.values()) * 3

# Milestones: 1,2,3,4,5,10,15, then every 15m from 30m to 24h
EARLY_MINUTES = [1, 2, 3, 4, 5, 10, 15]
DELTA_MILESTONES_MIN = EARLY_MINUTES + [i for i in range(30, 24*60 + 1, 15)]

# ======== ENV EXPECTED ========
# COINGLASS_API_KEY=...
# TELEGRAM_BOT_TOKEN=...
# TELEGRAM_CHAT_ID=...  (or TELEGRAM_CHANNEL)
# GOOGLE_SHEETS_ID=...  (spreadsheet id)
# GOOGLE_SHEETS_TAB=Alerts  (optional; default Alerts)
# GOOGLE_SA_JSON=service_account.json  (path to GCP service account key)
# GSHEET_WEBHOOK_URL=https://script.google.com/... (optional fallback for appends)

# ========== Helpers ==========
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

def new_alert_id(coin, tf, window_pct, ts, thr=None):
    # if thr is None -> shock id; else include threshold
    base = f"{coin}|{tf}|{int(float(window_pct))}|{int(ts)}"
    return base if thr is None else f"{base}|{int(round(thr*100))}"

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
        ws.update(f"A{HEADER_ROW}", [SHEET_HEADERS])
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

# ========== Shock detection ==========
def prune_history(hist, now_ts):
    keep_seconds = MAX_HISTORY_MINUTES * 60
    return [pt for pt in hist if now_ts - pt[0] <= keep_seconds][-500:]

def detect_shock(tf, hist, now_imb, now_ts):
    rule = SHOCK_RULES.get(tf)
    if not rule:
        return (False, None, None)
    window = rule["minutes"] * 60
    delta_needed = rule["delta"]
    for ts, prev_imb in reversed(hist):
        dt_sec = now_ts - ts
        if dt_sec > window:
            break
        if abs(now_imb - prev_imb) >= delta_needed:
            return True, prev_imb, round(dt_sec / 60, 1)
    return False, None, None

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
            for window_pct in windows:
                try:
                    data, params, source = fetch_any(coin, tf)
                    time.sleep(PER_REQUEST_PAUSE)
                    price = last_close(data.get("price_candlesticks", []))
                    current_prices[coin] = price
                    levels = aggregate_totals_by_level(
                        data.get("y_axis", []),
                        data.get("liquidation_leverage_data", [])
                    )
                    below, above = split_window(levels, price, float(window_pct))
                    imb, ta, tb = imbalance(below, above)
                except Exception as e:
                    line = f"{utc_now} | {coin:<5} {tf:<4} | win=±{window_pct}% | ERROR: {e}"
                    print(line, flush=True); lines.append(line)
                    continue

                # ---------- SINGLE shock evaluation per (coin, tf, window) ----------
                shock_key = f"{coin}:{tf}:{window_pct}:shock"
                shock_rec = state.get(shock_key, {})
                shock_hist = shock_rec.get("hist", [])
                shock_hist.append([now_ts, float(imb)])
                shock_hist = prune_history(shock_hist, now_ts)

                is_shock, prev_imb, dt_min = detect_shock(tf, shock_hist[:-1], imb, now_ts)
                shock_ok = False
                if is_shock:
                    rule = SHOCK_RULES[tf]
                    shock_ok = cooldown_ok(shock_rec.get("last_shock_alert_utc"), rule["cooldown_min"], now_ts)

                if is_shock and shock_ok:
                    parts = [f"<b>Heatmap Alert</b> {coin} • {tf} • ±{float(window_pct):.0f}% window"]
                    parts.append("⚡ Shock: " + format_shock_detail(prev_imb, imb, dt_min))
                    parts += [
                        f"Above: ${ta:,.0f} • Below: ${tb:,.0f}",
                        f"Current price: ${price:,.2f}",
                        f"Source: {source}",
                        f"Params: {params}",
                        f"UTC: {utc_now}",
                    ]
                    text = "\n".join(parts)
                    sent = send_telegram(text)

                    # Sheet row for shock (no thr)
                    date_str = utc_now_dt.strftime("%Y-%m-%d")
                    time_str = utc_now_dt.strftime("%H:%M:%S")
                    row_values = [date_str, time_str, coin, round(price, 4), tf,
                                  window_display(window_pct),
                                  "shock: " + format_shock_detail(prev_imb, imb, dt_min)] + \
                                 [""] * len(DELTA_MILESTONES_MIN)
                    staged_rows.append(row_values)

                    aid = new_alert_id(coin, tf, window_pct, now_ts, thr=None)  # shock id
                    staged_meta.append({"aid": aid, "coin": coin, "base_price": price, "ts": now_ts})

                    shock_rec["last_shock_alert_utc"] = now_utc().isoformat()

                # Persist shock hist every loop
                shock_rec.update({"hist": shock_hist, "last_checked_utc": now_utc().isoformat()})
                state[shock_key] = shock_rec

                # ---------- Crossing evaluation per threshold ----------
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

                    crossed = False; reason = ""
                    if "active" not in rec:
                        if startup_mode == "all":
                            crossed = True; reason = "initial(all)"
                        elif startup_mode == "over" and active_now:
                            crossed = True; reason = "initial(over)"
                    else:
                        crossed = active_now and (not prev_active or prev_sign != sign_now)
                        if crossed: reason = "crossing"

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
                        text = "\n".join(parts)
                        sent = send_telegram(text)

                        date_str = utc_now_dt.strftime("%Y-%m-%d")
                        time_str = utc_now_dt.strftime("%H:%M:%S")
                        alert_label = "crossing: " + format_crossing_detail(imb, prev_sign, sign_now, thr)
                        row_values = [date_str, time_str, coin, round(price, 4), tf,
                                      window_display(window_pct),
                                      f"thr=±{int(thr*100)}% • {alert_label}"] + \
                                     [""] * len(DELTA_MILESTONES_MIN)
                        staged_rows.append(row_values)

                        aid = new_alert_id(coin, tf, window_pct, now_ts, thr)
                        staged_meta.append({"aid": aid, "coin": coin, "base_price": price, "ts": now_ts})

                    # Print per-threshold status
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
                    parts = aid.split("|")  # shock ids have 4 parts; crossing have 5
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

def main():
    ap = argparse.ArgumentParser(description="Coinglass Heatmap Alert Runner (Telegram + Shock + Sheets + Milestones, multi-threshold)")
    ap.add_argument("--windows", type=str, help='comma-separated window percents, e.g. "5,6,7"')
    ap.add_argument("--window", type=float, help="single window percent (convenience)")
    ap.add_argument("--thresholds", type=str, help='comma-separated crossing thresholds as fractions, e.g. "0.30,0.35,0.40"')
    ap.add_argument("--threshold", type=float, default=None, help="single crossing threshold (fraction), e.g. 0.30")
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_S, help="seconds between checks")
    ap.add_argument("--startup", choices=["over","all","none"], default="over",
                    help="first run: 'over' (alert if already ≥ thr), 'all' (alert everything once), 'none'")
    ap.add_argument("--reset-state", action="store_true", help="ignore prior alerts_state.json on startup")
    args = ap.parse_args()

    windows = parse_windows(args)
    thresholds = parse_thresholds(args)

    print("=== Coinglass Heatmap Alert Runner (Telegram + Shock + Sheets + Milestones) ===", flush=True)
    print(f"Watchlist: {', '.join(WATCH_COINS)} | TFs: {', '.join(TIMEFRAMES)} | Windows: " +
          ", ".join(f"±{w:.0f}%" for w in windows), flush=True)
    print(f"crossing thr(s)={', '.join(f'±{int(t*100)}%' for t in thresholds)} | "
          f"interval={args.interval}s | startup={args.startup}", flush=True)
    print("shock rules: " + ", ".join(f"{tf}: Δ≥{r['delta']:.0%} in ≤{r['minutes']}m (cooldown {r['cooldown_min']}m)"
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

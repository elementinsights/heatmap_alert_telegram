#!/usr/bin/env python3
"""
Coinglass Model-2 heatmap alert runner (Telegram).
- Crossing alerts (|imbalance| >= threshold)
- Shock alerts (|Δimbalance| >= shock_delta within shock_minutes)
- Stores short imbalance history per coin×timeframe×window in alerts_state.json
"""

import os, json, math, time, requests, argparse, datetime as dt
from collections import defaultdict
from dotenv import load_dotenv

# ------------ CONFIG ------------
API_HOST = "https://open-api-v4.coinglass.com"
COIN_ENDPOINT = "/api/futures/liquidation/aggregated-heatmap/model2"
PAIR_ENDPOINT = "/api/futures/liquidation/heatmap/model2"

WATCH_COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE"]
TIMEFRAMES  = ["12h", "24h", "72h"]

DEFAULT_WINDOW_PCTS = [5.0, 6.0, 7.0]   # default: alert at ±5%, ±6%, ±7%
DEFAULT_THRESHOLD   = 0.30              # crossing threshold (±30%)
DEFAULT_INTERVAL_S  = 60                # 1 minute
STATE_FILE          = "alerts_state.json"
PER_REQUEST_PAUSE   = 0.5

# Shock rules: per-timeframe Δimb must happen within this many minutes
SHOCK_RULES = {
    "12h": {"delta": 0.30, "minutes": 15,  "cooldown_min": 0},
    "24h": {"delta": 0.30, "minutes": 120, "cooldown_min": 0},
    "72h": {"delta": 0.30, "minutes": 360, "cooldown_min": 0},
}
# How much history to retain (minutes). Keep comfortably above the largest window.
MAX_HISTORY_MINUTES = max(v["minutes"] for v in SHOCK_RULES.values()) * 3

# ------------ Helpers ------------
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
    try: j = r.json()
    except Exception: j = {}
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

# ------------ Data shaping ------------
def last_close(price_candles):
    if not price_candles or len(price_candles[-1]) < 5:
        raise RuntimeError("price_candlesticks missing/short")
    return float(price_candles[-1][4])

def aggregate_totals_by_level(y_axis, triples):
    levels = []
    for v in (y_axis or []):
        try: levels.append(float(v))
        except Exception: levels.append(float("nan"))
    sums = defaultdict(float)
    for row in (triples or []):
        if not isinstance(row, (list, tuple)) or len(row) < 3: continue
        _, yi, amt = row[:3]
        try: yi = int(yi); amt = float(amt)
        except Exception: continue
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

# ------------ State I/O ------------
def load_state(reset=False):
    if reset: return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: dict):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass

# ------------ Telegram ------------
def send_telegram(text: str) -> bool:
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("TELEGRAM_CHANNEL")
    if not (token and chat_id):
        print("[warn] Telegram not sent: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=15)
        ok = (r.status_code == 200 and r.json().get("ok") is True)
        if not ok: print("[error] Telegram send failed:", r.text)
        return ok
    except Exception as e:
        print("[error] Telegram exception:", e); return False

# ------------ Shock detection ------------
def prune_history(hist, now_ts):
    """Keep only recent history needed for shock checks."""
    keep_seconds = MAX_HISTORY_MINUTES * 60
    return [pt for pt in hist if now_ts - pt[0] <= keep_seconds][-500:]  # also cap length

def detect_shock(tf, hist, now_imb, now_ts):
    """Return (is_shock, prev_imb, dt_minutes) based on SHOCK_RULES[tf]."""
    rule = SHOCK_RULES.get(tf)
    if not rule: return (False, None, None)
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
    if not last_ts_str: return True
    try:
        last_ts = dt.datetime.fromisoformat(last_ts_str).timestamp()
    except Exception:
        return True
    return (now_ts - last_ts) >= cooldown_min * 60

# ------------ Run loop ------------
def run_once(windows, threshold, startup_mode, state):
    lines = []
    utc_now_dt = now_utc()
    utc_now = utc_now_dt.strftime("%Y-%m-%d %H:%M:%S")
    now_ts = utc_now_dt.timestamp()

    for coin in WATCH_COINS:
        for tf in TIMEFRAMES:
            for window_pct in windows:
                try:
                    data, params, source = fetch_any(coin, tf)
                    time.sleep(PER_REQUEST_PAUSE)
                    price = last_close(data.get("price_candlesticks", []))
                    levels = aggregate_totals_by_level(
                        data.get("y_axis", []),
                        data.get("liquidation_leverage_data", [])
                    )
                    below, above = split_window(levels, price, float(window_pct))
                    imb, ta, tb = imbalance(below, above)
                except Exception as e:
                    lines.append(f"{utc_now} | {coin:<5} {tf:<4} | win=±{window_pct}% | ERROR: {e}")
                    continue

                key = f"{coin}:{tf}:{window_pct}"
                rec = state.get(key, {})
                hist = rec.get("hist", [])
                # Append current point and prune
                hist.append([now_ts, float(imb)])
                hist = prune_history(hist, now_ts)

                # Crossing logic
                prev_active = bool(rec.get("active"))
                prev_sign = int(rec.get("sign", 0))
                active_now = abs(imb) >= threshold
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

                # Shock logic
                is_shock, prev_imb, dt_min = detect_shock(tf, hist[:-1], imb, now_ts)
                shock_ok = False
                if is_shock:
                    rule = SHOCK_RULES[tf]
                    shock_ok = cooldown_ok(rec.get("last_shock_alert_utc"), rule["cooldown_min"], now_ts)

                status = "OK "
                # Build messages
                if crossed or (is_shock and shock_ok):
                    direction = "Above→Below" if imb > 0 else "Below→Above"
                    parts = [f"<b>Heatmap Alert</b> {coin} • {tf} • ±{float(window_pct):.0f}% window"]
                    if crossed:
                        parts.append(f"Crossing: <b>{imb:.2%}</b> ({direction}) ≥ {threshold:.0%}")
                    if is_shock and shock_ok:
                        d = abs(imb - prev_imb)
                        parts.append(f"⚡ Shock: {prev_imb:.2%} → {imb:.2%} (Δ{d:.2%} in {dt_min} min)")
                    parts += [
                        f"Above: ${ta:,.0f} • Below: ${tb:,.0f}",
                        f"Current price: ${price:,.2f}",
                        f"Source: {source}",
                        f"Params: {params}",
                        f"UTC: {utc_now}",
                        f"Note: {reason}" if reason else "",
                    ]
                    text = "\n".join(p for p in parts if p)
                    sent = send_telegram(text)
                    status += "📣" if sent else "📣✗"

                lines.append(
                    f"{utc_now} | {coin:<5} {tf:<4} | win=±{float(window_pct):.0f}% | "
                    f"imb={imb:>+6.2%} | above=${ta:>12,.0f} | below=${tb:>12,.0f} | {status}"
                )

                # Update state
                rec.update({
                    "active": active_now,
                    "sign": sign_now,
                    "last_imbalance": float(imb),
                    "last_checked_utc": now_utc().isoformat(),
                    "hist": hist,
                })
                if is_shock and shock_ok:
                    rec["last_shock_alert_utc"] = now_utc().isoformat()
                state[key] = rec

    return lines, state

def parse_windows(args):
    # Priority: --windows (comma list) > --window (single) > DEFAULT_WINDOW_PCTS
    if args.windows:
        out = []
        for w in args.windows.split(","):
            w = w.strip()
            if not w: continue
            try:
                out.append(float(w))
            except ValueError:
                pass
        return out or DEFAULT_WINDOW_PCTS
    if args.window is not None:
        return [float(args.window)]
    return DEFAULT_WINDOW_PCTS

def main():
    ap = argparse.ArgumentParser(description="Coinglass Heatmap Alert Runner (Telegram + Shock, multi-window)")
    ap.add_argument("--windows", type=str,
                    help='comma-separated window percents to evaluate, e.g. "5,6,7" (default uses 5,6,7)')
    ap.add_argument("--window", type=float,
                    help="single window percent (deprecated convenience; overrides defaults if provided)")
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                    help="crossing threshold (0.30=30%%)")
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_S,
                    help="seconds between checks (default 60)")
    ap.add_argument("--startup", choices=["over","all","none"], default="over",
                    help="first run: 'over' (alert if already ≥ thr), 'all' (alert everything once), 'none'")
    ap.add_argument("--reset-state", action="store_true",
                    help="ignore prior alerts_state.json on startup")
    args = ap.parse_args()

    windows = parse_windows(args)

    print("=== Coinglass Heatmap Alert Runner (Telegram + Shock) ===")
    print(f"Watchlist: {', '.join(WATCH_COINS)} | TFs: {', '.join(TIMEFRAMES)} | Windows: " +
          ", ".join(f"±{w:.0f}%" for w in windows))
    print(f"crossing thr=±{int(args.threshold*100)}% | interval={args.interval}s | startup={args.startup}")
    print("shock rules: " + ", ".join(f"{tf}: Δ≥{r['delta']:.0%} in ≤{r['minutes']}m" for tf,r in SHOCK_RULES.items()))
    if args.reset_state: print("State: RESET")
    print("Ctrl-C to stop.\n")

    state = load_state(reset=args.reset_state)
    try:
        while True:
            lines, state = run_once(windows, args.threshold, args.startup, state)
            for ln in lines: print(ln)
            save_state(state)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped by user.")

if __name__ == "__main__":
    main()

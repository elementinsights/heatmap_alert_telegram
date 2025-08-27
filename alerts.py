#!/usr/bin/env python3
"""
Headless Coinglass Model-2 heatmap alert runner with startup alerts & reset.
- Watches: BTC, ETH, SOL, XRP, DOGE, HYPE
- Timeframes: 12h, 24h, 72h
- Triggers on crossings of ±threshold within ±window
- On first run: configurable via --startup {over,all,none}
"""

import os, json, math, time, ssl, smtplib, requests, argparse, datetime as dt
from email.utils import formatdate
from collections import defaultdict
from dotenv import load_dotenv

# ------------ DEFAULT CONFIG ------------
API_HOST = "https://open-api-v4.coinglass.com"
COIN_ENDPOINT = "/api/futures/liquidation/aggregated-heatmap/model2"
PAIR_ENDPOINT = "/api/futures/liquidation/heatmap/model2"

WATCH_COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE"]
TIMEFRAMES  = ["12h", "24h", "72h"]

DEFAULT_WINDOW_PCT  = 5.0       # ± window for Above/Below totals
DEFAULT_THRESHOLD   = 0.20      # ±30% threshold
DEFAULT_INTERVAL_S  = 300       # run every 5 minutes
STATE_FILE          = "alerts_state.json"
PER_REQUEST_PAUSE   = 0.5       # throttle between API requests (s)

# ------------ API HELPERS ------------
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

# ------------ DATA SHAPING ------------
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

# ------------ STATE & EMAIL ------------
def load_state(reset=False):
    if reset:
        return {}
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

def send_email(subject: str, body: str) -> bool:
    load_dotenv()
    host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USERNAME")
    pwd  = os.getenv("SMTP_PASSWORD")
    sender = os.getenv("EMAIL_FROM", user or "")
    recipients = [a.strip() for a in os.getenv("EMAIL_TO", "").split(",") if a.strip()]
    if not (user and pwd and sender and recipients):
        print("[warn] Email not sent: missing SMTP_USERNAME/SMTP_PASSWORD/EMAIL_FROM/EMAIL_TO in .env")
        return False
    msg = f"From: {sender}\nTo: {', '.join(recipients)}\nDate: {formatdate(localtime=True)}\nSubject: {subject}\n\n{body}"
    try:
        with smtplib.SMTP(host, port, timeout=20) as s:
            s.ehlo(); s.starttls(context=ssl.create_default_context())
            s.login(user, pwd); s.sendmail(sender, recipients, msg.encode("utf-8"))
        return True
    except Exception as e:
        print("[error] Email send failed:", e); return False

# ------------ RUN LOOP ------------
def run_once(window_pct, threshold, startup_mode, state, first_cycle=False):
    """
    startup_mode: 'over' (alert on first run if |imb|>=thr), 'all' (alert all on first run), 'none'
    first_cycle: True only on the first loop iteration after start/reset
    """
    lines = []
    utc_now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for coin in WATCH_COINS:
        for tf in TIMEFRAMES:
            try:
                data, params, source = fetch_any(coin, tf)
                time.sleep(PER_REQUEST_PAUSE)
                price = last_close(data.get("price_candlesticks", []))
                levels = aggregate_totals_by_level(data.get("y_axis", []),
                                                   data.get("liquidation_leverage_data", []))
                below, above = split_window(levels, price, window_pct)
                imb, ta, tb = imbalance(below, above)
            except Exception as e:
                lines.append(f"{utc_now} | {coin:<5} {tf:<4} | ERROR: {e}")
                continue

            key = f"{coin}:{tf}"
            prev = state.get(key)
            active_now = abs(imb) >= threshold
            sign_now = 1 if imb > 0 else (-1 if imb < 0 else 0)

            crossed = False
            reason = ""
            if prev is None:
                # first time we've seen this pair this process (or after reset)
                if startup_mode == "all":
                    crossed = True; reason = "initial(all)"
                elif startup_mode == "over" and active_now:
                    crossed = True; reason = "initial(over)"
                else:
                    crossed = False
            else:
                # normal crossing logic
                prev_active = bool(prev.get("active"))
                prev_sign = int(prev.get("sign", 0))
                crossed = active_now and (not prev_active or prev_sign != sign_now)
                if crossed: reason = "crossing"

            status = "OK "
            if crossed:
                status = "ALRT"
                direction = "Above>Below" if imb > 0 else "Below>Above"
                subject = f"[Heatmap Alert] {coin} {tf} imbalance {imb:.2%} ({direction})"
                body = (
                    f"Coin: {coin}\nTimeframe: {tf}\n±Window: {window_pct}%\n"
                    f"Imbalance: {imb:.2%} ({direction})\n"
                    f"Above total: ${ta:,.0f}\nBelow total: ${tb:,.0f}\n"
                    f"Last price: ${price:,.2f}\nSource: {source}\n"
                    f"Params: {params}\nUTC: {utc_now}\nNote: {reason}\n"
                )
                emailed = send_email(subject, body)
                status += "✉" if emailed else "✉✗"

            lines.append(f"{utc_now} | {coin:<5} {tf:<4} | imb={imb:>+6.2%} | "
                         f"above=${ta:>12,.0f} | below=${tb:>12,.0f} | {status} {reason}")

            # update state
            state[key] = {
                "active": active_now,
                "sign": sign_now,
                "last_imbalance": imb,
                "last_checked_utc": utc_now
            }

    return lines, state

def main():
    ap = argparse.ArgumentParser(description="Coinglass Heatmap Alert Runner")
    ap.add_argument("--window", type=float, default=DEFAULT_WINDOW_PCT, help="±window percent (default 5.0)")
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="imbalance threshold (0.30=30%%)")
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_S, help="seconds between checks (default 300)")
    ap.add_argument("--startup", choices=["over","all","none"], default="over",
                    help="first-run behavior: 'over'=alert if already over threshold (default); "
                         "'all'=alert everything on first run; 'none'=no first-run alerts")
    ap.add_argument("--reset-state", action="store_true", help="ignore prior state on startup")
    args = ap.parse_args()

    print("=== Coinglass Heatmap Alert Runner ===")
    print(f"Watchlist: {', '.join(WATCH_COINS)} | TFs: {', '.join(TIMEFRAMES)}")
    print(f"window=±{args.window}% | threshold=±{int(args.threshold*100)}% | interval={args.interval}s | startup={args.startup}")
    if args.reset_state:
        print("State: RESET (starting fresh)")
    print("Press Ctrl-C to stop.\n")

    state = load_state(reset=args.reset_state)

    try:
        first_cycle = True
        while True:
            lines, state = run_once(args.window, args.threshold, args.startup, state, first_cycle=first_cycle)
            first_cycle = False
            for ln in lines:
                print(ln)
            save_state(state)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped by user.")

if __name__ == "__main__":
    main()

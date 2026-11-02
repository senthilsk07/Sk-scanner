"""
Intraday Breakout Scanner (T1 & T2) for NSE stocks, powered by the Upstox API.

Env vars:
  UPSTOX_ACCESS_TOKEN   required - your Upstox access token
  VOLUME_HIKE_MULT      optional - breakout volume must be >= this x morning avg (default 1.5)

Note: Upstox v2 historical endpoints only offer 1-minute and 30-minute candles.
Native 5-minute candles are served by the Upstox v3 historical-candle endpoints
(same access token), so this app uses those.
"""
import codecs
import json
import os
import re
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
IST = ZoneInfo("Asia/Kolkata")
BASE = "https://api.upstox.com"
MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
HIKE_MULT = float(os.environ.get("VOLUME_HIKE_MULT", "1.5"))

# ---------------------------------------------------------------- sectors
SECTORS = {
    "Banking": ["HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK", "INDUSINDBK", "BANKBARODA", "PNB"],
    "IT": ["TCS", "INFY", "HCLTECH", "WIPRO", "TECHM", "LTIM", "PERSISTENT", "COFORGE"],
    "Auto": ["MARUTI", "M&M", "BAJAJ-AUTO", "EICHERMOT", "HEROMOTOCO", "TVSMOTOR", "ASHOKLEY", "BOSCHLTD"],
    "Pharma": ["SUNPHARMA", "DRREDDY", "CIPLA", "DIVISLAB", "LUPIN", "AUROPHARMA", "TORNTPHARM", "ZYDUSLIFE"],
    "FMCG": ["HINDUNILVR", "ITC", "NESTLEIND", "BRITANNIA", "DABUR", "GODREJCP", "TATACONSUM", "MARICO"],
    "Metals": ["TATASTEEL", "JSWSTEEL", "HINDALCO", "VEDL", "JINDALSTEL", "NMDC", "SAIL", "COALINDIA"],
    "Oil & Gas": ["RELIANCE", "ONGC", "BPCL", "IOC", "GAIL", "PETRONET", "HINDPETRO", "OIL"],
    "Power": ["NTPC", "POWERGRID", "TATAPOWER", "ADANIPOWER", "NHPC", "TORNTPOWER", "JSWENERGY", "SJVN"],
    "Realty": ["DLF", "GODREJPROP", "OBEROIRLTY", "PRESTIGE", "LODHA", "PHOENIXLTD", "BRIGADE", "SOBHA"],
    "Infrastructure": ["LT", "SIEMENS", "ABB", "BHEL", "CUMMINSIND", "HAL", "BEL", "IRCON"],
    "Financial Services": ["BAJFINANCE", "BAJAJFINSV", "HDFCLIFE", "SBILIFE", "CHOLAFIN", "SHRIRAMFIN", "MUTHOOTFIN", "PFC"],
    "Cement": ["ULTRACEMCO", "GRASIM", "AMBUJACEM", "SHREECEM", "DALBHARAT", "ACC", "RAMCOCEM", "JKCEMENT"],
    "Telecom & Media": ["BHARTIARTL", "IDEA", "INDUSTOWER", "ZEEL", "SUNTV", "PVRINOX", "TATACOMM", "HFCL"],
    "Consumer Durables": ["TITAN", "HAVELLS", "VOLTAS", "DIXON", "CROMPTON", "BLUESTARCO", "WHIRLPOOL", "KALYANKJIL"],
}

# ---------------------------------------------------------------- instruments
_inst = {"map": None, "err": None, "mb": 0, "t0": None, "stage": "idle"}
_inst_lock = threading.Lock()


WANTED = {sym for syms in SECTORS.values() for sym in syms}


OBJ = re.compile(r"\{[^{}]*\}")


def instruments():
    """symbol -> {key, name}. Streams + regex-scans the Upstox master (low memory, fast)."""
    with _inst_lock:
        if _inst["map"] is None:
            out, buf = {}, ""
            _inst.update(t0=time.time(), mb=0, stage="downloading", err=None)
            print("[instruments] load started", flush=True)
            got = 0
            dec = zlib.decompressobj(16 + zlib.MAX_WBITS)
            txt = codecs.getincrementaldecoder("utf-8")("ignore")
            try:
                with requests.get(MASTER_URL, stream=True, timeout=(15, 60)) as r:
                    r.raise_for_status()
                    while True:
                        chunk = r.raw.read(65536)
                        if not chunk:
                            break
                        got += len(chunk)
                        _inst["mb"] = round(got / 1e6, 1)
                        buf += txt.decode(dec.decompress(chunk))
                        last = 0
                        for m in OBJ.finditer(buf):
                            last = m.end()
                            t = m.group()
                            if "NSE_EQ" not in t:
                                continue
                            d = json.loads(t)
                            if (d.get("segment") == "NSE_EQ" and d.get("instrument_type") == "EQ"
                                    and d.get("trading_symbol") in WANTED):
                                out[d["trading_symbol"]] = {"key": d["instrument_key"],
                                                            "name": d.get("name") or d["trading_symbol"]}
                        buf = buf[last:]
                if not out:
                    raise RuntimeError("Instrument file loaded but no matching stocks found")
                _inst["map"], _inst["err"], _inst["stage"] = out, None, "done"
                print(f"[instruments] loaded {len(out)} stocks in {time.time() - _inst['t0']:.0f}s", flush=True)
            except Exception as e:
                _inst["err"] = f"{type(e).__name__}: {e}"[:200]
                _inst["stage"] = "failed"
                print("[instruments] FAILED", _inst["err"], flush=True)
                raise
    return _inst["map"]


# ---------------------------------------------------------------- data fetch
_session = requests.Session()
_session.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=32))
_cache = {}
_cache_lock = threading.Lock()


def mm(h, m):
    return h * 60 + m


def ready_since():
    """Timestamp of the latest 5-min candle boundary (+6s for Upstox to publish it)."""
    n = datetime.now(IST)
    b = n.replace(minute=n.minute - n.minute % 5, second=6, microsecond=0)
    if b > n:
        b -= timedelta(minutes=5)
    return b.timestamp()


def fetch_candles(key, day):
    """Ascending list of 5-minute candles {t (minutes since midnight), o, h, l, c, v} for `day`."""
    today = datetime.now(IST).date()
    ck = (key, day)
    with _cache_lock:
        hit = _cache.get(ck)
    # past days never change; today only changes once per 5-minute candle
    if hit and (day != today or hit[0] >= ready_since()):
        return hit[1]

    token = os.environ.get("UPSTOX_ACCESS_TOKEN", "")
    k = quote(key, safe="")
    if day == today:
        url = f"{BASE}/v3/historical-candle/intraday/{k}/minutes/5"
    else:
        url = f"{BASE}/v3/historical-candle/{k}/minutes/5/{day}/{day}"
    r = _session.get(url, headers={"Accept": "application/json", "Authorization": f"Bearer {token}"}, timeout=20)
    if r.status_code == 401:
        raise RuntimeError("Upstox token invalid or expired (401)")
    if r.status_code == 429:
        raise RuntimeError("Upstox rate limit hit (429)")
    r.raise_for_status()

    out = []
    for c in r.json().get("data", {}).get("candles", []):
        ts = datetime.fromisoformat(c[0]).astimezone(IST)
        if ts.date() != day:
            continue
        out.append({"t": ts.hour * 60 + ts.minute, "o": c[1], "h": c[2], "l": c[3], "c": c[4], "v": c[5]})
    out.sort(key=lambda x: x["t"])
    with _cache_lock:
        _cache[ck] = (time.time(), out)
    return out


# ---------------------------------------------------------------- rule engine
def hhmm(minutes):
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def scan_breakout(c, H, L, avg_vol, start, end=mm(14, 40)):
    """
    Walk candles whose start time is in [start, end]. A breakout needs a CLOSE beyond H/L.
    Volume must beat BOTH previous candles and be >= HIKE_MULT x avg_vol.
    Returns (confirmed_hit, last_unconfirmed_hit).
    """
    last_fail = None
    for i, x in enumerate(c):
        if x["t"] < start or x["t"] > end or i < 2:
            continue
        if x["c"] > H:
            side, level = "Long", H
        elif x["c"] < L:
            side, level = "Short", L
        else:
            continue
        ok2 = x["v"] > c[i - 1]["v"] and x["v"] > c[i - 2]["v"]
        ratio = x["v"] / avg_vol if avg_vol else 0
        okh = ratio >= HIKE_MULT
        hit = {"side": side, "level": level, "time": hhmm(x["t"] + 5), "vol2": ok2, "hike": okh, "ratio": round(ratio, 2)}
        if ok2 and okh:
            return hit, last_fail
        last_fail = hit
    return None, last_fail


def evaluate(c):
    blank = {"signal": "No Signal", "strategy": None, "side": None, "level": None, "time": None,
             "vol2": None, "hike": None, "ratio": None, "range": None}
    base = [x for x in c if mm(9, 15) <= x["t"] < mm(9, 55)]
    if not base or base[-1]["t"] < mm(9, 50):  # opening range not complete yet
        return blank
    H, L = max(x["h"] for x in base), min(x["l"] for x in base)
    avg = sum(x["v"] for x in base) / len(base)
    res = dict(blank, range=[H, L])

    window = [x for x in c if mm(9, 55) <= x["t"] < mm(10, 25)]  # 09:55-10:25
    early = any(x["c"] > H or x["c"] < L for x in window)

    if not early:
        # ---- T1: stay inside range 09:55-10:25 (no touch), then close-breakout 10:30-14:45
        if any(x["h"] >= H or x["l"] <= L for x in window):
            return res  # wick touched the range: T1 invalid
        hit, fail = scan_breakout(c, H, L, avg, mm(10, 30))
        if hit:
            return dict(res, signal="T1 Triggered", strategy="T1", **hit)
        return dict(res, signal="Watching", strategy="T1", **(fail or {}))

    # ---- T2: early close-breakout before 10:25 -> extend range to 09:15-10:55
    ext = [x for x in c if mm(9, 15) <= x["t"] < mm(10, 55)]
    EH, EL = max(x["h"] for x in ext), min(x["l"] for x in ext)
    eavg = sum(x["v"] for x in ext) / len(ext)
    res = dict(res, range=[EH, EL], strategy="T2")
    if ext[-1]["t"] < mm(10, 50):  # still extending
        return dict(res, signal="Watching")
    hit, fail = scan_breakout(c, EH, EL, eavg, mm(10, 55))
    if hit:
        return dict(res, signal="T2 Triggered", **hit)
    return dict(res, signal="Watching", **(fail or {}))


SWAP = {"T1": "T2", "T2": "T1"}


def relabel(res):
    """Show the strategy names the way the trader expects (T1 <-> T2 swapped)."""
    if res.get("strategy"):
        res["strategy"] = SWAP[res["strategy"]]
    if res["signal"].endswith("Triggered"):
        res["signal"] = f'{res["strategy"]} Triggered'
    return res


# ---------------------------------------------------------------- routes
@app.route("/")
def index():
    return render_template("index.html", today=datetime.now(IST).date().isoformat())


@app.route("/api/sectors")
def sectors():
    return jsonify(list(SECTORS))


@app.route("/api/scan")
def scan():
    sector = request.args.get("sector", "All")
    try:
        day = date.fromisoformat(request.args.get("date") or datetime.now(IST).date().isoformat())
    except ValueError:
        return jsonify(error="Invalid date. Use YYYY-MM-DD."), 400
    if not os.environ.get("UPSTOX_ACCESS_TOKEN"):
        return jsonify(error="UPSTOX_ACCESS_TOKEN is not set on the server."), 500

    now = datetime.now(IST)
    is_today = day == now.date()
    if day > now.date() or day.weekday() >= 5:
        return jsonify(error="Pick a past or current weekday (NSE is closed on weekends)."), 400

    names = SECTORS if sector == "All" else {sector: SECTORS.get(sector, [])}
    if _inst["map"] is None:
        threading.Thread(target=_warm, daemon=True).start()
        msg = _inst["err"] or "Starting up: loading Upstox instrument list. Tap Scan now again in 30 seconds."
        return jsonify(error=msg), 503
    try:
        imap = instruments()
    except Exception as e:
        return jsonify(error=f"Could not load Upstox instrument list: {e}"), 502

    jobs = [(sec, sym) for sec, syms in names.items() for sym in syms]
    now_mm = now.hour * 60 + now.minute

    def work(job):
        sec, sym = job
        row = {"symbol": sym, "sector": sec, "name": sym}
        inst = imap.get(sym)
        if not inst:
            return dict(row, signal="No Data", note="Symbol not found in Upstox master")
        row["name"] = inst["name"]
        try:
            c = fetch_candles(inst["key"], day)
            if is_today:  # only fully closed candles
                c = [x for x in c if x["t"] + 5 <= now_mm]
            if not c:
                return dict(row, signal="No Data", note="No candles")
            return {**row, **relabel(evaluate(c))}
        except Exception as e:
            return dict(row, signal="No Data", note=str(e)[:120])

    with ThreadPoolExecutor(max_workers=16) as ex:
        rows = list(ex.map(work, jobs))

    market_open = is_today and now.weekday() < 5 and mm(9, 15) <= now_mm <= mm(15, 30)
    return jsonify(rows=rows, meta={"date": day.isoformat(), "market_open": market_open,
                                    "updated": now.strftime("%H:%M:%S"),
                                    "next_in": 306 - (now.minute % 5) * 60 - now.second, "hike_mult": HIKE_MULT})


@app.route("/status")
def status():
    m = _inst["map"]
    return jsonify(instruments_loaded=m is not None, count=len(m or {}), error=_inst["err"], stage=_inst["stage"], downloaded_mb=_inst["mb"],
                   seconds=round(time.time() - _inst["t0"]) if _inst["t0"] else None,
                   token_set=bool(os.environ.get("UPSTOX_ACCESS_TOKEN")))


@app.route("/health")
def health():
    return "ok"


def _warm():
    try:
        instruments()
    except Exception:
        pass


threading.Thread(target=_warm, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
                      

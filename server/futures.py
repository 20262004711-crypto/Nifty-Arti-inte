"""Futures key + lot size (shared, har user ke liye ek hi). Instruments file STREAM hoti hai
taaki 512MB RAM wale free server par memory na phate."""
import gzip, json
from datetime import datetime
import requests
import ijson
import start as st
from upstox_adapter import INDEX_CONFIG


def _stream_futures(names):
    if not (st.INSTRUMENTS_FILE.exists() and
            datetime.fromtimestamp(st.INSTRUMENTS_FILE.stat().st_mtime).date() == datetime.now().date()):
        with requests.get(st.INSTRUMENTS_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(st.INSTRUMENTS_FILE, "wb") as f:
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
    keep = []
    with gzip.open(st.INSTRUMENTS_FILE, "rb") as f:
        for d in ijson.items(f, "item"):
            if "FUT" not in str(d.get("instrument_type", "")).upper():
                continue
            nm = str(d.get("name", "")).upper()
            ts = str(d.get("trading_symbol", "")).upper()
            if nm in names or any(ts.startswith(n + " FUT") for n in names):
                keep.append(json.loads(json.dumps(d, default=float)))   # Decimal -> float
    return keep


def get_futures_info(names=("NIFTY", "SENSEX")) -> dict:
    cache = st._load_cache()
    out = {n: cache[n] for n in names if st._cache_ok(cache.get(n))}
    missing = [n for n in names if n not in out]
    if not missing:
        return out
    data = _stream_futures({INDEX_CONFIG[n]["fut_name"].upper() for n in missing})
    today = datetime.now(st.IST).date()
    for n in missing:
        f, why = st.select_futures(data, INDEX_CONFIG[n], today)
        if not f or not f.get("lot_size"):
            raise RuntimeError(f"{n} futures/lot size nahi mila: {why}")
        out[n] = {"key": f["instrument_key"], "lot_size": int(f["lot_size"]),
                  "expiry": st._expiry_date(f["expiry"]).isoformat()}
        cache[n] = out[n]
    st.FUT_CACHE_FILE.write_text(json.dumps(cache, indent=2))
    return out

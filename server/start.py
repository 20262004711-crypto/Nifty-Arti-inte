"""
EK HI SCRIPT - SAB KUCH AUTOMATIC
==================================
Pehli baar:
    python start.py --setup
    (API Key, API Secret, Redirect URI ek baar poochega, save kar lega)

Roz (market din):
    python start.py
    - Login URL print karega
    - Browser mein kholo, login karo, redirect URL se 'code=' wala part copy karo
    - Yahan paste karo jab poochega
    - Baaki SAB khud kar lega: token lena, futures key nikaalna (cached), engine chalana
"""
import json
import os
import sys
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

CONFIG_FILE = Path("upstox_config.json")
FUT_CACHE_FILE = Path("futures_key_cache.json")


# =====================================================================
# Setup (ek baar)
# =====================================================================

def setup():
    print("=== EK BAAR KA SETUP ===\n")
    print("Upstox Developer Console (account.upstox.com/developer/apps) se yeh 3 cheezein lo:\n")
    client_id = input("API Key (client_id): ").strip()
    client_secret = input("API Secret (client_secret): ").strip()
    redirect_uri = input("Redirect URI (jo app banate waqt daala tha, e.g. https://127.0.0.1): ").strip()

    cfg = {"client_id": client_id, "client_secret": client_secret, "redirect_uri": redirect_uri}
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2))
    print(f"\n✓ Saved to {CONFIG_FILE}. Ab roz sirf 'python start.py' chalana.")


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        sys.exit("Config nahi mila. Pehle chalao: python start.py --setup")
    return json.loads(CONFIG_FILE.read_text())


# =====================================================================
# Daily token exchange
# =====================================================================

def get_access_token(cfg: dict) -> str:
    login_url = (
        "https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code&client_id={cfg['client_id']}&redirect_uri={cfg['redirect_uri']}"
    )
    print("\nBrowser mein login page khul raha hai...")
    print(f"(Agar khud na khule to yeh URL manually paste karo:\n{login_url}\n)")
    webbrowser.open(login_url)

    print("Login/allow karne ke baad browser jis URL par redirect hoga,")
    print("usmein 'code=' ke baad wala hissa yahan paste karo.")
    raw = input("\nRedirect URL ya sirf code paste karo: ").strip()

    # User poora URL paste kare ya sirf code, dono handle karo
    if "code=" in raw:
        code = raw.split("code=")[1].split("&")[0]
    else:
        code = raw

    resp = requests.post(
        "https://api.upstox.com/v2/login/authorization/token",
        data={
            "code": code,
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "redirect_uri": cfg["redirect_uri"],
            "grant_type": "authorization_code",
        },
        headers={"accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    data = resp.json()
    if "access_token" not in data:
        sys.exit(f"❌ Token nahi mila: {data}")

    print("✓ Access token mil gaya.")
    return data["access_token"]


# =====================================================================
# Futures key + lot size (cached - expiry tak valid)
# =====================================================================

IST = timezone(timedelta(hours=5, minutes=30))
INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
INSTRUMENTS_FILE = Path("instruments_complete.json.gz")   # din bhar reuse hoti hai


def _expiry_date(v):
    """Upstox expiry: epoch ms (ya seconds, ya date string) - teeno handle."""
    if v is None or v == "":
        return None
    try:
        if isinstance(v, str) and not v.strip().isdigit():
            return datetime.fromisoformat(v.strip()[:10]).date()
        n = float(v)
        if n > 1e11:          # milliseconds
            n /= 1000
        return datetime.fromtimestamp(n, tz=IST).date()
    except Exception:
        return None


def _load_cache() -> dict:
    if FUT_CACHE_FILE.exists():
        try:
            return json.loads(FUT_CACHE_FILE.read_text())
        except Exception:
            return {}
    return {}


def _cache_ok(entry) -> bool:
    """Entry tabhi valid jab key + lot_size dono hon aur contract expire na hua ho."""
    if not isinstance(entry, dict) or not entry.get("key") or not entry.get("lot_size"):
        return False
    try:
        return datetime.fromisoformat(entry["expiry"]).date() >= datetime.now(IST).date()
    except Exception:
        return False


def _load_instruments() -> list:
    import gzip
    fresh = (INSTRUMENTS_FILE.exists() and
             datetime.fromtimestamp(INSTRUMENTS_FILE.stat().st_mtime).date() == datetime.now().date())
    if fresh:
        print("Instruments file aaj ki hai - disk se pad rahe hain...")
        raw = INSTRUMENTS_FILE.read_bytes()
    else:
        print("Instruments file download ho rahi hai - thoda time lega...")
        raw = requests.get(INSTRUMENTS_URL, timeout=120).content
        INSTRUMENTS_FILE.write_bytes(raw)
    return json.loads(gzip.decompress(raw))


def select_futures(data: list, spec: dict, today) -> tuple:
    """(contract, info_msg). Exact name pehle, phir trading_symbol se. Expired contract nahi."""
    fx, nm = spec["fut_exchange"], spec["fut_name"].upper()

    def is_fut(d):
        return "FUT" in str(d.get("instrument_type", "")).upper()

    def on_exch(d):
        return fx in (d.get("exchange"), d.get("segment"))

    cands = [d for d in data if is_fut(d) and on_exch(d) and str(d.get("name", "")).upper() == nm]
    if not cands:    # fallback: trading_symbol "NIFTY FUT 27 OCT 26" jaisa
        cands = [d for d in data if is_fut(d) and on_exch(d)
                 and str(d.get("trading_symbol", "")).upper().startswith(nm + " FUT")]
    if not cands:
        return None, "futures records hi nahi mile (exchange/name/instrument_type match nahi hua)"
    live = [d for d in cands if (_expiry_date(d.get("expiry")) or today) >= today
            and _expiry_date(d.get("expiry")) is not None]
    if not live:
        exps = sorted({str(_expiry_date(d.get("expiry"))) for d in cands})
        return None, f"{len(cands)} futures mile par sab expired lag rahe hain. Expiry dates: {exps[:6]}"
    live.sort(key=lambda d: _expiry_date(d["expiry"]))
    return live[0], ""


def _print_diagnostics(data: list, spec: dict):
    nm = spec["fut_name"].upper()
    like = [d for d in data if nm in json.dumps(d).upper() and "FUT" in str(d.get("instrument_type", "")).upper()]
    print(f"\n--- DIAGNOSTIC: '{nm}' wale FUT records: {len(like)} ---")
    combos = sorted({(str(d.get("exchange")), str(d.get("segment")), str(d.get("name")),
                      str(d.get("instrument_type"))) for d in like})
    for c in combos[:10]:
        print("  (exchange, segment, name, instrument_type) =", c)
    for d in like[:2]:
        print("  sample:", json.dumps(d)[:400])
    print("--- (ye output mujhe bhej do) ---\n")


def get_futures_info(names) -> dict:
    """{"NIFTY": {"key","lot_size","expiry"}, ...} - instruments file se.
    Lot size kabhi guess nahi hota; na mile to script ruk jaati hai."""
    from upstox_adapter import INDEX_CONFIG
    cache = _load_cache()
    out = {n: cache.get(n) for n in names if _cache_ok(cache.get(n))}
    missing = [n for n in names if n not in out]

    for n in out:
        print(f"✓ {n}: futures {out[n]['key']} | lot {out[n]['lot_size']} (cache)")
    if not missing:
        return out

    data = _load_instruments()
    today = datetime.now(IST).date()
    failed = []
    for n in missing:
        spec = INDEX_CONFIG[n]
        f, why = select_futures(data, spec, today)
        if not f:
            print(f"❌ {n}: {why}")
            _print_diagnostics(data, spec)
            failed.append(n)
            continue
        if not f.get("lot_size"):
            print(f"❌ {n}: instruments file mein lot_size nahi mila - guess nahi karunga.")
            failed.append(n)
            continue
        out[n] = {"key": f["instrument_key"], "lot_size": int(f["lot_size"]),
                  "expiry": _expiry_date(f["expiry"]).isoformat()}
        print(f"✓ {n}: futures {out[n]['key']} | lot {out[n]['lot_size']} | expiry {out[n]['expiry']}")
        cache[n] = out[n]

    FUT_CACHE_FILE.write_text(json.dumps(cache, indent=2))    # jo mile wo save, taaki dobara na dhoondna pade
    if failed:
        sys.exit(f"Ruk gaye: {', '.join(failed)} ka futures nahi mila (upar diagnostic dekho).")
    return out


# =====================================================================
# Main
# =====================================================================

if __name__ == "__main__":
    if "--setup" in sys.argv:
        setup()
        sys.exit()

    cfg = load_config()
    token = get_access_token(cfg)
    infos = get_futures_info(["NIFTY", "SENSEX"])

    os.environ["UPSTOX_TOKEN"] = token
    os.environ["UPSTOX_NIFTY_FUT"] = infos["NIFTY"]["key"]

    print("\n" + "=" * 50)
    print("ENGINE START HO RAHA HAI (NIFTY + SENSEX parallel)...")
    print("=" * 50 + "\n")

    from upstox_adapter import run_live_multi
    specs = {n: {"key": v["key"], "lot_size": v["lot_size"]} for n, v in infos.items()}
    run_live_multi(token, specs, loop="--once" not in sys.argv)

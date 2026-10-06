"""
UPSTOX ADAPTER  ->  NIFTY OPTION DECISION ENGINE
=================================================
Sirf data laata hai. Koi decision yahan nahi hota.

Endpoints (verified):
  Intraday candles : GET /v3/historical-candle/intraday/{key}/{unit}/{interval}
  Prev-day candles : GET /v3/historical-candle/{key}/{unit}/{interval}/{to}/{from}
  Option contracts : GET /v2/option/contract?instrument_key=...
  Option chain     : GET /v2/option/chain?instrument_key=...&expiry_date=YYYY-MM-DD

⚠ DO ZARURI BAATEIN
1. NSE_INDEX|Nifty 50 par VOLUME hamesha 0 aata hai (index ka volume hota hi
   nahi). Us se VWAP aur volume-confirmation dono mar jaate hain - engine
   chup-chaap galat signal dega. Isliye adapter NIFTY FUTURES ka volume
   uthakar index candles par map karta hai. `futures_key` zaroor do.
2. Upstox `oi` + `prev_oi` deta hai -> ΔOI = oi - prev_oi (prev_oi PICHLE DIN
   ka closing OI hai, intraday running change nahi). Intraday ΔOI chahiye to
   `OIStore` use karo - wo din ke pehle snapshot ko baseline maanta hai.
"""

from __future__ import annotations
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict
from urllib.parse import quote
import time

import requests

from nifty_option_engine import (
    Candle, OptionRow, OptionChain, PrevDay, MarketInput,
    DecisionEngine, render, Position, update_position,
    config_for_index, use_config, status_color, _panel_html, render_page,
)

BASE = "https://api.upstox.com"
NIFTY_INDEX = "NSE_INDEX|Nifty 50"
SENSEX_INDEX = "BSE_INDEX|SENSEX"

# Har index ki settings ek jagah. Lot size yahan NAHI hai - wo hamesha Upstox se aata hai.
#  pts_scale : Nifty ke point-thresholds (buffer 5 / retest 18 / proximity 25) ko index level
#              ke ratio se scale karta hai (Sensex ~3.3x Nifty).
#  overrides : Config ke koi bhi field index-wise badalne ke liye. Sensex options Nifty se kam
#              liquid hain, isliye min OI/volume neeche rakhe (spread check wahi 1.5% hai).
INDEX_CONFIG = {
    "NIFTY":  dict(label="NIFTY",  index_key=NIFTY_INDEX,  fut_exchange="NSE_FO", fut_name="NIFTY",
                   pts_scale=1.0, overrides={}),
    "SENSEX": dict(label="SENSEX", index_key=SENSEX_INDEX, fut_exchange="BSE_FO", fut_name="SENSEX",
                   pts_scale=3.3, overrides=dict(min_oi=30_000, min_volume=10_000)),
}


class UpstoxError(RuntimeError):
    pass


# =====================================================================
# Intraday OI baseline store (ΔOI ke liye)
# =====================================================================

class OIStore:
    """
    Upstox ka prev_oi = pichle din ka close OI. Intraday buildup dekhne ke liye
    hum din ke pehle snapshot ko baseline rakhte hain aur us se change nikaalte
    hain. Optional: agar tum prev-day change hi chahte ho, use_intraday=False.
    """

    def __init__(self, use_intraday: bool = True):
        self.use_intraday = use_intraday
        self._base: Dict[str, int] = {}
        self._day: Optional[date] = None

    def delta(self, key: str, oi: int, prev_oi: int, today: date) -> int:
        if not self.use_intraday:
            return oi - prev_oi
        if self._day != today:
            self._base.clear()
            self._day = today
        if key not in self._base:
            # pehla snapshot: baseline prev_oi rakho taki turant 0 na dikhe
            self._base[key] = prev_oi if prev_oi else oi
        return oi - self._base[key]


class PremiumStore:
    """
    Har strike ka premium rolling snapshots mein rakhta hai taaki ~window_min
    pehle ka premium mil sake (intraday premium validation ke liye).
    History na ho (script abhi start hui) to 0 return -> engine check skip karta hai.
    """

    def __init__(self, window_min: int = 10):
        self.window = timedelta(minutes=window_min)
        self._h: Dict[str, List[tuple]] = {}
        self._day: Optional[date] = None

    def recent(self, key: str, ltp: float, now: datetime) -> float:
        if self._day != now.date():
            self._h.clear()
            self._day = now.date()
        hist = self._h.setdefault(key, [])
        ref = 0.0
        for ts, v in hist:                    # purane -> naye
            if now - ts >= self.window:
                ref = v                       # sabse naya jo kam se kam window purana ho
        if ltp:
            hist.append((now, ltp))
        cutoff = now - self.window * 3
        self._h[key] = [(t, v) for t, v in hist if t >= cutoff]
        return ref


# =====================================================================
# Client
# =====================================================================

class UpstoxFeed:
    def __init__(self, access_token: str,
                 index_key: str = NIFTY_INDEX,
                 futures_key: Optional[str] = None,
                 timeframe_min: int = 5,
                 intraday_oi: bool = True,
                 timeout: int = 10):
        self.token = access_token
        self.index_key = index_key
        self.futures_key = futures_key
        self.tf = timeframe_min
        self.timeout = timeout
        self.oi_store = OIStore(intraday_oi)
        self.prem_store = PremiumStore(10)
        self.s = requests.Session()
        self.s.headers.update({
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        })

    # ---------- low level ----------
    def _get(self, path: str, params: Optional[dict] = None, retries: int = 2) -> dict:
        url = BASE + path
        for attempt in range(retries + 1):
            try:
                r = self.s.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as e:
                if attempt == retries:
                    raise UpstoxError(f"Network fail {path}: {e}")
                time.sleep(1 + attempt); continue

            if r.status_code == 401:
                raise UpstoxError("401 - access_token expire ho gaya (roz subah naya lo)")
            if r.status_code == 429:
                time.sleep(2 + attempt * 2); continue
            if r.status_code >= 400:
                raise UpstoxError(f"{r.status_code} {path}: {r.text[:200]}")

            j = r.json()
            if j.get("status") != "success":
                raise UpstoxError(f"API status={j.get('status')} {str(j)[:200]}")
            return j
        raise UpstoxError(f"Rate limited: {path}")

    # ---------- candles ----------
    @staticmethod
    def _parse_candles(rows: List[list]) -> List[Candle]:
        """Upstox row: [ts, open, high, low, close, volume, oi] - newest first."""
        out = []
        for row in rows:
            ts = datetime.fromisoformat(row[0])
            out.append(Candle(ts.replace(tzinfo=None),
                              float(row[1]), float(row[2]), float(row[3]),
                              float(row[4]), float(row[5])))
        out.sort(key=lambda c: c.ts)          # engine ko oldest->newest chahiye
        return out

    def _intraday(self, key: str) -> List[Candle]:
        k = quote(key, safe="")   # "NSE_INDEX|Nifty 50" -> encode zaroori
        j = self._get(f"/v3/historical-candle/intraday/{k}/minutes/{self.tf}")
        return self._parse_candles(j["data"]["candles"])

    def _daily(self, key: str, days_back: int = 10) -> List[Candle]:
        to_d = date.today()
        from_d = to_d - timedelta(days=days_back)
        k = quote(key, safe="")
        j = self._get(f"/v3/historical-candle/{k}/days/1/{to_d}/{from_d}")
        return self._parse_candles(j["data"]["candles"])

    def candles(self) -> List[Candle]:
        """Index candles + futures volume mapped on (index ka apna volume 0 hota hai)."""
        idx = self._intraday(self.index_key)
        if not idx:
            raise UpstoxError("Intraday candles khaali - market band hai ya key galat")

        if self.futures_key:
            fut = {c.ts: c.volume for c in self._intraday(self.futures_key)}
            missing = 0
            for c in idx:
                v = fut.get(c.ts, 0.0)
                if v:
                    c.volume = v
                else:
                    missing += 1
            if missing > len(idx) * 0.3:
                print(f"⚠ {missing}/{len(idx)} candles ka futures volume match nahi hua "
                      f"- futures_key ya timeframe check karo")
        elif all(c.volume == 0 for c in idx):
            raise UpstoxError(
                "Index candles me volume 0 hai aur futures_key nahi diya. "
                "Volume ke bina VWAP aur breakout confirmation galat honge. "
                "futures_key pass karo (e.g. current-month NIFTY FUT)."
            )
        return idx

    def warmup_candles(self, max_candles: int = 60) -> List[Candle]:
        """Pichle sessions ki candles (sirf ADX warm-up). Din mein ek baar fetch, phir cache.
        Fail ho to khaali list - engine pehle jaise chalega."""
        today = date.today()
        if getattr(self, "_wu_day", None) == today:
            return self._wu
        try:
            k = quote(self.index_key, safe="")
            j = self._get(f"/v3/historical-candle/{k}/minutes/{self.tf}/{today}/{today - timedelta(days=6)}")
            rows = self._parse_candles(j["data"]["candles"])
            self._wu = [c for c in rows if c.ts.date() < today][-max_candles:]
        except Exception as e:
            print(f"⚠ ADX warm-up candles nahi mile ({e}) - intraday data se hi chalega")
            self._wu = []
        self._wu_day = today
        return self._wu

    def prev_day(self) -> PrevDay:
        d = self._daily(self.index_key)
        if len(d) < 2:
            raise UpstoxError("Prev-day data nahi mila")
        p = d[-2]            # last = aaj, second-last = pichla session
        return PrevDay(p.high, p.low, p.close)

    # ---------- option chain ----------
    def nearest_expiry(self) -> str:
        j = self._get("/v2/option/contract", {"instrument_key": self.index_key})
        exps = sorted({row["expiry"][:10] for row in j["data"]})
        today = date.today().isoformat()
        future = [e for e in exps if e >= today]
        if not future:
            raise UpstoxError("Koi upcoming expiry nahi mili")
        return future[0]

    def contract_lot_size(self, expiry: str) -> Optional[int]:
        """Us expiry ke option contracts ka lot size (jo tum actually trade karte ho).
        Ek hi value na mile to None - guess nahi karte."""
        j = self._get("/v2/option/contract", {"instrument_key": self.index_key})
        lots = {int(r["lot_size"]) for r in j["data"]
                if str(r.get("expiry", ""))[:10] == expiry and r.get("lot_size")}
        return lots.pop() if len(lots) == 1 else None

    def option_chain(self, expiry: Optional[str] = None) -> OptionChain:
        expiry = expiry or self.nearest_expiry()
        j = self._get("/v2/option/chain",
                      {"instrument_key": self.index_key, "expiry_date": expiry})
        data = j["data"]
        if not data:
            raise UpstoxError(f"Option chain khaali for {expiry}")

        today = date.today()
        now = datetime.now()
        spot = float(data[0].get("underlying_spot_price") or 0)
        rows: List[OptionRow] = []

        for d in data:
            ce, pe = d.get("call_options") or {}, d.get("put_options") or {}
            cm, pm = ce.get("market_data") or {}, pe.get("market_data") or {}
            cg, pg = ce.get("option_greeks") or {}, pe.get("option_greeks") or {}
            strike = float(d["strike_price"])

            ce_oi, ce_prev = int(cm.get("oi") or 0), int(cm.get("prev_oi") or 0)
            pe_oi, pe_prev = int(pm.get("oi") or 0), int(pm.get("prev_oi") or 0)

            rows.append(OptionRow(
                strike=strike,
                ce_ltp=float(cm.get("ltp") or 0), pe_ltp=float(pm.get("ltp") or 0),
                # close_price = pichle din ka close -> premium movement ka base
                ce_prev_ltp=float(cm.get("close_price") or 0),
                pe_prev_ltp=float(pm.get("close_price") or 0),
                ce_oi=ce_oi, pe_oi=pe_oi,
                ce_doi=self.oi_store.delta(f"CE{strike}", ce_oi, ce_prev, today),
                pe_doi=self.oi_store.delta(f"PE{strike}", pe_oi, pe_prev, today),
                ce_volume=int(cm.get("volume") or 0),
                pe_volume=int(pm.get("volume") or 0),
                ce_iv=float(cg.get("iv") or 0), pe_iv=float(pg.get("iv") or 0),
                ce_bid=float(cm.get("bid_price") or 0), ce_ask=float(cm.get("ask_price") or 0),
                pe_bid=float(pm.get("bid_price") or 0), pe_ask=float(pm.get("ask_price") or 0),
                ce_delta=float(cg.get("delta") or 0), pe_delta=float(pg.get("delta") or 0),
                ce_gamma=float(cg.get("gamma") or 0), pe_gamma=float(pg.get("gamma") or 0),
                ce_theta=float(cg.get("theta") or 0), pe_theta=float(pg.get("theta") or 0),
                ce_vega=float(cg.get("vega") or 0), pe_vega=float(pg.get("vega") or 0),
                ce_recent_ltp=self.prem_store.recent(f"CE{strike}", float(cm.get("ltp") or 0), now),
                pe_recent_ltp=self.prem_store.recent(f"PE{strike}", float(pm.get("ltp") or 0), now),
            ))

        rows.sort(key=lambda r: r.strike)
        if not spot:
            spot = rows[len(rows) // 2].strike
        return OptionChain(spot=spot, rows=rows, expiry=expiry)

    # ---------- one shot ----------
    def snapshot(self, expiry: Optional[str] = None) -> MarketInput:
        return MarketInput(
            candles=self.candles(),
            prev_day=self.prev_day(),
            chain=self.option_chain(expiry),
            warmup=self.warmup_candles(),
            now=datetime.now(),
        )


# =====================================================================
# Live loop
# =====================================================================

def _local_ip() -> str:
    """PC ka WiFi/LAN IP pata karo (internet bheje bina, sirf routing check)."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def _start_lan_server(directory: str, port: int = 8000):
    """Dashboard ko WiFi par serve karo taaki phone se bhi khul sake."""
    import http.server
    import threading
    import functools

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=directory)
    handler.log_message = lambda *a, **k: None   # console spam band
    httpd = http.server.ThreadingHTTPServer(("0.0.0.0", port), handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    return port


class IndexRunner:
    """Ek index (Nifty / Sensex) ka poora state: feed, engine, position, re-entry guard."""

    def __init__(self, key: str, token: str, fut_key: Optional[str], fut_lot: Optional[int],
                 max_risk: float, timeframe_min: int):
        spec = INDEX_CONFIG[key]
        self.k, self.spec = key, spec
        self.key, self.name = key.lower(), spec["label"]
        self.feed = UpstoxFeed(token, index_key=spec["index_key"], futures_key=fut_key,
                               timeframe_min=timeframe_min)
        self.fut_lot, self.max_risk = fut_lot, max_risk
        self.engine = DecisionEngine()
        self.expiry: Optional[str] = None
        self._exp_day: Optional[date] = None
        self.cfg = None
        self.position: Optional[Position] = None
        self.pos_sig = None       # open position ka (side, entry-level)
        self.blocked = None       # close hui trade ka signature - wahi setup dobara nahi
        self.last_status = None
        self.last_d = None

    def setup(self):
        """Expiry + lot size nikalta hai. Lot size na mile to guess nahi, band."""
        self.expiry = self.feed.nearest_expiry()
        self._exp_day = date.today()
        opt_lot = None
        try:
            opt_lot = self.feed.contract_lot_size(self.expiry)
        except UpstoxError as e:
            print(f"⚠ {self.name}: option contract lot size nahi mila ({e})")
        lot = opt_lot or self.fut_lot
        if not lot:
            raise SystemExit(f"❌ {self.name} ka lot size Upstox se nahi mila - guess nahi karunga. "
                             f"futures_key_cache.json delete karke dobara chalao.")
        if opt_lot and self.fut_lot and opt_lot != self.fut_lot:
            print(f"⚠ {self.name}: options lot {opt_lot} != futures lot {self.fut_lot} - options wala use ho raha hai")
        self.cfg = config_for_index(self.name, self.spec["pts_scale"], lot, **self.spec["overrides"])
        print(f"{self.name}: expiry {self.expiry} | lot size {lot} "
              f"({'option contract' if opt_lot else 'futures'}) | points x{self.spec['pts_scale']}")

    def _refresh_if_new_day(self):
        if self._exp_day != date.today():
            self.setup()

    def _summary(self, d, notice="") -> dict:
        pos = self.position
        if pos is not None and pos.status == "OPEN":
            sub = f"IN {pos.side} {pos.strike:,.0f} | P&L ₹{pos.pnl:+,.0f}"
        else:
            sub = d.reason[:55]
        return dict(key=self.key, label=self.name, status=d.status, color=status_color(d),
                    score=f"{d.score}/10", sub=sub)

    def tick(self) -> dict:
        self._refresh_if_new_day()
        snap = self.feed.snapshot(self.expiry)
        with use_config(self.cfg):
            d = self.engine.run(snap, max_risk=self.max_risk)
            ready = d.status.endswith("READY") and d.risk.ok
            sig = (d.side, round(d.entry.level)) if ready else None
            notice = ""

            if self.position is None:
                if not ready:
                    self.blocked = None            # setup hat gaya -> guard reset
                elif self.blocked is not None and sig == self.blocked:
                    notice = ("⏸ Pichli trade close ho chuki. Ye wahi purana setup hai - "
                              "naye signal ka intezaar (status READY se hat ke wapas aaye tab).")
                else:
                    self.position = Position(
                        side=d.side, strike=d.strike.strike, entry_premium=d.risk.entry_premium,
                        sl=d.risk.premium_sl, targets=d.risk.targets,
                        targets_hit=[False] * len(d.risk.targets), lots=d.risk.lots,
                        opened_at=datetime.now(), status="OPEN",
                        current_premium=d.risk.entry_premium, pnl=0.0, lot_size=self.cfg.lot_size)
                    self.pos_sig, self.blocked = sig, None
                    warn = (f" | ⚠ 1 lot risk ₹{d.risk.per_lot_risk:,.0f} > ₹{d.risk.max_risk:,.0f}"
                            if d.risk.over_limit else "")
                    print(f"  📈 {self.name} POSITION OPENED: {self.position.side} {self.position.strike:,.0f} "
                          f"@ ₹{self.position.entry_premium:.1f} | SL ₹{self.position.sl:.1f} | "
                          f"Targets {['₹%.0f' % t for t in self.position.targets]}{warn}")
            elif self.position.status == "OPEN":
                row = snap.chain.row(self.position.strike)
                if row:
                    cur = row.ce_ltp if self.position.side == "CE" else row.pe_ltp
                    prev_hits = list(self.position.targets_hit)
                    self.position = update_position(self.position, cur)
                    if self.position.status == "SL_HIT":
                        print(f"  🔴 {self.name} SL HIT @ ₹{cur:.1f} | P&L ₹{self.position.pnl:+,.0f}")
                    elif self.position.status == "FINAL_TARGET_HIT":
                        print(f"  🟢 {self.name} FINAL TARGET HIT @ ₹{cur:.1f} | P&L ₹{self.position.pnl:+,.0f}")
                    else:
                        for n, (old, new) in enumerate(zip(prev_hits, self.position.targets_hit), 1):
                            if new and not old:
                                print(f"  🎯 {self.name} T{n} HIT @ ₹{cur:.1f} - consider partial booking")
                    if self.position.status != "OPEN":
                        self.blocked = self.pos_sig    # same setup se turant re-entry nahi

            panel = _panel_html(d, self.position, self.key, notice)
            summary = self._summary(d)
            self.last_d = d
            print(f"[{datetime.now():%H:%M:%S}] {self.name}: {d.status} - {d.reason}")
            if d.status != self.last_status:
                print(f"  >>> {self.name} STATUS CHANGE: {self.last_status} -> {d.status}")
                self.last_status = d.status
            # Closed position agli scan cycle ke liye clear (is cycle mein dashboard par dikh chuki)
            if self.position is not None and self.position.status != "OPEN":
                self.position = None
        return dict(summary, html=panel)

    def cycle(self) -> dict:
        """tick() + error handling: ek index fail ho to doosra chalta rahe."""
        try:
            return self.tick()
        except UpstoxError as e:
            msg = str(e)
        except Exception as e:
            msg = f"unexpected: {e!r}"
        print(f"[{datetime.now():%H:%M:%S}] {self.name}: {msg}")
        notice = f"⚠ {self.name} data error: {msg[:120]}"
        if self.last_d is not None:
            with use_config(self.cfg):
                panel = _panel_html(self.last_d, self.position, self.key, notice)
            return dict(self._summary(self.last_d), html=panel, sub="data error - purana data")
        panel = (f'<section class="panel" id="p-{self.key}"><div class="notice">{notice}</div></section>')
        return dict(key=self.key, label=self.name, status="NO DATA", color="#64748b",
                    score="", sub=msg[:55], html=panel)


def _write_atomic(path, text: str):
    """Temp file mein likh ke replace - browser kabhi aadhi file nahi padhta."""
    import os
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    for _ in range(5):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:      # Windows: file us pal browser/server ne khuli rakhi
            time.sleep(0.2)
    path.write_text(text, encoding="utf-8")


def run_live_multi(access_token: str, specs: Dict[str, dict],
                   max_risk: float = 2000, interval_sec: int = 60,
                   timeframe_min: int = 5, loop: bool = True,
                   html_path: str = "dashboard.html",
                   lan_share: bool = True, lan_port: int = 8000):
    """specs: {"NIFTY": {"key": futures_key, "lot_size": fut_lot}, "SENSEX": {...}}
    Sab index parallel (ek hi loop mein), ek dashboard par."""
    import webbrowser, pathlib
    runners = [IndexRunner(k, access_token, v.get("key"), v.get("lot_size"), max_risk, timeframe_min)
               for k, v in specs.items()]
    for r in runners:
        r.setup()
    print(f"TF: {timeframe_min}m | 1-lot risk warning limit: ₹{max_risk:,.0f}\n")

    html_file = pathlib.Path(html_path).resolve()
    if lan_share:
        try:
            _start_lan_server(str(html_file.parent), lan_port)
            print(f"📱 Mobile par dekhne ke liye (SAME WiFi zaroori):")
            print(f"   http://{_local_ip()}:{lan_port}/{html_file.name}\n")
        except Exception as e:
            print(f"⚠ LAN server start nahi hua: {e}\n")

    opened = False
    title = " + ".join(r.name for r in runners) + " Option Engine"
    page_refresh = min(20, interval_sec)      # browser engine se zyada baar refresh kare, data fresh dikhe
    while True:
        t0 = time.time()
        panels = [r.cycle() for r in runners]
        page = render_page(panels, page_refresh, title, engine_sec=interval_sec, data_ts=time.time())
        _write_atomic(html_file, page)
        if not opened:
            webbrowser.open(f"file:///{html_file}")
            opened = True
        if not loop:
            return
        # fixed period: fetch mein jitna time laga, utna sleep kam (cycle ~60s hi rahe)
        time.sleep(max(5.0, interval_sec - (time.time() - t0)))


def run_live(access_token: str, futures_key: str, **kw):
    """Purana Nifty-only interface (compat)."""
    run_live_multi(access_token, {"NIFTY": {"key": futures_key, "lot_size": None}}, **kw)


if __name__ == "__main__":
    import os, sys
    tok = os.environ.get("UPSTOX_TOKEN")
    fut = os.environ.get("UPSTOX_NIFTY_FUT")     # e.g. "NSE_FO|53001"
    if not tok:
        sys.exit("UPSTOX_TOKEN env var set karo")
    run_live(tok, fut, loop="--once" not in sys.argv)

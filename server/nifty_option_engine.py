"""
NIFTY OPTION DECISION ENGINE
============================
Rule-based, data-source agnostic. Broker/API sirf data feed karta hai,
decision poora is file ke andar hota hai.

PIPELINE
  Price Action -> Levels -> Location -> Option Chain -> dOI -> Strike/Liquidity
  -> Premium Validation -> IV -> Entry Trigger -> Risk -> Confluence
  -> Mandatory Gates -> FINAL DECISION (READY / WATCH / WAIT / NO TRADE)
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from datetime import datetime, time
import math
import dataclasses
import contextlib

# =====================================================================
# 0. CONFIG  (sab thresholds ek jagah - tuning yahin karni hai)
# =====================================================================

@dataclass
class Config:
    # --- Trend / structure ---
    ema_fast: int = 9
    ema_slow: int = 21
    swing_lookback: int = 2          # fractal ke left/right bars
    adx_period: int = 14
    adx_strong: float = 25.0         # > iske upar = strong trend
    adx_weak: float = 18.0           # < iske niche = range-ish

    # --- Levels / location ---
    level_proximity_pts: float = 25.0   # level ke kitne paas = "NEAR"
    middle_zone_pct: float = 0.35       # range ke beech ka 35% = middle
    opening_range_min: int = 15         # ORB minutes

    # --- Option liquidity ---
    min_oi: int = 100_000
    min_volume: int = 50_000
    max_spread_pct: float = 1.5         # (ask-bid)/mid * 100
    min_premium: float = 20.0
    max_premium: float = 400.0

    # --- dOI significance ---
    doi_significant_pct: float = 5.0    # change vs OI %

    # --- IV ---
    iv_low: float = 10.0
    iv_normal_hi: float = 16.0
    iv_high_hi: float = 22.0            # iske upar = EXTREME

    # --- Premium validation ---
    min_premium_move_pct: float = 3.0   # (ab use nahi hota - kal ke close se compare galat tha)
    premium_recent_min_pct: float = 0.0 # premium pichle ~10 min mein is % se kam na gira ho
    min_delta_proxy: float = 0.35       # ATM ke aas paas hi rehna

    # --- Entry trigger ---
    breakout_buffer_pts: float = 5.0
    retest_tolerance_pts: float = 18.0  # 12->18: retest thoda door se bhi valid (middle-ground)
    volume_surge_mult: float = 1.3      # avg volume ka multiple - waisa hi strict
    max_bars_for_retest: int = 4        # 8->4: retest window 40min->20min (entry jaldi)
    fast_track_volume_mult: float = 1.8 # itna strong volume ho to retest SKIP, turant entry
    breakout_scan_bars: int = 12        # itni door tak breakout candle dhoondo (retest window se alag)
    min_body_ratio: float = 0.35        # breakout candle ka body >=45% range ho (weak/doji reject)
    require_vwap_hold: bool = True      # retest ke time price sahi side of VWAP pe rahe

    # --- Risk ---
    # SIRF offline demo/test ke liye default. Live run mein lot size hamesha
    # Upstox instruments/contract data se aata hai - kabhi guess nahi hota.
    lot_size: int = 65
    index_name: str = "NIFTY"
    max_risk_rupees: float = 2000.0
    premium_sl_pct: float = 25.0        # fallback premium SL
    rr_targets: Tuple[float, ...] = (1.0, 2.0, 3.0)

    # --- Score buckets ---
    score_no_trade: int = 4
    score_wait: int = 6
    score_watch: int = 7
    score_ready: int = 8

    # --- Session ---
    no_trade_before: time = time(9, 30)
    no_trade_after: time = time(15, 0)


CFG = Config()


def config_for_index(name: str, pts_scale: float = 1.0, lot_size: Optional[int] = None, **over) -> Config:
    """Index-wise config. Point-based thresholds (Nifty ke hisaab se likhe the) index ke
    level ke ratio se scale hote hain; premium bounds bhi (Sensex premium ~3x bada)."""
    base = Config()
    s_ = pts_scale
    kw = dict(
        index_name=name,
        level_proximity_pts=round(base.level_proximity_pts * s_),
        breakout_buffer_pts=round(base.breakout_buffer_pts * s_),
        retest_tolerance_pts=round(base.retest_tolerance_pts * s_),
        min_premium=round(base.min_premium * s_),
        max_premium=round(base.max_premium * s_),
    )
    if lot_size:
        kw["lot_size"] = int(lot_size)
    kw.update(over)
    return dataclasses.replace(base, **kw)


@contextlib.contextmanager
def use_config(cfg: Config):
    """Engine ke andar global CFG hai; ek index ka run/render/position-update iske andar karo."""
    names = [f.name for f in dataclasses.fields(Config)]
    saved = {n: getattr(CFG, n) for n in names}
    try:
        for n in names:
            setattr(CFG, n, getattr(cfg, n))
        yield cfg
    finally:
        for n, v in saved.items():
            setattr(CFG, n, v)


# =====================================================================
# 1. DATA MODELS
# =====================================================================

@dataclass
class Candle:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class OptionRow:
    strike: float
    ce_ltp: float = 0.0
    pe_ltp: float = 0.0
    ce_oi: int = 0
    pe_oi: int = 0
    ce_doi: int = 0            # change in OI
    pe_doi: int = 0
    ce_volume: int = 0
    pe_volume: int = 0
    ce_iv: float = 0.0
    pe_iv: float = 0.0
    ce_bid: float = 0.0
    ce_ask: float = 0.0
    pe_bid: float = 0.0
    pe_ask: float = 0.0
    ce_prev_ltp: float = 0.0   # premium movement validation ke liye
    pe_prev_ltp: float = 0.0
    ce_delta: float = 0.0      # broker se mile to real delta, warna 0
    pe_delta: float = 0.0
    ce_gamma: float = 0.0
    pe_gamma: float = 0.0
    ce_theta: float = 0.0
    pe_theta: float = 0.0
    ce_vega: float = 0.0
    pe_vega: float = 0.0
    ce_recent_ltp: float = 0.0   # ~10 min pehle ka premium (adapter rolling store); 0 = history abhi nahi
    pe_recent_ltp: float = 0.0


@dataclass
class OptionChain:
    spot: float
    rows: List[OptionRow]
    expiry: str = ""

    def step(self) -> float:
        """Strike gap chain se nikalta hai (Nifty 50, Sensex 100)."""
        ks = sorted({r.strike for r in self.rows})
        diffs = [b - a for a, b in zip(ks, ks[1:]) if b - a > 0]
        return min(diffs) if diffs else 50.0

    def atm(self, step: Optional[float] = None) -> float:
        step = step or self.step()
        return round(self.spot / step) * step

    def row(self, strike: float) -> Optional[OptionRow]:
        for r in self.rows:
            if abs(r.strike - strike) < 1e-6:
                return r
        return None


@dataclass
class PrevDay:
    high: float
    low: float
    close: float


@dataclass
class MarketInput:
    """Engine ko sirf yeh dena hai."""
    candles: List[Candle]          # intraday (5m recommended), oldest -> newest
    prev_day: PrevDay
    chain: OptionChain
    now: datetime = field(default_factory=datetime.now)
    iv_history: List[float] = field(default_factory=list)  # optional context
    warmup: List[Candle] = field(default_factory=list)     # pichle din ki candles - sirf ADX warm-up ke liye


# ---- Reason panel item ----
@dataclass
class Signal:
    label: str
    state: str          # "PASS" | "WARN" | "FAIL" | "INFO"
    detail: str = ""

    @property
    def icon(self) -> str:
        return {"PASS": "🟢", "WARN": "🟡", "FAIL": "🔴", "INFO": "⚪"}[self.state]


# =====================================================================
# 2. INDICATOR HELPERS
# =====================================================================

def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def vwap(candles: List[Candle]) -> float:
    pv = sum(((c.high + c.low + c.close) / 3) * c.volume for c in candles)
    v = sum(c.volume for c in candles)
    return pv / v if v else candles[-1].close


def adx(candles: List[Candle], period: int) -> float:
    """Wilder's ADX - simplified but faithful."""
    if len(candles) < period * 2:
        return 0.0
    tr, plus_dm, minus_dm = [], [], []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        tr.append(max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close)))
        up, dn = c.high - p.high, p.low - c.low
        plus_dm.append(up if (up > dn and up > 0) else 0.0)
        minus_dm.append(dn if (dn > up and dn > 0) else 0.0)

    def rma(x):
        s = sum(x[:period])
        out = [s / period]
        for v in x[period:]:
            out.append((out[-1] * (period - 1) + v) / period)
        return out

    atr_s, pdm_s, mdm_s = rma(tr), rma(plus_dm), rma(minus_dm)
    dx = []
    for a, p_, m_ in zip(atr_s, pdm_s, mdm_s):
        if a == 0:
            continue
        pdi, mdi = 100 * p_ / a, 100 * m_ / a
        if pdi + mdi:
            dx.append(100 * abs(pdi - mdi) / (pdi + mdi))
    if len(dx) < period:
        return sum(dx) / len(dx) if dx else 0.0
    return sum(dx[-period:]) / period


def atr(candles: List[Candle], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        c, p = candles[i], candles[i - 1]
        trs.append(max(c.high - c.low, abs(c.high - p.close), abs(c.low - p.close)))
    trs = trs[-period:]
    return sum(trs) / len(trs)


def swing_points(candles: List[Candle], lb: int) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
    """
    ATR-based ZigZag swings. Raw fractals noise dete hain, isliye ek minimum
    deviation (ATR ka multiple) ke baad hi naya swing maana jaata hai.
    Return: (swing_highs, swing_lows) as [(index, price), ...]
    """
    if len(candles) < 5:
        return [], []
    dev = max(atr(candles, 14) * 1.2, 8.0)
    highs, lows = [], []

    direction = 0          # +1 = up leg, -1 = down leg
    piv_i, piv_p = 0, candles[0].close

    for i, c in enumerate(candles):
        if direction >= 0 and c.high > piv_p:
            piv_i, piv_p = i, c.high
            direction = 1
        elif direction <= 0 and c.low < piv_p:
            piv_i, piv_p = i, c.low
            direction = -1

        if direction == 1 and c.low <= piv_p - dev:
            highs.append((piv_i, piv_p))
            direction, piv_i, piv_p = -1, i, c.low
        elif direction == -1 and c.high >= piv_p + dev:
            lows.append((piv_i, piv_p))
            direction, piv_i, piv_p = 1, i, c.high

    # chalta hua (unconfirmed) leg bhi include karo - live structure ke liye
    if direction == 1:
        highs.append((piv_i, piv_p))
    elif direction == -1:
        lows.append((piv_i, piv_p))
    return highs, lows


# =====================================================================
# 3. PRICE ACTION ENGINE  (Sec 2 + 3)
# =====================================================================

@dataclass
class TrendResult:
    trend: str          # BULLISH / BEARISH / RANGE
    strength: str       # STRONG / NORMAL / WEAK
    label: str          # STRONG BULLISH etc.
    vwap: float
    adx: float
    ema_fast: float
    ema_slow: float
    structure: str      # HH-HL / LL-LH / MIXED
    signals: List[Signal]


class PriceActionEngine:
    def run(self, m: MarketInput) -> TrendResult:
        c = m.candles
        closes = [x.close for x in c]
        ef = ema(closes, CFG.ema_fast)[-1]
        es = ema(closes, CFG.ema_slow)[-1]
        vw = vwap(c)
        adx_v = adx(list(m.warmup) + list(c), CFG.adx_period)   # warm-up: subah ADX 0.0 na aaye
        price = closes[-1]

        hs, ls = swing_points(c, CFG.swing_lookback)
        structure = "MIXED"
        if len(hs) >= 2 and len(ls) >= 2:
            hh = hs[-1][1] > hs[-2][1]
            hl = ls[-1][1] > ls[-2][1]
            ll = ls[-1][1] < ls[-2][1]
            lh = hs[-1][1] < hs[-2][1]
            if hh and hl:
                structure = "HH-HL"
            elif ll and lh:
                structure = "LL-LH"

        if structure == "MIXED" and len(c) >= 6:
            # Zigzag inconclusive (smooth grind / shallow pullback) -> half-vs-half
            # comparison se structure infer karo. '*' = inferred, confirmed nahi.
            # len(c)>=6 zaroori hai warna market-open ke chand candles mein
            # mid=0 ban jaata hai aur max()/min() empty list par crash karte hain.
            mid = len(c) // 2
            h1, h2 = max(x.high for x in c[:mid]), max(x.high for x in c[mid:])
            l1, l2 = min(x.low for x in c[:mid]), min(x.low for x in c[mid:])
            if h2 > h1 and l2 > l1:
                structure = "HH-HL*"
            elif h2 < h1 and l2 < l1:
                structure = "LL-LH*"

        base = structure.rstrip("*")

        sig: List[Signal] = []

        # 2-out-of-3 (structure, VWAP, EMA) - teeno chahiye tha pehle, ab thoda relax
        bull_votes = (base == "HH-HL") + (price > vw) + (ef > es)
        bear_votes = (base == "LL-LH") + (price < vw) + (ef < es)
        bull = bull_votes >= 2
        bear = bear_votes >= 2

        if bull:
            trend = "BULLISH"
        elif bear:
            trend = "BEARISH"
        else:
            trend = "RANGE"

        # ADX se strength
        if adx_v >= CFG.adx_strong:
            strength = "STRONG"
        elif adx_v >= CFG.adx_weak:
            strength = "NORMAL"
        else:
            strength = "WEAK"

        # Weak ADX + directional structure = effectively range
        if trend != "RANGE" and strength == "WEAK":
            sig.append(Signal("Trend Strength", "WARN", f"ADX {adx_v:.1f} weak - trend bharosemand nahi"))

        label = "RANGE / CHOPPY" if trend == "RANGE" else f"{strength} {trend}".replace("NORMAL ", "")

        sig.insert(0, Signal("Trend",
                             "PASS" if trend != "RANGE" else "FAIL",
                             f"{label} | structure {structure}"))
        sig.append(Signal("Market Structure",
                          "PASS" if base != "MIXED" else "FAIL",
                          structure + (" (inferred)" if structure.endswith("*") else "")))
        sig.append(Signal("VWAP",
                          "PASS" if (trend == "BULLISH" and price > vw) or (trend == "BEARISH" and price < vw) else "FAIL",
                          f"Spot {price:.0f} vs VWAP {vw:.0f}"))

        return TrendResult(trend, strength, label, vw, adx_v, ef, es, structure, sig)


# =====================================================================
# 4. LEVELS ENGINE  (Sec 4)
# =====================================================================

@dataclass
class Levels:
    pdh: float
    pdl: float
    pdc: float
    day_high: float
    day_low: float
    vwap: float
    or_high: float
    or_low: float
    swing_highs: List[float]
    swing_lows: List[float]
    support: float
    resistance: float
    call_wall: float
    put_support: float
    key_levels: List[float]      # sab confluence levels (clustered)
    signals: List[Signal]


def cluster(levels: List[float], tol: float) -> List[float]:
    """Paas-paas ke levels ko merge karo (confluence zone)."""
    vals = sorted(x for x in levels if x)
    out: List[List[float]] = []
    for v in vals:
        if out and v - out[-1][-1] <= tol:
            out[-1].append(v)
        else:
            out.append([v])
    return [sum(g) / len(g) for g in out]


class LevelsEngine:
    def run(self, m: MarketInput, tr: TrendResult, oc: "OIResult") -> Levels:
        c = m.candles
        spot = c[-1].close
        day_high = max(x.high for x in c)
        day_low = min(x.low for x in c)

        or_bars = max(1, CFG.opening_range_min // self._tf_minutes(c))
        or_high = max(x.high for x in c[:or_bars])
        or_low = min(x.low for x in c[:or_bars])

        hs, ls = swing_points(c, CFG.swing_lookback)
        # last few bars ke swings noise hote hain - established levels chahiye
        cutoff = len(c) - 5
        sh = [p for i, p in hs if i < cutoff]
        sl = [p for i, p in ls if i < cutoff]

        # multiple methods combine (day high/low apne hi side pe include nahi,
        # warna uptrend me "resistance = current price" ban jaata hai)
        res_cands = [m.prev_day.high, m.prev_day.close, or_high, oc.call_wall] + sh
        sup_cands = [m.prev_day.low, m.prev_day.close, or_low, oc.put_support] + sl

        key = cluster(res_cands + sup_cands, CFG.level_proximity_pts * 0.6)

        above = [x for x in key if x > spot + CFG.breakout_buffer_pts]
        below = [x for x in key if x < spot - CFG.breakout_buffer_pts]

        a = atr(c)
        resistance = min(above) if above else round(spot + a * 2)
        support = max(below) if below else round(spot - a * 2)

        sig = [Signal("Important Level",
                      "PASS" if (above and below) else "WARN",
                      f"S {support:.0f} / R {resistance:.0f} | {len(key)} key levels")]

        return Levels(m.prev_day.high, m.prev_day.low, m.prev_day.close,
                      day_high, day_low, tr.vwap, or_high, or_low,
                      sh, sl, support, resistance,
                      oc.call_wall, oc.put_support, key, sig)

    @staticmethod
    def _tf_minutes(c: List[Candle]) -> int:
        if len(c) < 2:
            return 5
        d = int((c[1].ts - c[0].ts).total_seconds() // 60)
        return max(1, d)


# =====================================================================
# 5. PRICE LOCATION ENGINE  (Sec 5)
# =====================================================================

@dataclass
class LocationResult:
    state: str
    distance_to_res: float
    distance_to_sup: float
    signals: List[Signal]


class LocationEngine:
    def run(self, spot: float, lv: Levels) -> LocationResult:
        d_res = lv.resistance - spot
        d_sup = spot - lv.support
        rng = max(1.0, lv.resistance - lv.support)
        pos = (spot - lv.support) / rng   # 0 = support, 1 = resistance

        if spot > lv.pdh:
            state = "ABOVE PDH"
        elif spot < lv.pdl:
            state = "BELOW PDL"
        elif abs(d_res) <= CFG.level_proximity_pts:
            state = "NEAR RESISTANCE"
        elif abs(d_sup) <= CFG.level_proximity_pts:
            state = "NEAR SUPPORT"
        elif abs(pos - 0.5) <= CFG.middle_zone_pct / 2:
            state = "MIDDLE OF RANGE"
        else:
            state = "TRENDING ZONE"

        bad = state == "MIDDLE OF RANGE"
        sig = [Signal("Price Location", "FAIL" if bad else "PASS",
                      f"{state} (R:{d_res:+.0f} / S:{d_sup:+.0f})")]
        return LocationResult(state, d_res, d_sup, sig)


# =====================================================================
# 6-7-8. OPTION CHAIN + OI + dOI ENGINE  (Sec 6,7,8,9)
# =====================================================================

def interpret_oi(price_up: bool, oi_up: bool) -> str:
    if price_up and oi_up:      return "LONG BUILDUP"
    if not price_up and oi_up:  return "SHORT BUILDUP"
    if price_up and not oi_up:  return "SHORT COVERING"
    return "LONG UNWINDING"


@dataclass
class OIResult:
    call_wall: float
    put_support: float
    top_ce: List[Tuple[float, int]]
    top_pe: List[Tuple[float, int]]
    pcr: float
    ce_action: str
    pe_action: str
    bias: str            # BULLISH / BEARISH / NEUTRAL
    confirmation: bool
    # Evidence (ATM strike ke actual numbers - #4/#5 feedback)
    atm_strike: float
    ce_oi_l: float          # lakhs
    pe_oi_l: float
    ce_doi_l: float         # ΔOI lakhs (signed)
    pe_doi_l: float
    ce_price_pct: float
    pe_price_pct: float
    ce_oi_pct: float
    pe_oi_pct: float
    signals: List[Signal]


def fmt_oi_pct(p: float) -> str:
    """Strike ATM bante waqt pichle din ka OI bahut chhota ho sakta hai -> % bhram deta hai."""
    if abs(p) <= 300:
        return f"{p:+.1f}%"
    return ">+300%" if p > 0 else "<-300%"


class OptionChainEngine:
    def run(self, m: MarketInput, trend: Optional[str] = None) -> OIResult:
        ch = m.chain
        atm = ch.atm()
        near = [r for r in ch.rows if abs(r.strike - atm) <= 6 * ch.step()]
        if not near:
            near = ch.rows

        top_ce = sorted(near, key=lambda r: r.ce_oi, reverse=True)[:3]
        top_pe = sorted(near, key=lambda r: r.pe_oi, reverse=True)[:3]
        call_wall = top_ce[0].strike if top_ce else 0.0
        put_support = top_pe[0].strike if top_pe else 0.0

        tot_ce = sum(r.ce_oi for r in near) or 1
        tot_pe = sum(r.pe_oi for r in near)
        pcr = tot_pe / tot_ce

        # ATM band ka dOI behaviour
        band = [r for r in near if abs(r.strike - atm) <= 3 * ch.step()]
        ce_doi = sum(r.ce_doi for r in band)
        pe_doi = sum(r.pe_doi for r in band)
        ce_prem_up = sum(r.ce_ltp - r.ce_prev_ltp for r in band) > 0
        pe_prem_up = sum(r.pe_ltp - r.pe_prev_ltp for r in band) > 0

        ce_action = interpret_oi(ce_prem_up, ce_doi > 0)
        pe_action = interpret_oi(pe_prem_up, pe_doi > 0)

        # Bias: Put writing + Call unwinding = bullish
        bull_pts = (pe_action == "SHORT BUILDUP") + (ce_action in ("SHORT COVERING", "LONG UNWINDING")) + (pcr > 1.0)
        bear_pts = (ce_action == "SHORT BUILDUP") + (pe_action in ("SHORT COVERING", "LONG UNWINDING")) + (pcr < 0.8)

        if bull_pts > bear_pts:
            bias = "BULLISH"
        elif bear_pts > bull_pts:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"

        confirmation = trend is not None and bias == trend

        # --- Evidence numbers (ATM strike ke actual %, doc ka #4/#5) ---
        atm_row = ch.row(atm)
        ce_oi_l = pe_oi_l = ce_doi_l = pe_doi_l = 0.0
        ce_price_pct = pe_price_pct = ce_oi_pct = pe_oi_pct = 0.0
        if atm_row:
            ce_oi_l, pe_oi_l = atm_row.ce_oi / 1e5, atm_row.pe_oi / 1e5
            ce_doi_l, pe_doi_l = atm_row.ce_doi / 1e5, atm_row.pe_doi / 1e5
            if atm_row.ce_prev_ltp:
                ce_price_pct = (atm_row.ce_ltp - atm_row.ce_prev_ltp) / atm_row.ce_prev_ltp * 100
            if atm_row.pe_prev_ltp:
                pe_price_pct = (atm_row.pe_ltp - atm_row.pe_prev_ltp) / atm_row.pe_prev_ltp * 100
            ce_base = atm_row.ce_oi - atm_row.ce_doi
            pe_base = atm_row.pe_oi - atm_row.pe_doi
            if ce_base > 0:
                ce_oi_pct = atm_row.ce_doi / ce_base * 100
            if pe_base > 0:
                pe_oi_pct = atm_row.pe_doi / pe_base * 100

        sig = [
            Signal("Option OI", "PASS" if call_wall and put_support else "FAIL",
                   f"Call Wall {call_wall:.0f} | Put Support {put_support:.0f} | PCR {pcr:.2f}"),
            Signal("Change in OI", "PASS" if confirmation else ("WARN" if bias == "NEUTRAL" else "FAIL"),
                   f"CE: {ce_action} (px {ce_price_pct:+.1f}% / OI {fmt_oi_pct(ce_oi_pct)}) | "
                   f"PE: {pe_action} (px {pe_price_pct:+.1f}% / OI {fmt_oi_pct(pe_oi_pct)}) -> bias {bias}"),
        ]
        return OIResult(call_wall, put_support,
                        [(r.strike, r.ce_oi) for r in top_ce],
                        [(r.strike, r.pe_oi) for r in top_pe],
                        pcr, ce_action, pe_action, bias, confirmation,
                        atm, ce_oi_l, pe_oi_l, ce_doi_l, pe_doi_l,
                        ce_price_pct, pe_price_pct, ce_oi_pct, pe_oi_pct, sig)


# =====================================================================
# 9. STRIKE SELECTION + LIQUIDITY + PREMIUM VALIDATION  (Sec 10,11)
# =====================================================================

@dataclass
class StrikeResult:
    ok: bool
    side: str            # CE / PE
    strike: float
    ltp: float
    bid: float
    ask: float
    spread_pct: float
    oi: int
    volume: int
    iv: float
    premium_move_pct: float
    premium_change_abs: float   # ₹ change (prev_ltp -> ltp) - #7 feedback
    delta: float
    gamma: float
    theta: float
    vega: float
    reasons: List[str]
    signals: List[Signal]


class StrikeSelectionEngine:
    def run(self, m: MarketInput, side: str) -> StrikeResult:
        ch = m.chain
        atm = ch.atm()
        # ATM se lekar 2 strike ITM/OTM tak candidates (delta proxy)
        cands = [r for r in ch.rows if abs(r.strike - atm) <= 3 * ch.step()]
        cands.sort(key=lambda r: abs(r.strike - atm))

        rejected: List[str] = []
        for r in cands:
            ltp, bid, ask, oi, vol, iv, prev, dlt, gma, the, veg = (
                (r.ce_ltp, r.ce_bid, r.ce_ask, r.ce_oi, r.ce_volume, r.ce_iv, r.ce_prev_ltp,
                 r.ce_delta, r.ce_gamma, r.ce_theta, r.ce_vega)
                if side == "CE" else
                (r.pe_ltp, r.pe_bid, r.pe_ask, r.pe_oi, r.pe_volume, r.pe_iv, r.pe_prev_ltp,
                 r.pe_delta, r.pe_gamma, r.pe_theta, r.pe_vega)
            )
            mid = (bid + ask) / 2 if (bid and ask) else ltp
            spread = ((ask - bid) / mid * 100) if mid else 999
            move = ((ltp - prev) / prev * 100) if prev else 0.0

            if oi < CFG.min_oi:
                rejected.append(f"{r.strike:.0f}{side}: OI low"); continue
            if vol < CFG.min_volume:
                rejected.append(f"{r.strike:.0f}{side}: volume low"); continue
            if spread > CFG.max_spread_pct:
                rejected.append(f"{r.strike:.0f}{side}: spread {spread:.1f}%"); continue
            if not (CFG.min_premium <= ltp <= CFG.max_premium):
                rejected.append(f"{r.strike:.0f}{side}: premium {ltp:.0f} out of band"); continue
            if dlt and abs(dlt) < CFG.min_delta_proxy:
                rejected.append(f"{r.strike:.0f}{side}: delta {abs(dlt):.2f} bahut kam"); continue
            # Premium validation: KAL ke close se nahi (gap-down din par har CE "sust" dikhta tha),
            # balki pichle ~10 min ke intraday premium se. History na ho to check skip.
            recent = r.ce_recent_ltp if side == "CE" else r.pe_recent_ltp
            if recent:
                rmove = (ltp - recent) / recent * 100
                if rmove < CFG.premium_recent_min_pct:
                    rejected.append(f"{r.strike:.0f}{side}: premium gir raha ({rmove:+.1f}% last ~10m)"); continue

            sig = [
                Signal("Volume/Liquidity", "PASS",
                       f"{r.strike:.0f}{side} OI {oi/1e5:.1f}L Vol {vol/1e3:.0f}K Spread {spread:.2f}%"),
            ]
            return StrikeResult(True, side, r.strike, ltp, bid, ask, spread,
                                oi, vol, iv, move, ltp - prev, abs(dlt), gma, the, veg,
                                rejected, sig)

        return StrikeResult(False, side, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            rejected,
                            [Signal("Volume/Liquidity", "FAIL",
                                    "Koi tradeable strike nahi mila: " + ("; ".join(rejected[:3]) or "chain empty"))])


# =====================================================================
# 10. IV ENGINE  (Sec 12)
# =====================================================================

@dataclass
class IVResult:
    iv: float
    state: str
    warning: str
    signals: List[Signal]


class IVEngine:
    def run(self, iv: float, hist: List[float]) -> IVResult:
        lo, nh, hh = CFG.iv_low, CFG.iv_normal_hi, CFG.iv_high_hi
        if hist and len(hist) >= 10:
            s = sorted(hist)
            lo = s[int(len(s) * .25)]
            nh = s[int(len(s) * .60)]
            hh = s[int(len(s) * .85)]

        if iv < lo:       state, warn = "LOW IV", "Premium sasta - move chhota ho sakta hai"
        elif iv <= nh:    state, warn = "NORMAL IV", ""
        elif iv <= hh:    state, warn = "HIGH IV", "⚠ PREMIUM MEHNGA - theta/IV crush risk"
        else:             state, warn = "EXTREME IV", "⚠⚠ EXTREME IV - buying avoid"

        st = "FAIL" if state == "EXTREME IV" else ("WARN" if state == "HIGH IV" else "PASS")
        return IVResult(iv, state, warn, [Signal("IV", st, f"{iv:.1f}% - {state}")])


# =====================================================================
# 11. ENTRY TRIGGER ENGINE  (Sec 13 + 14 false breakout)
# =====================================================================

@dataclass
class EntryResult:
    triggered: bool
    stage: str          # NONE / BREAKOUT / RETEST_WAIT / CONFIRMED / FALSE_BREAKOUT
    level: float
    entry_price: float
    invalidation: float
    detail: str
    signals: List[Signal]
    volume_ok: Optional[bool] = None   # breakout/pullback volume confirm hua? None = abhi koi breakout nahi


class EntryTriggerEngine:
    def run(self, m: MarketInput, side: str, lv: Levels) -> EntryResult:
        self._vol = None
        res = self._run(m, side, lv)
        res.volume_ok = self._vol
        return res

    def _run(self, m: MarketInput, side: str, lv: Levels) -> EntryResult:
        c = m.candles
        avg_vol = sum(x.volume for x in c[-20:]) / min(20, len(c))
        buf = CFG.breakout_buffer_pts

        # Kaunsa level break hua - saare key levels scan karo, sabse recent lo
        cands = lv.key_levels or [lv.resistance if side == "CE" else lv.support]
        idx, level = None, (lv.resistance if side == "CE" else lv.support)
        window = max(0, len(c) - CFG.breakout_scan_bars - 2)
        for i in range(len(c) - 1, window, -1):
            for L in cands:
                crossed = ((side == "CE" and c[i].close > L + buf and c[i - 1].close <= L) or
                           (side == "PE" and c[i].close < L - buf and c[i - 1].close >= L))
                strong = self._strong_candle(c[i])
                if crossed and strong and self._level_is_established(c, L, i, side):
                    idx, level = i, L; break
            if idx is not None:
                break

        if idx is None:
            level = lv.resistance if side == "CE" else lv.support
            # --- Continuation path: established trend me pullback -> hold ---
            cont = self._continuation(c, side, lv, avg_vol)
            if cont:
                return cont
            return self._out(False, "NONE", level, 0, 0,
                             f"{'Breakout' if side=='CE' else 'Breakdown'} abhi tak nahi ({level:.0f})")

        bo = c[idx]
        # Volume confirmation poore breakout leg par (impulse bar pehli bar
        # ke baad bhi aa sakti hai) - sirf ek candle dekhna bahut sakht hai.
        leg = c[idx:idx + 3]
        leg_vol = max(x.volume for x in leg)
        vol_ok = leg_vol >= avg_vol * CFG.volume_surge_mult
        self._vol = vol_ok
        after = c[idx + 1:]

        # 2) False breakout filter
        if after:
            if side == "CE" and after[0].close < level and not vol_ok:
                return self._out(False, "FALSE_BREAKOUT", level, 0, 0,
                                 "Wapas level ke niche + weak volume = FALSE BREAKOUT")
            if side == "PE" and after[0].close > level and not vol_ok:
                return self._out(False, "FALSE_BREAKOUT", level, 0, 0,
                                 "Wapas level ke upar + weak volume = FALSE BREAKDOWN")

        if not vol_ok:
            return self._out(False, "BREAKOUT", level, 0, 0,
                             f"Breakout hua par volume weak ({leg_vol:.0f} vs {avg_vol*CFG.volume_surge_mult:.0f} chahiye)")

        # 3) Retest hold - EXCEPT agar volume itna strong ho ki fast-track ho jaaye
        fast_track = leg_vol >= avg_vol * CFG.fast_track_volume_mult
        tol = CFG.retest_tolerance_pts
        retest_done = fast_track
        if not fast_track:
            for x in after:
                if side == "CE":
                    if x.low <= level + tol:
                        retest_done = x.close > level
                        if not retest_done:
                            return self._out(False, "FALSE_BREAKOUT", level, 0, 0,
                                             "Retest fail - close level ke niche")
                        break
                else:
                    if x.high >= level - tol:
                        retest_done = x.close < level
                        if not retest_done:
                            return self._out(False, "FALSE_BREAKOUT", level, 0, 0,
                                             "Retest fail - close level ke upar")
                        break

            if not retest_done:
                if len(after) >= CFG.max_bars_for_retest:
                    # bina retest ke bhi trend chala gaya -> momentum entry allow
                    retest_done = True
                else:
                    return self._out(False, "RETEST_WAIT", level, 0, 0,
                                     "Breakout confirmed, retest ka intezaar")

        # 4) VWAP hold - retest ke baad bhi price sahi side of VWAP pe hona chahiye
        if CFG.require_vwap_hold:
            vw = vwap(c)
            if side == "CE" and c[-1].close < vw:
                return self._out(False, "RETEST_WAIT", level, 0, 0,
                                 f"Retest hua par price VWAP ({vw:.0f}) ke niche - hold weak")
            if side == "PE" and c[-1].close > vw:
                return self._out(False, "RETEST_WAIT", level, 0, 0,
                                 f"Retest hua par price VWAP ({vw:.0f}) ke upar - hold weak")

        # 5) Breakout confirmed tabhi tak jab tak price abhi bhi level ke sahi side par hai
        live = m.chain.spot if m.chain.spot else c[-1].close
        if side == "CE" and (live <= level or c[-1].close <= level):
            return self._out(False, "FALSE_BREAKOUT", level, 0, 0,
                             f"Breakout ke baad price wapas level {level:.0f} ke niche ({live:.0f})")
        if side == "PE" and (live >= level or c[-1].close >= level):
            return self._out(False, "FALSE_BREAKOUT", level, 0, 0,
                             f"Breakdown ke baad price wapas level {level:.0f} ke upar ({live:.0f})")

        entry = c[-1].close
        swing = (min(x.low for x in c[idx:]) if side == "CE" else max(x.high for x in c[idx:]))
        inval = min(level, swing) if side == "CE" else max(level, swing)
        return self._out(True, "CONFIRMED", level, entry, inval,
                         f"Level {level:.0f} break + {'STRONG volume (fast-track)' if fast_track else 'volume + retest hold'} + VWAP OK")

    @staticmethod
    def _strong_candle(c: Candle) -> bool:
        """Weak/doji breakout candles reject - conviction chahiye (#entry accuracy)."""
        rng = c.high - c.low
        if rng <= 0:
            return False
        body = abs(c.close - c.open)
        return (body / rng) >= CFG.min_body_ratio

    @staticmethod
    def _level_is_established(c: List[Candle], L: float, brk_i: int, side: str) -> bool:
        """
        Level tabhi valid jab wo breakout se KAAFI pehle bana ho aur test hua ho.
        Warna breakout candle ka apna high/low hi 'level' ban jaata hai.
        """
        if brk_i < 5:
            return False
        hist = c[:brk_i - 2]                    # breakout ke pehle ka data
        tol = CFG.retest_tolerance_pts
        if side == "CE":
            # pehle yahan reject hua tha: koi bar high se chhua par close niche
            return any(x.high >= L - tol and x.close <= L for x in hist)
        return any(x.low <= L + tol and x.close >= L for x in hist)

    def _continuation(self, c: List[Candle], side: str, lv: Levels, avg_vol: float):
        """
        Trend already chal raha hai: price ek support (CE) / resistance (PE)
        par pullback kare, wahan se reject ho, volume aaye -> continuation entry.
        Reference levels: nearest key level + VWAP.
        """
        ref = lv.support if side == "CE" else lv.resistance
        refs = [ref, lv.vwap]
        last3 = c[-3:]
        if len(last3) < 3:
            return None
        tol = CFG.retest_tolerance_pts

        for L in refs:
            if not L:
                continue
            if side == "CE":
                touched = any(x.low <= L + tol for x in last3)
                held = c[-1].close > L and c[-1].close > c[-1].open
            else:
                touched = any(x.high >= L - tol for x in last3)
                held = c[-1].close < L and c[-1].close < c[-1].open
            if not (touched and held):
                continue
            self._vol = c[-1].volume >= avg_vol * 1.0
            if c[-1].volume < avg_vol * 1.0:
                return self._out(False, "RETEST_WAIT", L, 0, 0,
                                 f"Pullback {L:.0f} par hold, par volume weak")
            swing = min(x.low for x in last3) if side == "CE" else max(x.high for x in last3)
            inval = swing - CFG.breakout_buffer_pts if side == "CE" else swing + CFG.breakout_buffer_pts
            return self._out(True, "CONTINUATION", L, c[-1].close, inval,
                             f"Trend continuation - {L:.0f} pullback hold + volume")
        return None

    @staticmethod
    def _out(t, stage, level, entry, inval, detail):
        st = "PASS" if t else ("FAIL" if stage == "FALSE_BREAKOUT" else "WARN")
        return EntryResult(t, stage, level, entry, inval, detail,
                           [Signal("Entry Trigger", st, detail)])


# =====================================================================
# 12. RISK ENGINE  (Sec 17,18,19)
# =====================================================================

@dataclass
class RiskResult:
    ok: bool
    entry_premium: float
    premium_sl: float
    underlying_sl: float
    risk_per_unit: float
    lots: int
    total_risk: float
    targets: List[float]
    underlying_targets: List[float]
    detail: str
    signals: List[Signal]
    over_limit: bool = False      # 1 lot ka risk max_risk se upar (sirf warning, block nahi)
    per_lot_risk: float = 0.0
    max_risk: float = 0.0


class RiskEngine:
    def run(self, sr: StrikeResult, er: EntryResult, lv: Levels, spot: float,
            max_risk: Optional[float] = None) -> RiskResult:
        max_risk = max_risk if max_risk is not None else CFG.max_risk_rupees
        if not sr.ok or not er.triggered:
            return self._fail("Entry ya strike ready nahi - SL define nahi ho sakta")

        entry = sr.ltp
        # underlying invalidation -> premium SL (delta ~0.5 ATM proxy)
        pts_risk = abs(spot - er.invalidation)
        # Real delta (broker se) use karo; na mile to ATM proxy 0.5
        delta = sr.delta if sr.delta else 0.5
        prem_sl_from_underlying = max(1.0, entry - pts_risk * delta)
        prem_sl_fixed = entry * (1 - CFG.premium_sl_pct / 100)
        premium_sl = max(prem_sl_from_underlying, prem_sl_fixed)

        risk_unit = entry - premium_sl
        if risk_unit <= 0:
            return self._fail("Risk per unit invalid")

        per_lot = risk_unit * CFG.lot_size
        if per_lot <= 0:
            return self._fail("Lot size invalid - risk calculate nahi ho sakta")
        lots = int(max_risk // per_lot)
        over_limit = lots < 1
        if over_limit:
            lots = 1          # signal block nahi hota; dashboard par laal warning aati hai
        total = lots * per_lot

        targets = [round(entry + risk_unit * r, 1) for r in CFG.rr_targets]
        # underlying-based target validation
        rng = max(CFG.level_proximity_pts * 0.8, lv.resistance - lv.support)
        pool = list(lv.key_levels) + [lv.pdh, lv.pdl, lv.day_high, lv.day_low]
        if sr.side == "CE":
            u_tg = sorted({round(x) for x in pool if x > spot + CFG.breakout_buffer_pts * 2})[:3]
            while len(u_tg) < 3:
                u_tg.append(round((u_tg[-1] if u_tg else spot) + rng))
        else:
            u_tg = sorted({round(x) for x in pool if x < spot - CFG.breakout_buffer_pts * 2}, reverse=True)[:3]
            while len(u_tg) < 3:
                u_tg.append(round((u_tg[-1] if u_tg else spot) - rng))

        return RiskResult(True, entry, round(premium_sl, 1), er.invalidation,
                          round(risk_unit, 1), lots, round(total, 0), targets,
                          [round(x, 0) for x in u_tg],
                          f"{lots} lot x {CFG.lot_size} | risk ₹{total:,.0f}",
                          [Signal("Risk / SL", "WARN" if over_limit else "PASS",
                                  (f"SL ₹{premium_sl:.1f} | ⚠ 1 lot risk ₹{per_lot:,.0f} > limit ₹{max_risk:,.0f} - size tum decide karo"
                                   if over_limit else
                                   f"SL ₹{premium_sl:.1f} | {lots} lot | ₹{total:,.0f} risk"))],
                          over_limit, round(per_lot, 0), max_risk)

    @staticmethod
    def _fail(msg):
        return RiskResult(False, 0, 0, 0, 0, 0, 0, [], [], msg,
                          [Signal("Risk / SL", "FAIL", msg)])


# =====================================================================
# 13. CONFLUENCE SCORE  (Sec 15)
# =====================================================================

SCORE_KEYS = ["Trend", "Market Structure", "VWAP", "Important Level", "Price Location",
              "Option OI", "Change in OI", "Volume/Liquidity", "IV", "Entry Trigger"]


def confluence(signals: List[Signal]) -> Tuple[int, Dict[str, int]]:
    d = {k: 0 for k in SCORE_KEYS}
    for s in signals:
        if s.label in d and s.state == "PASS":
            d[s.label] = 1
    return sum(d.values()), d


def score_bucket(score: int) -> str:
    if score <= CFG.score_no_trade: return "NO TRADE"
    if score <= CFG.score_wait:     return "WAIT"
    if score == CFG.score_watch:    return "WATCH"
    if score >= 9:                  return "HIGH CONFLUENCE"
    return "SETUP READY"


# =====================================================================
# 14. FINAL DECISION  (Sec 16 gates + Sec 20)
# =====================================================================

@dataclass
class Position:
    """Entry ke baad ka live tracking - engine khud order nahi karta, bas
    monitor karta hai ki tumne jo trade liya (manually) uska SL/target kya
    status hai. Position start hoti hai jab status READY dikhe."""
    side: str
    strike: float
    entry_premium: float
    sl: float
    targets: List[float]
    targets_hit: List[bool]
    lots: int
    opened_at: datetime
    status: str          # OPEN / SL_HIT / FINAL_TARGET_HIT
    current_premium: float
    pnl: float
    lot_size: int = 0    # position khulte waqt ka lot size (0 = CFG.lot_size)


def update_position(pos: Position, current_ltp: float) -> Position:
    """Naya Position return karta hai (immutable style) - current premium ke
    against SL/targets check karke."""
    targets_hit = list(pos.targets_hit)
    status = pos.status
    if current_ltp <= pos.sl:
        status = "SL_HIT"
    elif pos.targets and current_ltp >= pos.targets[-1]:
        status = "FINAL_TARGET_HIT"
        targets_hit = [True] * len(pos.targets)
    else:
        for i, t in enumerate(pos.targets):
            if current_ltp >= t:
                targets_hit[i] = True
    lot = pos.lot_size or CFG.lot_size
    pnl = (current_ltp - pos.entry_premium) * lot * pos.lots
    return Position(pos.side, pos.strike, pos.entry_premium, pos.sl, pos.targets,
                    targets_hit, pos.lots, pos.opened_at, status, current_ltp, round(pnl, 0), lot)


@dataclass
class Projection:
    """
    Breakout confirm hone SE PEHLE ka indicative plan - calculated (ATR + level
    based), hardcoded nahi. Clearly 'PROJECTED' label ke saath dikhana hai,
    kyunki yeh actual filled entry nahi hai. (#2 feedback)
    """
    trigger_level: float
    sl_level: float
    targets_underlying: List[float]
    ref_premium: float
    premium_sl: float
    premium_targets: List[float]
    rr: float


@dataclass
class Decision:
    status: str          # CE SETUP READY / PE SETUP READY / WATCH / WAIT / NO TRADE
    side: Optional[str]
    reason: str
    score: int
    breakdown: Dict[str, int]
    signals: List[Signal]
    trend: TrendResult
    levels: Levels
    location: LocationResult
    oi: OIResult
    strike: StrikeResult
    iv: IVResult
    entry: EntryResult
    risk: RiskResult
    spot: float
    projection: Optional[Projection] = None


class DecisionEngine:
    def __init__(self, cfg: Config = CFG):
        self.cfg = cfg

    def run(self, m: MarketInput, max_risk: Optional[float] = None) -> Decision:
        # Live tick (option chain se) use karo, na ki last-closed-candle ka
        # stale close - candle close 5 min tak purana ho sakta hai.
        spot = m.chain.spot if m.chain.spot else m.candles[-1].close
        sigs: List[Signal] = []

        # --- Layer 1: Market Intelligence ---
        tr = PriceActionEngine().run(m);                 sigs += tr.signals
        oi = OptionChainEngine().run(m, tr.trend);       sigs += oi.signals
        lv = LevelsEngine().run(m, tr, oi);              sigs += lv.signals
        loc = LocationEngine().run(spot, lv);            sigs += loc.signals

        side = "CE" if tr.trend == "BULLISH" else ("PE" if tr.trend == "BEARISH" else None)

        # placeholders
        sr = StrikeResult(False, side or "CE", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, [], [])
        ivr = IVEngine().run(self._atm_iv(m, side or "CE"), m.iv_history)
        er = EntryResult(False, "NONE", 0, 0, 0, "Trend clear nahi", [])
        rr = RiskEngine._fail("Pending")

        if side:
            sr = StrikeSelectionEngine().run(m, side);   sigs += sr.signals
            ivr = IVEngine().run(sr.iv if sr.ok else self._atm_iv(m, side), m.iv_history)
            sigs += ivr.signals
            er = EntryTriggerEngine().run(m, side, lv);  sigs += er.signals
            rr = RiskEngine().run(sr, er, lv, spot, max_risk)
            sigs += rr.signals
        else:
            sigs += [Signal("Volume/Liquidity", "INFO", "Direction clear nahi - strike skip"),
                     Signal("IV", "INFO", f"{ivr.iv:.1f}% - {ivr.state} (ATM)"),
                     Signal("Entry Trigger", "FAIL", "Trend RANGE hai"),
                     Signal("Risk / SL", "FAIL", "Entry nahi")]

        score, brk = confluence(sigs)

        # ---- Sequential gate cascade (Sec 20) ----
        status, reason = self._gates(tr, loc, lv, sr, ivr, er, rr, side, score, m.now)

        proj = self._project(m, side, lv, sr, er) if (side and not er.triggered) else None

        return Decision(status, side if status.endswith("READY") else None, reason,
                        score, brk, sigs, tr, lv, loc, oi, sr, ivr, er, rr, spot, proj)

    @staticmethod
    def _project(m: MarketInput, side: str, lv: Levels, sr: StrikeResult, er: Optional["EntryResult"] = None) -> Projection:
        """Indicative plan agar/jab breakout confirm ho - ATR + level se calculate,
        koi hardcoded number nahi. (#2 feedback)"""
        a = atr(m.candles)
        # Header wala level hi use karo: jo level engine ne detect kiya (er.level), warna nearest S/R
        trigger = (er.level if (er and er.level) else (lv.resistance if side == "CE" else lv.support))
        sl = trigger - a * 1.5 if side == "CE" else trigger + a * 1.5
        risk_pts = abs(trigger - sl)
        rr0 = CFG.rr_targets[0]
        targets_u = [round(trigger + risk_pts * r, 0) if side == "CE" else round(trigger - risk_pts * r, 0)
                     for r in CFG.rr_targets]

        atm_row = m.chain.row(m.chain.atm())
        ref_ltp = (sr.ltp if sr.ok else
                   (atm_row.ce_ltp if side == "CE" else atm_row.pe_ltp) if atm_row else 0.0)
        dlt = sr.delta if (sr.ok and sr.delta) else 0.5
        premium_sl = max(1.0, ref_ltp - risk_pts * dlt)
        premium_targets = [round(ref_ltp + abs(t - trigger) * dlt, 1) for t in targets_u]

        return Projection(trigger, round(sl, 0), targets_u, round(ref_ltp, 1),
                          round(premium_sl, 1), premium_targets, rr0)

    def _gates(self, tr, loc, lv, sr, ivr, er, rr, side, score, now):
        t = now.time()
        if t < CFG.no_trade_before:
            return "WAIT", f"Opening volatility window - {CFG.no_trade_before.strftime('%H:%M')} tak trade skip"
        if t > CFG.no_trade_after:
            return "NO TRADE", f"Closing volatility window - {CFG.no_trade_after.strftime('%H:%M')} ke baad naya trade nahi"
        if tr.trend == "RANGE":
            return "NO TRADE", "Market RANGE / structure mixed - directional buying band"
        if tr.strength == "WEAK":
            return "WAIT", f"Trend weak (ADX {tr.adx:.1f}) - momentum ka intezaar"
        if loc.state == "MIDDLE OF RANGE":
            return "WAIT", "Price range ke beech - koi edge nahi"
        if not lv.support or not lv.resistance:
            return "WAIT", "Major level identify nahi hua"
        if side == "CE" and loc.state == "NEAR RESISTANCE" and not er.triggered:
            return "WAIT", "Resistance ke paas - breakout ke bina CE chase nahi"
        if side == "PE" and loc.state == "NEAR SUPPORT" and not er.triggered:
            return "WAIT", "Support ke paas - breakdown ke bina PE chase nahi"
        if not sr.ok:
            return "NO TRADE", "Koi tradeable strike nahi: " + ("; ".join(sr.reasons[:2]) or "no strike")
        if ivr.state == "EXTREME IV":
            return "NO TRADE", ivr.warning
        if er.stage == "FALSE_BREAKOUT":
            return "WAIT", er.detail
        if not er.triggered:
            return "WAIT", er.detail
        if not rr.ok:
            return "NO TRADE", rr.detail
        if score < CFG.score_ready:
            return "WATCH", f"Confluence sirf {score}/10 - mandatory pass par score kam"
        warn = f" (⚠ {ivr.warning})" if ivr.warning else ""
        return f"{side} SETUP READY", f"Sab conditions confirm - {er.detail}{warn}"

    @staticmethod
    def _atm_iv(m: MarketInput, side: str) -> float:
        r = m.chain.row(m.chain.atm())
        if not r:
            return 0.0
        return r.ce_iv if side == "CE" else r.pe_iv


# =====================================================================
# 15. DASHBOARD  (Sec 21)
# =====================================================================

STATUS_ICON = {"NO TRADE": "🔴", "WAIT": "🟡", "WATCH": "🟠"}


def render(d: Decision) -> str:
    icon = STATUS_ICON.get(d.status, "🟢")
    W = 58
    L = []
    def row(k, v=""):
        L.append(f"║ {k:<18}{str(v):<{W-21}}║")
    def sep():
        L.append("╠" + "═" * (W - 2) + "╣")

    L.append("╔" + "═" * (W - 2) + "╗")
    L.append("║" + "NIFTY OPTION DECISION ENGINE".center(W - 2) + "║")
    sep()
    row("NIFTY", f"{d.spot:,.2f}")
    row("TREND", f"{'🟢' if d.trend.trend=='BULLISH' else '🔴' if d.trend.trend=='BEARISH' else '⚪'} {d.trend.label}")
    row("ADX", f"{d.trend.adx:.1f}")
    row("VWAP", f"{d.levels.vwap:,.0f}")
    row("PDH / PDL", f"{d.levels.pdh:,.0f} / {d.levels.pdl:,.0f}")
    row("OPENING RANGE", f"{d.levels.or_low:,.0f} - {d.levels.or_high:,.0f}")
    sep()
    row("SUPPORT", f"{d.levels.support:,.0f}")
    row("RESISTANCE", f"{d.levels.resistance:,.0f}")
    row("PRICE LOCATION", d.location.state)
    sep()
    row("CALL WALL", f"{d.oi.call_wall:,.0f}")
    row("PUT SUPPORT", f"{d.oi.put_support:,.0f}")
    row("PCR", f"{d.oi.pcr:.2f}")
    row("CE / PE dOI", f"{d.oi.ce_action} / {d.oi.pe_action}")
    row("OI BIAS", f"{d.oi.bias}  {'✓' if d.oi.confirmation else '✗'}")
    sep()
    if d.strike.ok:
        row("SELECTED STRIKE", f"{d.strike.strike:,.0f} {d.strike.side}  @ ₹{d.strike.ltp:.1f}")
        row("OI / VOLUME", f"{d.strike.oi/1e5:.1f}L / {d.strike.volume/1e3:.0f}K")
        row("SPREAD", f"{d.strike.spread_pct:.2f}%")
        row("PREMIUM MOVE", f"{d.strike.premium_move_pct:+.1f}%")
    else:
        row("SELECTED STRIKE", "—")
    row("IV", f"{d.iv.iv:.1f}%  {d.iv.state}")
    sep()
    row("ENTRY STAGE", d.entry.stage)
    if d.risk.ok:
        row("ENTRY", f"₹{d.risk.entry_premium:.1f}")
        row("SL (premium)", f"₹{d.risk.premium_sl:.1f}")
        row("SL (underlying)", f"{d.risk.underlying_sl:,.0f}")
        row("TARGETS", " / ".join(f"₹{t:.0f}" for t in d.risk.targets))
        row("UNDERLYING TGT", " / ".join(f"{t:,.0f}" for t in d.risk.underlying_targets))
        row("POSITION", f"{d.risk.lots} lot  |  RISK ₹{d.risk.total_risk:,.0f}"
                                + ("  ⚠ LIMIT SE UPAR" if d.risk.over_limit else ""))
    else:
        row("RISK", "—")
    sep()
    row("CONFLUENCE", f"{d.score} / 10   ({score_bucket(d.score)})")
    L.append("║" + " " * (W - 2) + "║")
    row("STATUS", f"{icon} {d.status}")
    for i, line in enumerate(_wrap(d.reason, W - 21)):
        row("REASON" if i == 0 else "", line)
    L.append("╚" + "═" * (W - 2) + "╝")

    # Reason panel
    L.append("")
    L.append("REASON PANEL")
    L.append("-" * W)
    for s in d.signals:
        L.append(f" {s.icon} {s.label:<20} {s.detail}")
    return "\n".join(L)


def _wrap(text: str, width: int) -> List[str]:
    words, out, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > width:
            out.append(cur); cur = w
        else:
            cur = f"{cur} {w}".strip()
    if cur:
        out.append(cur)
    return out or [""]


# =====================================================================
# 16. HTML DASHBOARD (browser me dekhne ke liye - cmd se accha)
# =====================================================================

_STATUS_COLOR = {
    "NO TRADE": "#ef4444", "WAIT": "#eab308", "WATCH": "#f97316",
}


def status_color(d: Decision) -> str:
    return "#22c55e" if d.status.endswith("READY") else _STATUS_COLOR.get(d.status, "#94a3b8")


def _panel_html(d: Decision, position: "Position" = None, key: str = "nifty",
                notice: str = "") -> str:
    """Ek index ka poora panel (banner + cards + reason + plan + position)."""
    idx_name = CFG.index_name
    is_ready = d.status.endswith("READY")
    color = "#22c55e" if is_ready else _STATUS_COLOR.get(d.status, "#94a3b8")
    trend_color = "#22c55e" if d.trend.trend == "BULLISH" else "#ef4444" if d.trend.trend == "BEARISH" else "#94a3b8"
    entry_status = "READY" if is_ready else d.status
    side = d.strike.side if d.strike.ok else ("CE" if d.trend.trend == "BULLISH" else "PE" if d.trend.trend == "BEARISH" else None)

    def row(label, value, big=False):
        cls = "big" if big else ""
        return f'<div class="row"><span class="lbl">{label}</span><span class="val {cls}">{value}</span></div>'

    def sig_row(s: Signal):
        c = {"PASS": "#22c55e", "WARN": "#eab308", "FAIL": "#ef4444", "INFO": "#64748b"}[s.state]
        return (f'<div class="sig"><span class="dot" style="background:{c}"></span>'
                f'<span class="sig-label">{s.label}</span><span class="sig-detail">{s.detail}</span></div>')

    # ---- Trigger / Confirmation lines (#3) ----
    trigger_txt = confirm_txt = ""
    if side and d.entry.stage not in ("CONFIRMED", "CONTINUATION"):
        lvl = d.entry.level or (d.levels.resistance if side == "CE" else d.levels.support)
        arrow = "&gt;" if side == "CE" else "&lt;"
        trigger_txt = f"Trigger: {idx_name} {arrow} {lvl:,.0f}"
        confirm_txt = "Confirmation: candle close + volume surge + retest hold"

    # ---- Underlying % move since prev close (#7) ----
    u_chg_pts = d.spot - d.levels.pdc
    u_chg_pct = (u_chg_pts / d.levels.pdc * 100) if d.levels.pdc else 0.0

    # ---- Level map, vertical, spot marked (#9) ----
    lvl_items = {
        "PDH": d.levels.pdh, "PDL": d.levels.pdl, "VWAP": d.trend.vwap,
        "SUPPORT": d.levels.support, "RESISTANCE": d.levels.resistance,
        "CALL WALL": d.oi.call_wall, "PUT SUPPORT": d.oi.put_support,
        "SPOT": d.spot,
    }
    ordered = sorted(lvl_items.items(), key=lambda kv: kv[1], reverse=True)
    level_rows = ""
    for name, val in ordered:
        is_spot = name == "SPOT"
        style = f'style="color:{color};font-weight:800"' if is_spot else ""
        dot = "●" if is_spot else "─"
        level_rows += (f'<div class="lvlrow" {style}>'
                       f'<span class="lvl-price">{val:,.0f}</span>'
                       f'<span class="lvl-dot">{dot}</span>'
                       f'<span class="lvl-name">{name}</span></div>')

    risk_html = ""
    if d.risk.ok:
        if d.risk.over_limit:
            total_row = (f'<div class="row"><span class="lbl">Total Risk</span>'
                         f'<span class="val big" style="color:#ef4444">₹{d.risk.total_risk:,.0f}</span></div>'
                         f'<div class="warnline">⚠ 1 lot risk ₹{d.risk.per_lot_risk:,.0f} &gt; limit '
                         f'₹{d.risk.max_risk:,.0f} - size tum decide karo</div>')
        else:
            total_row = row("Total Risk", f"₹{d.risk.total_risk:,.0f}", big=True)
        risk_html = f"""
        <div class="card">
          <div class="card-title">ENTRY / SL / TARGETS <span class="tag-live">FILLED PLAN</span></div>
          {row("Entry", f"₹{d.risk.entry_premium:.1f}")}
          {row("SL (premium)", f"₹{d.risk.premium_sl:.1f}")}
          {row("SL (underlying)", f"{d.risk.underlying_sl:,.0f}")}
          {row("Targets", " / ".join(f"₹{t:.0f}" for t in d.risk.targets))}
          {row("Underlying Tgt", " / ".join(f"{t:,.0f}" for t in d.risk.underlying_targets))}
          {row("Position", f"{d.risk.lots} lot &times; {CFG.lot_size}")}
          {total_row}
        </div>"""
    elif d.projection:
        p = d.projection
        risk_html = f"""
        <div class="card">
          <div class="card-title">ENTRY / SL / TARGETS <span class="tag-proj">PROJECTED</span></div>
          {row("If triggers at", f"{p.trigger_level:,.0f}")}
          {row("SL (underlying)", f"{p.sl_level:,.0f}")}
          {row("Targets", " / ".join(f"{t:,.0f}" for t in p.targets_underlying))}
          {row("Ref premium (now)", f"₹{p.ref_premium:.1f}")}
          {row("Premium SL (proj)", f"₹{p.premium_sl:.1f}")}
          {row("Premium Tgt (proj)", " / ".join(f"₹{t:.0f}" for t in p.premium_targets))}
          {row("R:R", f"1 : {p.rr:.1f}")}
        </div>"""

    strike_html = ""
    if d.strike.ok:
        greeks = ""
        if d.strike.gamma or d.strike.theta or d.strike.vega:
            greeks = (f'{row("Gamma", f"{d.strike.gamma:.4f}")}'
                     f'{row("Theta", f"{d.strike.theta:.2f}")}'
                     f'{row("Vega", f"{d.strike.vega:.2f}")}')
        prev_ltp = d.strike.ltp - d.strike.premium_change_abs
        strike_html = f"""
        <div class="card">
          <div class="card-title">SELECTED STRIKE</div>
          {row("Strike", f"{d.strike.strike:,.0f} {d.strike.side}", big=True)}
          {row("LTP", f"₹{d.strike.ltp:.1f}")}
          {row("Delta", f"{d.strike.delta:.2f}")}
          {greeks}
          {row("OI / Volume", f"{d.strike.oi/1e5:.1f}L / {d.strike.volume/1e3:.0f}K")}
          {row("Spread", f"{d.strike.spread_pct:.2f}%")}
          {row("Premium Move", f"₹{prev_ltp:.0f}&rarr;₹{d.strike.ltp:.0f} ({d.strike.premium_move_pct:+.1f}%)")}
          {row("vs Underlying", f"{u_chg_pts:+.0f} pts ({u_chg_pct:+.2f}%)")}
        </div>"""

    oi_evidence = f"""
      {row("CE @ATM OI", f"{d.oi.ce_oi_l:.1f}L (Δ{d.oi.ce_doi_l:+.1f}L / {fmt_oi_pct(d.oi.ce_oi_pct)})")}
      {row("PE @ATM OI", f"{d.oi.pe_oi_l:.1f}L (Δ{d.oi.pe_doi_l:+.1f}L / {fmt_oi_pct(d.oi.pe_oi_pct)})")}
      {row("CE px move", f"{d.oi.ce_price_pct:+.1f}% &rarr; {d.oi.ce_action}")}
      {row("PE px move", f"{d.oi.pe_price_pct:+.1f}% &rarr; {d.oi.pe_action}")}
    """

    sig_rows = "".join(sig_row(s) for s in d.signals)

    # ---- Trade Plan card (checklist, real signals se - koi fake tick nahi) ----
    def find_sig(label):
        return next((s for s in d.signals if s.label == label), None)

    checklist_map = [
        ("Trend confirmed", find_sig("Trend")),
        ("Price location OK", find_sig("Price Location")),
        ("Level break/hold", find_sig("Entry Trigger")),
        ("Breakout volume", Signal("Breakout volume", "INFO" if d.entry.volume_ok is None else ("PASS" if d.entry.volume_ok else "FAIL"))),
        ("Option liquidity", find_sig("Volume/Liquidity")),
        ("OI confirmation", find_sig("Change in OI")),
    ]
    check_rows = ""
    for label, s in checklist_map:
        if s is None:
            continue
        mark = "✓" if s.state == "PASS" else ("○" if s.state in ("WARN", "INFO") else "✗")
        mcolor = "#22c55e" if s.state == "PASS" else ("#eab308" if s.state in ("WARN", "INFO") else "#ef4444")
        check_rows += (f'<div class="check-row"><span style="color:{mcolor}">{mark}</span> '
                       f'<span>{label}</span></div>')

    if is_ready or d.entry.stage in ("CONFIRMED", "CONTINUATION"):
        dir_word = "BULLISH BREAKOUT" if side == "CE" else "BEARISH BREAKDOWN"
        if is_ready:
            plan_head = f'<div class="plan-head" style="color:{color}">🟢 {dir_word}</div>'
        else:
            import html as _html
            plan_head = (f'<div class="plan-head" style="color:#eab308">⏸ {dir_word} - NOT TRADEABLE ({_html.escape(d.status)})</div>'
                         f'<div class="lbl" style="margin-bottom:10px">{_html.escape(d.reason)}</div>')
        plan_body = f"""
          {plan_head}
          {row("ENTRY", f"₹{d.risk.entry_premium:.1f}" if d.risk.ok else f"{d.entry.level:,.0f}")}
          {row("STOP LOSS", f"₹{d.risk.premium_sl:.1f}" if d.risk.ok else "Dynamic")}
          {row("TARGET 1", f"₹{d.risk.targets[0]:.0f}" if d.risk.ok and d.risk.targets else "Dynamic")}
          {row("TARGET 2", f"₹{d.risk.targets[1]:.0f}" if d.risk.ok and len(d.risk.targets) > 1 else "Dynamic")}
          <div class="check-title">CONFIRMATION</div>
          {check_rows}"""
    else:
        plan_body = f"""
          <div class="plan-head" style="color:#64748b">🔒 TRADE PLAN LOCKED</div>
          <div class="lbl" style="margin-bottom:10px">Waiting for confirmation...</div>
          <div class="check-title">CONFIRMATION</div>
          {check_rows}"""

    trade_plan_html = f'<div class="signals" style="margin-top:14px"><div class="card-title">TRADE PLAN</div>{plan_body}</div>'

    position_html = ""
    if position:
        pcolor = ("#ef4444" if position.status == "SL_HIT" else
                  "#22c55e" if position.status == "FINAL_TARGET_HIT" else
                  "#22c55e" if position.pnl >= 0 else "#ef4444")
        status_word = {"OPEN": "IN POSITION", "SL_HIT": "🔴 SL HIT - CLOSED",
                       "FINAL_TARGET_HIT": "🟢 FINAL TARGET HIT - CLOSED"}.get(position.status, position.status)
        tgt_rows = ""
        for i, (t, hit) in enumerate(zip(position.targets, position.targets_hit), 1):
            mark = "✓" if hit else "○"
            mc = "#22c55e" if hit else "#64748b"
            tgt_rows += f'<div class="check-row"><span style="color:{mc}">{mark}</span> <span>T{i}: ₹{t:.0f}</span></div>'
        position_html = f"""
        <div class="signals" style="margin-top:14px; border-color:{pcolor}66">
          <div class="card-title">LIVE POSITION <span style="color:{pcolor}">{status_word}</span></div>
          {row("Side / Strike", f"{position.side} {position.strike:,.0f}")}
          {row("Entry", f"₹{position.entry_premium:.1f}")}
          {row("Current LTP", f"₹{position.current_premium:.1f}")}
          {row("SL", f"₹{position.sl:.1f}")}
          {row("P&amp;L", f"₹{position.pnl:+,.0f}", big=True)}
          <div class="check-title">TARGETS</div>
          {tgt_rows}
        </div>"""


    notice_html = f'<div class="notice">{notice}</div>' if notice else ""
    return f"""<section class="panel" id="p-{key}" style="--c:{color};--tc:{trend_color}">
  {notice_html}
  <div class="status-banner">
    <div class="status-text">{d.status}</div>
    <div class="status-reason">{d.reason}</div>
    {f'<div class="trigger-line">{trigger_txt} &nbsp;|&nbsp; {confirm_txt}</div>' if trigger_txt else ''}
    <div class="spot-row">
      <span>{idx_name} <b>{d.spot:,.2f}</b></span>
      <span>({u_chg_pts:+.0f} pts / {u_chg_pct:+.2f}% vs PDC)</span>
      <span>Trend <b class="trend-val">{d.trend.label}</b></span>
      <span>ADX <b>{d.trend.adx:.1f}</b></span>
      <span>Location <b>{d.location.state}</b></span>
    </div>
  </div>

  <div class="quality-row">
    <div class="quality-box"><div class="num" style="color:{color}">{d.score}/10</div>
      <div class="lbl">SETUP QUALITY ({score_bucket(d.score)})</div></div>
    <div class="quality-box"><div class="num" style="color:{color}">{entry_status}</div>
      <div class="lbl">ENTRY STATUS</div></div>
  </div>

  <div class="grid">
    <div class="card">
      <div class="card-title">LEVEL MAP</div>
      <div class="levelmap">{level_rows}</div>
    </div>
    <div class="card">
      <div class="card-title">OPTION CHAIN OI (ATM evidence)</div>
      {row("Call Wall", f"{d.oi.call_wall:,.0f}")}
      {row("Put Support", f"{d.oi.put_support:,.0f}")}
      {row("PCR", f"{d.oi.pcr:.2f}")}
      {row("OI Bias", f"{d.oi.bias} {'✓' if d.oi.confirmation else '✗'}")}
      {oi_evidence}
    </div>
    {strike_html}
    {risk_html}
  </div>

  <div class="signals">
    <div class="card-title">REASON PANEL (10-point checklist)</div>
    {sig_rows}
  </div>
  {trade_plan_html}
  {position_html}
</section>"""


_PAGE_CSS = """
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { background:#0b0f17; color:#e2e8f0; font-family:-apple-system,Segoe UI,Roboto,sans-serif;
          margin:0; padding:12px; }
  .wrap { max-width:1100px; margin:0 auto; }
  .topbar { display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; }
  h1 { font-size:15px; letter-spacing:2px; color:#64748b; text-transform:uppercase; margin:0; }
  .live-badge { font-size:12px; color:#22c55e; }
  .chips { display:grid; grid-template-columns:repeat(auto-fit, minmax(150px,1fr)); gap:8px; margin-bottom:10px; }
  .chip { display:block; text-decoration:none; color:#e2e8f0; background:#141b29; border:1px solid #1f2937;
          border-left:4px solid var(--c); border-radius:10px; padding:8px 12px; }
  .chip.on { background:#1a2333; border-color:var(--c); }
  .chip .cn { font-size:12px; letter-spacing:1px; color:#94a3b8; display:flex; justify-content:space-between; }
  .chip .cs { font-size:16px; font-weight:800; color:var(--c); margin-top:2px; }
  .chip .cx { font-size:11px; color:#64748b; }
  .panel[hidden] { display:none; }
  .notice { background:#eab30818; border:1px solid #eab30866; color:#fcd34d; border-radius:10px;
            padding:8px 12px; font-size:12.5px; margin-bottom:8px; text-align:center; }
  .warnline { color:#ef4444; font-size:12.5px; font-weight:700; padding:4px 0 0; }
  .status-banner { background:color-mix(in srgb, var(--c) 13%, transparent); border:2px solid var(--c);
        border-radius:12px; padding:10px 16px; text-align:center; margin-bottom:8px; }
  .status-text { font-size:20px; font-weight:800; color:var(--c); }
  .status-reason { color:#94a3b8; margin-top:2px; font-size:12px; }
  .trigger-line { color:#cbd5e1; font-size:12px; margin-top:4px; }
  .spot-row { display:flex; justify-content:center; gap:18px; margin-top:6px;
               font-size:12px; color:#94a3b8; flex-wrap:wrap; }
  .spot-row b { color:#e2e8f0; font-size:15px; }
  .quality-row { display:flex; justify-content:center; gap:32px; margin:8px 0; }
  .quality-box { text-align:center; }
  .quality-box .num { font-size:20px; font-weight:800; }
  .quality-box .lbl { color:#64748b; font-size:11px; letter-spacing:1px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(260px,1fr)); gap:10px; }
  @media (min-width: 860px) {
    .grid { grid-template-columns:repeat(3, 1fr); }
    .sig-label, .sig-detail { font-size:14.5px; }
    .row { font-size:15px; }
  }
  .card { background:#141b29; border:1px solid #1f2937; border-radius:10px; padding:10px 12px; }
  .card-title { font-size:10px; letter-spacing:1.2px; color:#64748b; margin-bottom:5px;
                 display:flex; justify-content:space-between; align-items:center; }
  .tag-proj { background:#eab30822; color:#eab308; padding:2px 6px; border-radius:6px; font-size:9px; }
  .tag-live { background:#22c55e22; color:#22c55e; padding:2px 6px; border-radius:6px; font-size:9px; }
  .row { display:flex; justify-content:space-between; padding:2.5px 0; font-size:13px;
          border-bottom:1px solid #1a2333; }
  .row:last-child { border-bottom:none; }
  .lbl { color:#94a3b8; }
  .val { font-weight:600; text-align:right; }
  .val.big { color:var(--c); font-size:16px; }
  .trend-val { color:var(--tc); font-weight:700; }
  .levelmap { display:flex; flex-direction:column; gap:0px; }
  .lvlrow { display:flex; align-items:center; gap:6px; font-size:12px; color:#94a3b8; padding:1.5px 0; }
  .lvl-price { width:70px; text-align:right; font-weight:600; }
  .lvl-dot { color:#475569; }
  .signals { background:#141b29; border:1px solid #1f2937; border-radius:10px;
              padding:10px 12px; margin-top:8px; }
  .sig { display:flex; align-items:center; gap:8px; padding:3px 0; font-size:12.5px;
          border-bottom:1px solid #1a2333; }
  .sig:last-child { border-bottom:none; }
  .dot { width:8px; height:8px; border-radius:50%; flex-shrink:0; }
  .sig-label { color:#94a3b8; width:135px; flex-shrink:0; }
  .sig-detail { color:#cbd5e1; }
  .plan-head { font-size:14px; font-weight:800; margin-bottom:5px; }
  .check-title { font-size:10px; color:#64748b; letter-spacing:1px; margin:5px 0 3px; }
  .check-row { display:flex; gap:6px; font-size:12px; padding:1.5px 0; color:#cbd5e1; }
  .ts { text-align:center; color:#475569; font-size:10px; margin-top:8px; }
"""


def render_page(panels: List[dict], refresh_sec: int = 60, title: str = "Option Decision Engine",
                engine_sec: int = 60, data_ts: Optional[float] = None) -> str:
    """panels: [{key,label,status,color,score,sub,html}] - upar chips (tabs), neeche panels.
    Chip hamesha dono index ka status dikhata hai, isliye doosre tab ka signal miss nahi hota."""
    import html as _html, time as _time
    data_ms = int((data_ts or _time.time()) * 1000)
    chips = ""
    for p in panels:
        chips += (f'<a class="chip" href="#{p["key"]}" data-k="{p["key"]}" style="--c:{p["color"]}">'
                  f'<div class="cn"><span>{_html.escape(p["label"])}</span><span>{p.get("score", "")}</span></div>'
                  f'<div class="cs">{_html.escape(p["status"])}</div>'
                  f'<div class="cx">{_html.escape(p.get("sub", ""))}</div></a>')
    bodies = "\n".join(p["html"] for p in panels)
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="{refresh_sec}">
<title>{title}</title>
<style>{_PAGE_CSS}</style></head>
<body><div class="wrap">
  <div class="topbar">
    <h1>{title}</h1>
    <span class="live-badge" id="live-badge">🟢 LIVE</span>
  </div>
  <div class="chips">{chips}</div>
  {bodies}
  <div class="ts">Data as of {datetime.fromtimestamp(data_ms / 1000):%H:%M:%S} · page har {refresh_sec}s refresh · engine har {engine_sec}s</div>
</div>
<script>
  const dataTs = {data_ms};          // engine ne data kab likha (page load nahi)
  const slow = {engine_sec} * 2 + 20, dead = {engine_sec} * 5;
  function badge() {{
    const sec = Math.max(0, Math.floor((Date.now() - dataTs) / 1000));
    const el = document.getElementById('live-badge');
    if (sec > dead) {{ el.textContent = `🔴 ENGINE RUKA? data ${{sec}}s purana`; el.style.color = '#ef4444'; }}
    else if (sec > slow) {{ el.textContent = `🟠 DATA DELAY • ${{sec}}s purana`; el.style.color = '#f97316'; }}
    else {{ el.textContent = `🟢 LIVE • data ${{sec}}s purana`; el.style.color = '#22c55e'; }}
  }}
  badge(); setInterval(badge, 1000);
  const panels = [...document.querySelectorAll('.panel')];
  const chips = [...document.querySelectorAll('.chip')];
  const keys = panels.map(p => p.id.slice(2));
  function show(k) {{
    if (!keys.includes(k)) k = keys[0];
    panels.forEach(p => p.hidden = (p.id !== 'p-' + k));
    chips.forEach(c => c.classList.toggle('on', c.dataset.k === k));
  }}
  chips.forEach(c => c.addEventListener('click', e => {{
    e.preventDefault(); history.replaceState(null, '', '#' + c.dataset.k); show(c.dataset.k);
  }}));
  show(location.hash.slice(1));
</script>
</body></html>"""


def render_html(d: Decision, refresh_sec: int = 60, position: "Position" = None) -> str:
    """Single-index page (demo/preview scripts ke liye) - purana interface same."""
    key = CFG.index_name.lower()
    return render_page([{
        "key": key, "label": CFG.index_name, "status": d.status, "color": status_color(d),
        "score": f"{d.score}/10", "sub": d.reason[:60],
        "html": _panel_html(d, position, key),
    }], refresh_sec, f"{CFG.index_name} Option Engine")

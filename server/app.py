"""
Multi-user web server: har user apne Upstox API Key/Secret se login karta hai.
- Secret sirf RAM mein rehta hai (disk par kabhi nahi likhta).
- Har user ka apna engine thread (apna token, apni position).
Chalane ke liye: gunicorn app:app --workers 1 --threads 8   (workers=1 zaroori: sessions RAM mein hain)
"""
import contextlib
import html
import os
import secrets
import threading
import time
from pathlib import Path
from urllib.parse import urlencode

import requests
from flask import Flask, request, redirect, make_response, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
os.chdir(DATA_DIR)                      # start.py ki cache/instrument files yahin banengi

import nifty_option_engine as eng       # noqa: E402
import upstox_adapter as ua             # noqa: E402
from nifty_option_engine import render_page  # noqa: E402
import futures                          # noqa: E402

# ---- Engine ka CFG global hai -> threads ke beech lock zaroori ----
_cfg_lock = threading.RLock()
_orig_use_config = eng.use_config


@contextlib.contextmanager
def _locked_use_config(cfg):
    with _cfg_lock:
        with _orig_use_config(cfg) as c:
            yield c


ua.use_config = _locked_use_config

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

INTERVAL = 60            # engine cycle (sec)
IDLE_STOP = 20 * 60      # koi page na khole to itni der baad thread ruk jaata hai
SESSION_TTL = 24 * 3600
COOKIE = "sid"


class Sess:
    def __init__(self, cid, secret, redirect_uri, max_risk):
        self.sid = secrets.token_urlsafe(24)
        self.cid, self.secret, self.redirect_uri, self.max_risk = cid, secret, redirect_uri, max_risk
        self.token = None
        self.page = None
        self.error = ""
        self.last_seen = time.time()
        self.stop = threading.Event()
        self.thread = None
        self.lock = threading.Lock()


SESSIONS = {}
_fut_lock = threading.Lock()


def redirect_uri_for(req) -> str:
    base = os.environ.get("BASE_URL") or req.host_url.rstrip("/")
    return base + "/callback"


# ---------------------------------------------------------------------
# Engine thread (ek user = ek thread)
# ---------------------------------------------------------------------

def _loop(s: Sess):
    try:
        with _fut_lock:
            infos = futures.get_futures_info(("NIFTY", "SENSEX"))
        runners = [ua.IndexRunner(n, s.token, infos[n]["key"], infos[n]["lot_size"], s.max_risk, 5)
                   for n in ("NIFTY", "SENSEX")]
        for r in runners:
            r.setup()
    except BaseException as e:           # SystemExit bhi (lot size na mile)
        s.error = f"Start nahi hua: {e}"
        return
    while not s.stop.is_set():
        if time.time() - s.last_seen > IDLE_STOP:
            return
        t0 = time.time()
        panels = [r.cycle() for r in runners]
        if any("401" in p.get("sub", "") for p in panels):
            s.token, s.error = None, "Token expire ho gaya - dobara login karo."
            return
        s.page = render_page(panels, 20, "NIFTY + SENSEX Option Engine",
                             engine_sec=INTERVAL, data_ts=time.time())
        s.stop.wait(max(5.0, INTERVAL - (time.time() - t0)))


def ensure_running(s: Sess):
    with s.lock:
        if s.token and (s.thread is None or not s.thread.is_alive()):
            s.stop.clear()
            s.error = ""
            s.thread = threading.Thread(target=_loop, args=(s,), daemon=True)
            s.thread.start()


def _reaper():
    while True:
        time.sleep(600)
        now = time.time()
        for sid, s in list(SESSIONS.items()):
            if now - s.last_seen > SESSION_TTL:
                s.stop.set()
                SESSIONS.pop(sid, None)


threading.Thread(target=_reaper, daemon=True).start()


def current() -> "Sess | None":
    s = SESSIONS.get(request.cookies.get(COOKIE, ""))
    if s:
        s.last_seen = time.time()
    return s


# ---------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------

BASE_CSS = """
:root{color-scheme:dark}*{box-sizing:border-box}
body{background:#0b0f17;color:#e2e8f0;font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:0;padding:16px}
.box{max-width:460px;margin:24px auto}h1{font-size:18px;letter-spacing:2px;color:#94a3b8;text-transform:uppercase}
label{display:block;font-size:12px;color:#94a3b8;margin:14px 0 4px}
input{width:100%;padding:12px;border-radius:10px;border:1px solid #1f2937;background:#141b29;color:#e2e8f0;font-size:15px}
button{width:100%;margin-top:18px;padding:14px;border:0;border-radius:10px;background:#22c55e;color:#04120a;font-weight:800;font-size:15px}
.note{background:#141b29;border:1px solid #1f2937;border-radius:10px;padding:10px 12px;font-size:12.5px;color:#94a3b8;margin-top:14px;word-break:break-all}
.err{background:#ef444418;border:1px solid #ef444466;color:#fca5a5;border-radius:10px;padding:10px 12px;font-size:13px;margin-bottom:10px}
code{color:#fcd34d}
"""


def login_page(redirect_uri: str, msg: str = "") -> str:
    err = f'<div class="err">{html.escape(msg)}</div>' if msg else ""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Option Engine Login</title>
<style>{BASE_CSS}</style></head><body><div class="box">
<h1>Option Engine · Login</h1>{err}
<form method="post" action="/login" id="f">
<label>API Key (client_id)</label><input name="client_id" id="cid" required autocomplete="off">
<label>API Secret</label><input name="client_secret" id="sec" type="password" required autocomplete="off">
<label>Max risk per trade (₹)</label><input name="max_risk" id="risk" type="number" value="2000" min="100">
<button type="submit">Login with Upstox</button></form>
<div class="note">Apne Upstox app (account.upstox.com/developer/apps) mein yeh <b>Redirect URI</b> daalo:<br>
<code>{html.escape(redirect_uri)}</code></div>
<div class="note">Key/Secret sirf is phone mein yaad rehte hain aur server ki RAM mein session tak. Disk par save nahi hote.
Token roz subah expire hota hai, to roz ek baar login.</div>
</div>
<script>
 const g=k=>{{try{{return localStorage.getItem(k)||''}}catch(e){{return ''}}}};
 cid.value=g('cid');sec.value=g('sec');if(g('risk'))risk.value=g('risk');
 f.addEventListener('submit',()=>{{try{{localStorage.setItem('cid',cid.value);localStorage.setItem('sec',sec.value);localStorage.setItem('risk',risk.value)}}catch(e){{}}}});
</script></body></html>"""


def loading_page(msg: str) -> str:
    return (f'<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<meta http-equiv="refresh" content="3"><style>{BASE_CSS}</style></head>'
            f'<body><div class="box"><h1>Option Engine</h1><div class="note">{html.escape(msg)}</div></div></body></html>')


LOGOUT_BAR = ('<form method="post" action="/logout" style="text-align:center;margin:14px 0">'
              '<button style="background:#1f2937;color:#94a3b8;width:auto;padding:8px 18px;font-size:12px">Logout</button></form>')


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------

@app.get("/")
def index():
    s = current()
    if not s or not s.token:
        msg = s.error if s else ""
        return login_page(redirect_uri_for(request), msg)
    ensure_running(s)
    if s.error and not s.page:
        return login_page(redirect_uri_for(request), s.error)
    if not s.page:
        return loading_page("Engine start ho raha hai... (pehli baar 30-60 sec lag sakte hain)")
    page = s.page.replace("</body>", LOGOUT_BAR + "</body>")
    r = make_response(page)
    r.headers["Cache-Control"] = "no-store"
    return r


@app.post("/login")
def login():
    cid = request.form.get("client_id", "").strip()
    sec = request.form.get("client_secret", "").strip()
    try:
        risk = float(request.form.get("max_risk") or 2000)
    except ValueError:
        risk = 2000.0
    if not cid or not sec:
        return login_page(redirect_uri_for(request), "Key aur Secret dono daalo."), 400
    old = current()
    if old:
        old.stop.set()
        SESSIONS.pop(old.sid, None)
    s = Sess(cid, sec, redirect_uri_for(request), risk)
    SESSIONS[s.sid] = s
    url = "https://api.upstox.com/v2/login/authorization/dialog?" + urlencode(
        {"response_type": "code", "client_id": cid, "redirect_uri": s.redirect_uri, "state": s.sid})
    r = redirect(url)
    r.set_cookie(COOKIE, s.sid, max_age=SESSION_TTL, httponly=True, secure=request.is_secure, samesite="Lax")
    return r


@app.get("/callback")
def callback():
    s = SESSIONS.get(request.cookies.get(COOKIE, "")) or SESSIONS.get(request.args.get("state", ""))
    code = request.args.get("code")
    if not s or not code:
        return login_page(redirect_uri_for(request), "Login adhura raha - dobara try karo."), 400
    try:
        resp = requests.post(
            "https://api.upstox.com/v2/login/authorization/token",
            data={"code": code, "client_id": s.cid, "client_secret": s.secret,
                  "redirect_uri": s.redirect_uri, "grant_type": "authorization_code"},
            headers={"accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            timeout=15)
        data = resp.json()
    except Exception as e:
        return login_page(redirect_uri_for(request), f"Upstox se baat nahi hui: {e}"), 502
    if "access_token" not in data:
        return login_page(redirect_uri_for(request), f"Token nahi mila: {str(data)[:160]}"), 400
    s.token = data["access_token"]
    s.secret = ""                       # token mil gaya, secret ki ab zaroorat nahi
    s.page, s.error = None, ""
    ensure_running(s)
    r = redirect("/")
    r.set_cookie(COOKIE, s.sid, max_age=SESSION_TTL, httponly=True, secure=request.is_secure, samesite="Lax")
    return r


@app.post("/logout")
def logout():
    s = current()
    if s:
        s.stop.set()
        SESSIONS.pop(s.sid, None)
    r = redirect("/")
    r.delete_cookie(COOKIE)
    return r


@app.get("/health")
def health():
    return jsonify(ok=True, sessions=len(SESSIONS))

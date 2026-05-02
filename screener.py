#!/usr/bin/env python3
"""
Growth Momentum Stock Screener
- Auto-fetches full S&P 500 + NASDAQ 100 from Wikipedia (always current)
- Compares with previous day's results (new entries + exits highlighted in email)
- Runs on GitHub Actions and emails results via Gmail
"""

import yfinance as yf
import pandas as pd
import smtplib, os, json, logging
from concurrent.futures import ThreadPoolExecutor
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ── CONFIG ────────────────────────────────────────────────────────
GMAIL_ADDRESS  = os.environ.get("GMAIL_ADDRESS",  "your.gmail@gmail.com")
GMAIL_APP_PASS = os.environ.get("GMAIL_APP_PASS", "xxxx xxxx xxxx xxxx")
SEND_TO        = os.environ.get("SEND_TO",        "recipient@email.com")

CONDITIONS = {
    "mktcap_min_b":   2.0,
    "eps_growth_min": 15.0,
    "rev_growth_min": 15.0,
    "above_sma84":    True,
    "ath_within_pct": 15.0,
    "rs_min":         70,
}

ACTIVE_EXCHANGES = [
    "US",
    "BME",
    "XETRA",
    "EURONEXT",
    "AMS",
    # "LSE",
    # "BIT",
    # "SIX",
    # "STO",
    # "OSL",
]

MAX_PER_EXCHANGE = 600
MAX_WORKERS      = 12
HISTORY_FILE     = "last_results.json"   # saved in repo between runs

# ── Fetch live S&P 500 + NASDAQ 100 from Wikipedia ────────────────
def get_us_universe():
    tickers = []
    try:
        sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
                             storage_options={'User-Agent': 'Mozilla/5.0'})[0]
        sp_tickers = [str(t).replace('.', '-') for t in sp500['Symbol'].tolist()]
        tickers.extend(sp_tickers)
        print(f"  ✓ S&P 500: {len(sp_tickers)} from Wikipedia")
    except Exception as e:
        print(f"  ⚠ S&P 500 fetch failed: {e}")

    try:
        ndx_tables = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100',
                                  storage_options={'User-Agent': 'Mozilla/5.0'})
        ndx_tickers = []
        for tbl in ndx_tables:
            for col in ['Ticker', 'Symbol']:
                if col in tbl.columns:
                    ndx_tickers = [str(x).replace('.', '-') for x in tbl[col].tolist()
                                   if isinstance(x, str) and x.strip()]
                    break
            if ndx_tickers: break
        tickers.extend(ndx_tickers)
        print(f"  ✓ NASDAQ 100: {len(ndx_tickers)} from Wikipedia")
    except Exception as e:
        print(f"  ⚠ NASDAQ 100 fetch failed: {e}")

    seen, unique = set(), []
    for t in tickers:
        if t and t not in seen and len(t) <= 6:
            seen.add(t); unique.append(t)
    print(f"  → {len(unique)} unique US tickers\n")
    return unique

# ── European tickers ──────────────────────────────────────────────
EUROPEAN_TICKERS = {
    "BME":      ['ITX.MC','SAN.MC','TEF.MC','BBVA.MC','IBE.MC','REP.MC','FER.MC','ACS.MC','ANA.MC','ENG.MC','GRF.MC','ACX.MC','MTS.MC','AENA.MC','ELE.MC','REE.MC','MAP.MC','NTGY.MC','BKT.MC','CABK.MC','SAB.MC','MEL.MC','VIS.MC','COL.MC','CIE.MC','TRE.MC','LOG.MC','CLNX.MC','IAG.MC','AMS.MC'],
    "XETRA":    ['SAP.DE','SIE.DE','ALV.DE','MRK.DE','DTE.DE','BMW.DE','MBG.DE','BAYN.DE','DB1.DE','BAS.DE','EOAN.DE','RWE.DE','MUV2.DE','DBK.DE','HEI.DE','VNA.DE','IFX.DE','DHER.DE','ADS.DE','PUM.DE','ZAL.DE','DHL.DE','CON.DE','BOSS.DE','HLAG.DE','QIA.DE','ENR.DE','LEG.DE'],
    "EURONEXT": ['MC.PA','OR.PA','TTE.PA','SAN.PA','AIR.PA','BNP.PA','SU.PA','RI.PA','CS.PA','EL.PA','ORA.PA','GLE.PA','ACA.PA','KER.PA','SGO.PA','VIE.PA','ML.PA','CAP.PA','LR.PA','DG.PA','RMS.PA','PUB.PA','DSY.PA','WLN.PA','FR.PA','AF.PA','HO.PA','SW.PA','STLAP.PA','VK.PA','EDEN.PA'],
    "AMS":      ['ASML.AS','HEIA.AS','REN.AS','IMCD.AS','AKZA.AS','NN.AS','PHIA.AS','AGN.AS','BESI.AS','ADYEN.AS','ASRNL.AS','UMG.AS','WKL.AS','KPN.AS','MT.AS','RAND.AS','AALB.AS','ING.AS','ABN.AS','URW.AS','AMG.AS','JDEP.AS','SBMO.AS','TKWY.AS'],
    "LSE":      ['SHEL.L','AZN.L','HSBA.L','ULVR.L','BP.L','GSK.L','RIO.L','REL.L','NG.L','LSEG.L','PRU.L','BATS.L','LLOY.L','NWG.L','BARC.L','VOD.L','GLEN.L','CRH.L','IMB.L','RKT.L','AHT.L','DGE.L','SKG.L','SSE.L','EXPN.L','WPP.L','SGE.L'],
    "BIT":      ['ENI.MI','ENEL.MI','ISP.MI','UCG.MI','G.MI','TIT.MI','STM.MI','RACE.MI','MONC.MI','LDO.MI','CPR.MI','CNHI.MI','AMP.MI','SPM.MI','BAMI.MI','INW.MI'],
    "SIX":      ['NESN.SW','ROG.SW','NOVN.SW','ABBN.SW','ZURN.SW','ALC.SW','GIVN.SW','LONN.SW','SIKA.SW','UBSG.SW','CFR.SW','BALN.SW','SLHN.SW','GEBN.SW','PGHN.SW','SCMN.SW','LOGN.SW','TEMN.SW'],
    "STO":      ['ERIC-B.ST','VOLV-B.ST','INVE-B.ST','ATCO-A.ST','SAND.ST','SEB-A.ST','SWED-A.ST','HEXA-B.ST','ABB.ST','TELIA.ST','EVO.ST','LIFCO-B.ST','ASSA-B.ST','HUSQ-B.ST','NIBE-B.ST','AAK.ST','SKF-B.ST','BOL.ST'],
    "OSL":      ['EQNR.OL','DNB.OL','MOWI.OL','NHY.OL','AKERBP.OL','YAR.OL','KAHOT.OL','ORK.OL','SUBC.OL','RECSI.OL'],
}

FLAGS = {"US":"🇺🇸","BME":"🇪🇸","XETRA":"🇩🇪","EURONEXT":"🇫🇷","AMS":"🇳🇱","LSE":"🇬🇧","BIT":"🇮🇹","SIX":"🇨🇭","STO":"🇸🇪","OSL":"🇳🇴"}

# ── Build universe ────────────────────────────────────────────────
print("Building ticker universe...")
TICKERS = dict(EUROPEAN_TICKERS)
TICKERS["US"] = get_us_universe()

# ── Fetch & filter ────────────────────────────────────────────────
def fetch_one(symbol):
    try:
        info = yf.Ticker(symbol).info
        price = info.get('regularMarketPrice') or info.get('currentPrice')
        if not info or not price or price == 0: return None
        return {
            'symbol':   symbol,
            'name':     (info.get('longName') or info.get('shortName') or symbol)[:40],
            'sector':   info.get('sector', '—'),
            'currency': info.get('currency', 'USD'),
            'price':    price,
            'mktcap':   info.get('marketCap'),
            'high52':   info.get('fiftyTwoWeekHigh'),
            'low52':    info.get('fiftyTwoWeekLow'),
            'sma50':    info.get('fiftyDayAverage'),
            'sma200':   info.get('twoHundredDayAverage'),
            'eps_g':    info.get('earningsGrowth'),
            'rev_g':    info.get('revenueGrowth'),
        }
    except: return None

def passes(d):
    if not d or not d['price']: return False
    c = CONDITIONS
    if c['mktcap_min_b'] and d['mktcap'] and d['mktcap'] < c['mktcap_min_b'] * 1e9: return False
    if c['ath_within_pct'] is not None and d['high52']:
        if (d['high52'] - d['price']) / d['high52'] * 100 > c['ath_within_pct']: return False
    if c['eps_growth_min'] is not None and d['eps_g'] is not None:
        if d['eps_g'] * 100 < c['eps_growth_min']: return False
    if c['rev_growth_min'] is not None and d['rev_g'] is not None:
        if d['rev_g'] * 100 < c['rev_growth_min']: return False
    if c['above_sma84'] and d['sma50'] and d['price']:
        sma = d['sma50'] * 0.58 + (d['sma200'] or d['sma50']) * 0.42
        if d['price'] < sma: return False
    if c['rs_min'] and d['high52'] and d['low52'] and d['high52'] > d['low52']:
        rs = int((d['price'] - d['low52']) / (d['high52'] - d['low52']) * 99)
        if rs < c['rs_min']: return False
    return True

def build_row(d, exchange):
    p, h, l = d['price'], d['high52'], d['low52']
    sma   = (d['sma50'] * 0.58 + (d['sma200'] or d['sma50']) * 0.42) if d['sma50'] else None
    rs    = int((p - l) / (h - l) * 99) if (h and l and h > l) else None
    eps_p = round(d['eps_g'] * 100, 1)  if d['eps_g'] is not None else None
    rev_p = round(d['rev_g'] * 100, 1)  if d['rev_g'] is not None else None
    ath_d = round((h - p) / h * 100, 1) if h else None
    above = (p > sma) if sma else None
    score = sum([eps_p is not None and eps_p > 20, rev_p is not None and rev_p > 20,
                 rs is not None and rs > 80, ath_d is not None and ath_d < 5, above is True])
    curr  = {'GBP':'£','EUR':'€','CHF':'₣','SEK':'kr','NOK':'kr','DKK':'kr'}.get(d['currency'], '$')
    return {
        'symbol': d['symbol'], 'name': d['name'], 'exchange': exchange,
        'flag': FLAGS.get(exchange, ''), 'sector': d['sector'],
        'curr': curr, 'price': round(p, 2),
        'mktcap_b': round(d['mktcap'] / 1e9, 1) if d['mktcap'] else None,
        'eps_growth': eps_p, 'rev_growth': rev_p,
        'above_sma': above, 'ath_diff': ath_d, 'rs': rs,
        'signal': 'Strong' if score >= 4 else 'Medium',
    }

# ── Load / save previous results ─────────────────────────────────
def load_previous():
    """Load yesterday's results from the JSON file saved in the repo."""
    if not os.path.exists(HISTORY_FILE):
        return {}, None
    try:
        with open(HISTORY_FILE) as f:
            data = json.load(f)
        return {r['symbol']: r for r in data.get('results', [])}, data.get('date')
    except:
        return {}, None

def save_current(results):
    """Save today's results to JSON so tomorrow's run can compare."""
    with open(HISTORY_FILE, 'w') as f:
        json.dump({
            'date': datetime.now().strftime('%Y-%m-%d'),
            'results': results
        }, f, indent=2)
    print(f"✓ Saved {len(results)} results to {HISTORY_FILE}")

# ── Main scan ─────────────────────────────────────────────────────
def run():
    results, total = [], 0
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Starting scan...")
    for ex in ACTIVE_EXCHANGES:
        tickers = TICKERS.get(ex, [])[:MAX_PER_EXCHANGE]
        print(f"  [{ex}] {len(tickers)} tickers...", end=' ', flush=True)
        matched = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for d in pool.map(fetch_one, tickers):
                total += 1
                if passes(d):
                    results.append(build_row(d, ex))
                    matched += 1
        print(f"{matched} passed")
    # Deduplicate
    seen, unique = set(), []
    for r in sorted(results, key=lambda x: x['rs'] or 0, reverse=True):
        if r['symbol'] not in seen:
            seen.add(r['symbol']); unique.append(r)
    print(f"\nTotal: {total} scanned | {len(unique)} unique matches")
    return unique

# ── Email HTML ────────────────────────────────────────────────────
def stock_row(r, badge=None):
    eps = f"{r['eps_growth']:+.1f}%" if r['eps_growth'] is not None else 'n/a'
    rev = f"{r['rev_growth']:+.1f}%" if r['rev_growth'] is not None else 'n/a'
    mc  = f"${r['mktcap_b']}B" if r['mktcap_b'] is not None else '—'
    sma = '✓' if r['above_sma'] is True else ('✗' if r['above_sma'] is False else '—')
    rs  = str(r['rs']) if r['rs'] is not None else '—'
    eps_color = '#22c55e' if r['eps_growth'] and r['eps_growth'] > 0 else '#ef4444'
    rev_color = '#22c55e' if r['rev_growth'] and r['rev_growth'] > 0 else '#ef4444'
    badge_html = f'<span style="background:{badge[1]};color:{badge[2]};font-size:9px;padding:1px 6px;border-radius:10px;margin-left:6px;font-weight:700">{badge[0]}</span>' if badge else ''
    if badge and badge[0] == 'NEW':
        bg = '#0a1f0a'
    elif badge and badge[0] == 'EXIT':
        bg = '#1f0a0a'
    elif r['signal'] == 'Strong':
        bg = '#0d1a0d'
    else:
        bg = '#1a1600'
    return f"""<tr style="background:{bg}">
      <td style="padding:10px 14px;border-bottom:1px solid #1e1e27">
        <b style="color:#e8e8f0;font-size:14px">{r['flag']} {r['symbol']}</b>{badge_html}<br>
        <span style="color:#666;font-size:11px">{r['name']} · {r['exchange']}</span>
      </td>
      <td style="padding:10px 14px;border-bottom:1px solid #1e1e27;color:#666;font-size:12px">{r['sector']}</td>
      <td style="padding:10px 14px;border-bottom:1px solid #1e1e27;color:#e8e8f0;font-family:monospace">{r['curr']}{r['price']}</td>
      <td style="padding:10px 14px;border-bottom:1px solid #1e1e27;color:#666;font-size:12px">{mc}</td>
      <td style="padding:10px 14px;border-bottom:1px solid #1e1e27;color:{eps_color};font-family:monospace">{eps}</td>
      <td style="padding:10px 14px;border-bottom:1px solid #1e1e27;color:{rev_color};font-family:monospace">{rev}</td>
      <td style="padding:10px 14px;border-bottom:1px solid #1e1e27;color:#888;text-align:center">{sma}</td>
      <td style="padding:10px 14px;border-bottom:1px solid #1e1e27;color:#4f8eff;font-family:monospace;font-weight:600">{rs}</td>
    </tr>"""

THEAD = """<tr style="background:#0a0a0f">
  <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Stock</th>
  <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Sector</th>
  <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Price</th>
  <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Mkt Cap</th>
  <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">EPS gr.</th>
  <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Rev gr.</th>
  <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">SMA↑</th>
  <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">RS</th>
</tr>"""

def table(rows_html):
    return f'<table width="100%" style="border-collapse:collapse;background:#111118;border-radius:8px;overflow:hidden"><thead>{THEAD}</thead><tbody>{rows_html}</tbody></table>'

def section_html(title, color, rows, badge=None):
    if not rows: return ''
    rows_html = ''.join(stock_row(r, badge) for r in rows)
    return f'<h2 style="color:{color};font-size:15px;margin:28px 0 8px;font-family:sans-serif">{title} — {len(rows)} stocks</h2>{table(rows_html)}'

def build_email(results, prev_map, prev_date):
    now    = datetime.now().strftime('%Y-%m-%d')
    today_map = {r['symbol']: r for r in results}

    # Classify
    new_entries = [r for r in results if r['symbol'] not in prev_map]
    exits       = [r for r in prev_map.values() if r['symbol'] not in today_map]
    strong      = [r for r in results if r['signal'] == 'Strong']
    medium      = [r for r in results if r['signal'] == 'Medium']

    # Conditions summary
    conds, c = [], CONDITIONS
    if c['mktcap_min_b']:   conds.append(f"Mkt cap > ${c['mktcap_min_b']}B")
    if c['eps_growth_min']: conds.append(f"EPS > {c['eps_growth_min']}%")
    if c['rev_growth_min']: conds.append(f"Rev > {c['rev_growth_min']}%")
    if c['above_sma84']:    conds.append("Above SMA84")
    if c['ath_within_pct']: conds.append(f"Within {c['ath_within_pct']}% of 52wH")
    if c['rs_min']:         conds.append(f"RS > {c['rs_min']}")

    # Changes section
    has_prev   = bool(prev_map)
    prev_label = f"vs {prev_date}" if prev_date else "first run"
    changes_html = ''
    if has_prev:
        changes_html = f'''
        <div style="margin-bottom:28px">
          <h2 style="color:#e8e8f0;font-size:16px;margin:0 0 12px;font-family:sans-serif">
            📊 Changes {prev_label}
          </h2>
          <div style="display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap">
            <div style="background:#0a1f0a;border:1px solid #22c55e44;border-radius:8px;padding:12px 20px;min-width:90px">
              <div style="font-size:24px;font-weight:700;color:#22c55e">+{len(new_entries)}</div>
              <div style="font-size:11px;color:#555;margin-top:2px">New entries</div>
            </div>
            <div style="background:#1f0a0a;border:1px solid #ef444444;border-radius:8px;padding:12px 20px;min-width:90px">
              <div style="font-size:24px;font-weight:700;color:#ef4444">-{len(exits)}</div>
              <div style="font-size:11px;color:#555;margin-top:2px">Dropped out</div>
            </div>
            <div style="background:#111118;border:1px solid #1e1e27;border-radius:8px;padding:12px 20px;min-width:90px">
              <div style="font-size:24px;font-weight:700">{len(results)}</div>
              <div style="font-size:11px;color:#555;margin-top:2px">Total today</div>
            </div>
          </div>
          {section_html('🟢 New — entered the screen today', '#22c55e', new_entries, badge=('NEW','#22c55e','#000'))}
          {section_html('🔴 Exits — dropped out since yesterday', '#ef4444', exits, badge=('EXIT','#ef4444','#fff'))}
        </div>'''
    else:
        changes_html = '<div style="background:#111118;border:1px solid #1e1e27;border-radius:8px;padding:14px 18px;margin-bottom:28px;font-size:12px;color:#555">ℹ️ First run — no previous data to compare. From tomorrow you\'ll see entries and exits.</div>'

    return f"""<!DOCTYPE html><html>
<body style="background:#0a0a0f;color:#e8e8f0;font-family:sans-serif;padding:28px;max-width:900px;margin:auto">

  <div style="border-bottom:1px solid #1e1e27;padding-bottom:16px;margin-bottom:24px">
    <h1 style="font-size:22px;margin:0 0 6px">📈 Daily Growth Screener — {now}</h1>
    <div style="color:#555;font-size:12px">{'  ·  '.join(conds)}</div>
  </div>

  <div style="display:flex;gap:16px;margin-bottom:28px;flex-wrap:wrap">
    <div style="background:#111118;border:1px solid #1e1e27;border-radius:8px;padding:14px 22px">
      <div style="font-size:26px;font-weight:700">{len(results)}</div>
      <div style="font-size:11px;color:#555;margin-top:3px">Total matches</div>
    </div>
    <div style="background:#111118;border:1px solid #22c55e44;border-radius:8px;padding:14px 22px">
      <div style="font-size:26px;font-weight:700;color:#22c55e">{len(strong)}</div>
      <div style="font-size:11px;color:#555;margin-top:3px">Strong signals</div>
    </div>
    <div style="background:#111118;border:1px solid #f59e0b44;border-radius:8px;padding:14px 22px">
      <div style="font-size:26px;font-weight:700;color:#f59e0b">{len(medium)}</div>
      <div style="font-size:11px;color:#555;margin-top:3px">Medium signals</div>
    </div>
    <div style="background:#111118;border:1px solid #1e1e27;border-radius:8px;padding:14px 22px">
      <div style="font-size:14px;font-weight:600;color:#888">{', '.join(ACTIVE_EXCHANGES)}</div>
      <div style="font-size:11px;color:#555;margin-top:3px">Exchanges scanned</div>
    </div>
  </div>

  {changes_html}

  {section_html('⭐ Strong signals', '#22c55e', strong)}
  {section_html('◑ Medium signals', '#f59e0b', medium)}

  <div style="margin-top:28px;padding-top:14px;border-top:1px solid #1e1e27;font-size:11px;color:#333">
    Data: Yahoo Finance · Indices: live from Wikipedia · RS = 52-week range position
  </div>
</body></html>"""

def send_email(results, prev_map, prev_date):
    now    = datetime.now().strftime('%Y-%m-%d')
    strong = sum(1 for r in results if r['signal'] == 'Strong')
    new    = sum(1 for r in results if r['symbol'] not in prev_map)
    exits  = sum(1 for r in prev_map.values() if r['symbol'] not in {x['symbol'] for x in results})
    subj   = f"📈 Screener {now} — {len(results)} matches ({strong} strong)"
    if prev_map:
        subj += f" | +{new} new  -{exits} exits"
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subj
    msg['From']    = GMAIL_ADDRESS
    msg['To']      = SEND_TO
    msg.attach(MIMEText(build_email(results, prev_map, prev_date), 'html'))
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as s:
        s.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        s.sendmail(GMAIL_ADDRESS, SEND_TO, msg.as_string())
    print(f"✅ Email sent to {SEND_TO}")

# ── Entry point ───────────────────────────────────────────────────
if __name__ == '__main__':
    # Load previous day's results
    prev_map, prev_date = load_previous()
    if prev_map:
        print(f"📂 Loaded {len(prev_map)} results from previous run ({prev_date})")
    else:
        print("📂 No previous results found — this is the first run")

    # Run scan
    results = run()

    # Save today's results for tomorrow's comparison
    save_current(results)

    # Send email
    if GMAIL_ADDRESS != "your.gmail@gmail.com":
        send_email(results, prev_map, prev_date)
    else:
        print(f"⚠️  Configure secrets to receive email. Found {len(results)} matches.")

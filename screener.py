#!/usr/bin/env python3
"""
Growth Momentum Stock Screener
Runs automatically every morning via GitHub Actions
Sends results by email via Gmail App Password
"""

import yfinance as yf
import pandas as pd
import smtplib, os, logging
from concurrent.futures import ThreadPoolExecutor
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ── CONFIG — edit these directly or set as GitHub Secrets ────────
GMAIL_ADDRESS  = os.environ.get("GMAIL_ADDRESS",  "your.gmail@gmail.com")
GMAIL_APP_PASS = os.environ.get("GMAIL_APP_PASS", "xxxx xxxx xxxx xxxx")
SEND_TO        = os.environ.get("SEND_TO",        "recipient@email.com")

CONDITIONS = {
    "mktcap_min_b":   2.0,    # Market cap > X billion  (0 = off)
    "eps_growth_min": 15.0,   # EPS growth > X% YoY     (None = off)
    "rev_growth_min": 15.0,   # Revenue growth > X% YoY (None = off)
    "above_sma84":    True,   # Price above 84-day SMA
    "ath_within_pct": 15.0,   # Within X% of 52w high   (None = off)
    "rs_min":         70,     # RS rank > X (0-99)       (0 = off)
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

MAX_PER_EXCHANGE = 100
MAX_WORKERS      = 10

# ── Ticker universe ───────────────────────────────────────────────
TICKERS = {
    "US": list(dict.fromkeys([
        'AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','AVGO','JPM','LLY',
        'V','UNH','XOM','MA','JNJ','PG','HD','COST','MRK','ABBV','CVX','CRM',
        'BAC','NFLX','AMD','PEP','TMO','ORCL','ACN','CSCO','ABT','WFC','MCD',
        'GS','PM','DIS','CAT','NOW','INTU','ISRG','QCOM','UBER','AMGN','RTX',
        'NEE','PFE','SPGI','BKNG','AXP','LOW','BLK','SYK','GILD','TJX','VRTX',
        'ADI','SCHW','C','BA','MDT','ETN','REGN','LRCX','ZTS','PLD','GE','DUK',
        'CME','ICE','SHW','ITW','NOC','APD','CL','KLAC','EMR','MCO','FCX','NSC',
        'CSX','MSI','MPC','CTAS','WM','ORLY','FDX','MAR','ADBE','COP','EOG',
        'TMUS','MRVL','PANW','ADP','CDNS','SNPS','MCHP','FTNT','ABNB','CRWD',
        'DDOG','PAYX','FAST','ODFL','ROST','IDXX','VRSK','GEHC','CPRT','TEAM',
        'ZS','TTD','ADSK','WDAY','DLTR','EBAY','EA','ALGN','ANSS','CTSH','PCAR',
    ])),
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

# ── Fetch ─────────────────────────────────────────────────────────
def fetch_one(symbol):
    try:
        info = yf.Ticker(symbol).info
        price = info.get('regularMarketPrice') or info.get('currentPrice')
        if not info or not price or price == 0:
            return None
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
    except:
        return None

# ── Filter ────────────────────────────────────────────────────────
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

# ── Scan ──────────────────────────────────────────────────────────
def run():
    results = []
    total   = 0
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Starting scan...")

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
            seen.add(r['symbol'])
            unique.append(r)

    print(f"\nTotal: {total} scanned | {len(unique)} unique matches")
    return unique

# ── Email HTML ────────────────────────────────────────────────────
def build_email(results):
    now    = datetime.now().strftime('%Y-%m-%d')
    strong = [r for r in results if r['signal'] == 'Strong']
    medium = [r for r in results if r['signal'] == 'Medium']

    def row(r):
        eps = f"{r['eps_growth']:+.1f}%" if r['eps_growth'] is not None else 'n/a'
        rev = f"{r['rev_growth']:+.1f}%" if r['rev_growth'] is not None else 'n/a'
        mc  = f"${r['mktcap_b']}B"        if r['mktcap_b']   is not None else '—'
        sma = '✓' if r['above_sma'] is True else ('✗' if r['above_sma'] is False else '—')
        rs  = str(r['rs']) if r['rs'] is not None else '—'
        eps_color = '#22c55e' if r['eps_growth'] and r['eps_growth'] > 0 else '#ef4444'
        rev_color = '#22c55e' if r['rev_growth'] and r['rev_growth'] > 0 else '#ef4444'
        bg  = '#0d1a0d' if r['signal'] == 'Strong' else '#1a1600'
        return f"""<tr style="background:{bg}">
          <td style="padding:10px 14px;border-bottom:1px solid #1e1e27">
            <b style="color:#e8e8f0;font-size:14px">{r['flag']} {r['symbol']}</b><br>
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

    thead = """<tr style="background:#0a0a0f">
      <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Stock</th>
      <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Sector</th>
      <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Price</th>
      <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Mkt Cap</th>
      <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">EPS gr.</th>
      <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">Rev gr.</th>
      <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">SMA↑</th>
      <th style="padding:8px 14px;text-align:left;color:#444;font-size:10px;text-transform:uppercase;letter-spacing:.08em">RS</th>
    </tr>"""

    def section(title, color, rows):
        if not rows: return ''
        return f"""
        <h2 style="color:{color};font-size:15px;margin:28px 0 8px;font-family:sans-serif">{title} — {len(rows)} stocks</h2>
        <table width="100%" style="border-collapse:collapse;background:#111118;border-radius:8px;overflow:hidden">
          <thead>{thead}</thead>
          <tbody>{''.join(row(r) for r in rows)}</tbody>
        </table>"""

    conds = []
    c = CONDITIONS
    if c['mktcap_min_b']:   conds.append(f"Mkt cap > ${c['mktcap_min_b']}B")
    if c['eps_growth_min']: conds.append(f"EPS > {c['eps_growth_min']}%")
    if c['rev_growth_min']: conds.append(f"Rev > {c['rev_growth_min']}%")
    if c['above_sma84']:    conds.append("Above SMA84")
    if c['ath_within_pct']: conds.append(f"Within {c['ath_within_pct']}% of 52wH")
    if c['rs_min']:         conds.append(f"RS > {c['rs_min']}")

    return f"""<!DOCTYPE html>
<html><body style="background:#0a0a0f;color:#e8e8f0;font-family:sans-serif;padding:28px;max-width:900px;margin:auto">
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

  {section('⭐ Strong signals', '#22c55e', strong)}
  {section('◑ Medium signals', '#f59e0b', medium)}

  <div style="margin-top:28px;padding-top:14px;border-top:1px solid #1e1e27;font-size:11px;color:#333">
    Data: Yahoo Finance · RS = 52-week range position · n/a = no data (stock passes through)
  </div>
</body></html>"""

# ── Send email ────────────────────────────────────────────────────
def send_email(results):
    now = datetime.now().strftime('%Y-%m-%d')
    strong_count = sum(1 for r in results if r['signal'] == 'Strong')

    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"📈 Screener {now} — {len(results)} matches ({strong_count} strong)"
    msg['From']    = GMAIL_ADDRESS
    msg['To']      = SEND_TO
    msg.attach(MIMEText(build_email(results), 'html'))

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as s:
        s.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        s.sendmail(GMAIL_ADDRESS, SEND_TO, msg.as_string())

    print(f"✅ Email sent to {SEND_TO}")

# ── Entry point ───────────────────────────────────────────────────
if __name__ == '__main__':
    results = run()
    if GMAIL_ADDRESS != "your.gmail@gmail.com":
        send_email(results)
    else:
        print("⚠️  Set GMAIL_ADDRESS, GMAIL_APP_PASS and SEND_TO to receive emails.")
        print(f"   Found {len(results)} matches — configure secrets to get the email report.")

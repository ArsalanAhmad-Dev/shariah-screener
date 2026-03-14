"""
NSE Shariah-Compliant Stock Screener v4 — With Scoring System
=============================================================
Scoring (100 points total):
  PE Discount     25pts  — how cheap vs history
  ROE             20pts  — business quality
  Free Cash Flow  20pts  — real profitability
  D/E Ratio       15pts  — financial safety
  Revenue Growth  10pts  — momentum
  RSI             10pts  — entry timing

Verdicts:
  80-100 → ⭐ Top Pick
  60-79  → 🟢 Strong
  40-59  → 🟡 Moderate
  0-39   → 🔴 Weak

Schedule: Daily 4:30 PM IST (11:00 UTC) via GitHub Actions
"""

import os
import time
import logging
import requests
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
TELEGRAM_TOKEN   = "8536746285:AAFqZg7j39T-4RcQM9Z-7ZcTn9y2juuqRMY"
TELEGRAM_CHAT_ID = "835287998"
GOOGLE_SHEET_ID  = os.environ.get("GOOGLE_SHEET_ID", "1VEt5UTJi8owOFA5fbmZd42a7n59sanOVPDLIscLe4Kw")
CREDENTIALS_FILE = "credentials.json"

# Filter thresholds
MAX_DE_RATIO       = 0.33
MIN_REVENUE_GROWTH = 0.0
PE_DISCOUNT_PCT    = 0.20
PRICE_DROP_PCT     = 0.20

# Scoring thresholds
ROE_EXCELLENT  = 25    # ROE >= 25% → full points
ROE_GOOD       = 15    # ROE >= 15% → partial
FCF_EXCELLENT  = 1e9   # FCF >= 1B → full points
RSI_OVERSOLD   = 35    # RSI <= 35 → full points (deeply oversold)
RSI_CHEAP      = 45    # RSI <= 45 → partial

# Sheet tabs
TAB_FUNDAMENTALS = "Fundamentals"
TAB_RESULTS      = "Results"
TAB_ALERTS_LOG   = "Alerts Log"

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("screener.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════
# 1. HALAL STOCK LIST
# ══════════════════════════════════════════════
def get_halal_list() -> list[str]:
    symbols = [
        "AARTIIND","ABBOTINDIA","AEGISCHEM","AIAENG","AJANTPHARM",
        "ALKYLAMINE","ALKEM","APLAPOLLO","APLLTD","ASIANPAINT",
        "ASTRAL","ATUL","AUROBINDO","AVANTIFEED","BALKRISIND",
        "BAYERCROP","BERGEPAINT","BRITANNIA","CAPLIPOINT","CDSL",
        "CIPLA","COFORGE","COLPAL","CRISIL","DABUR",
        "DEEPAKFERT","DEEPAKNTR","DIXON","DRREDDY","EIDPARRY",
        "ELGIEQUIP","EMAMILTD","ERIS","FINEORG","FLUOROCHEM",
        "GALAXYSURF","GILLETTE","GLAXO","GODREJCP","GODREJIND",
        "GRANULES","HAPPSTMNDS","HCLTECH","HINDUNILVR","HONAUT",
        "INDIAMART","INFY","INTELLECT","IPCALAB","JBCHEPHARM",
        "JUBLFOOD","KANSAINER","KPITTECH","KRBL","LALPATHLAB",
        "LAURUSLABS","LTIM","LTTS","LUPIN","MARICO",
        "MASTEK","METROPOLIS","MPHASIS","NATCOPHARM","NAUKRI",
        "NAVINFLUOR","NESTLEIND","OFSS","PAGEIND","PERSISTENT",
        "PFIZER","PIDILITIND","PIIND","POLYCAB","RELAXO",
        "ROSSARI","SEQUENT","SHREECEM","SIEMENS","SOLARA",
        "SONACOMS","SUDARSCHEM","SUNPHARMA","SYMPHONY","TATACONSUM",
        "TATAELXSI","TCS","TECHM","THYROCARE","TRENT",
        "ULTRACEMCO","VINATIORGA","WIPRO","ZYDUSLIFE"
    ]
    log.info(f"Halal universe: {len(symbols)} stocks")
    return sorted(symbols)


# ══════════════════════════════════════════════
# 2. YFINANCE DATA FETCH
# ══════════════════════════════════════════════
def get_stock_data(symbol: str) -> dict | None:
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        info   = ticker.info

        # ── Price ─────────────────────────────────────────────────
        current_price = info.get("currentPrice") or info.get("regularMarketPrice")
        high_52w      = info.get("fiftyTwoWeekHigh")
        if not current_price or not high_52w:
            return None

        pct_below_high = (high_52w - current_price) / high_52w

        # ── Fundamentals ──────────────────────────────────────────
        de_ratio       = info.get("debtToEquity")
        revenue_growth = info.get("revenueGrowth")
        pe_current     = info.get("trailingPE")
        roe            = info.get("returnOnEquity")       # decimal e.g. 0.28
        fcf            = info.get("freeCashflow")         # absolute INR
        market_cap     = info.get("marketCap")

        # Normalise D/E (yfinance returns as %, e.g. 45 = 0.45)
        if de_ratio is not None:
            de_ratio = de_ratio / 100

        # ── 5yr average PE ────────────────────────────────────────
        pe_5yr_avg = _calc_5yr_avg_pe(ticker, symbol)

        # ── RSI (14-day, weekly closes) ───────────────────────────
        rsi = _calc_rsi(ticker, symbol)

        # ── Historical price data for context ─────────────────────
        hist = ticker.history(period="1y", interval="1d")
        low_52w = float(hist["Low"].min()) if not hist.empty else None

        return {
            "current_price":  round(current_price, 2),
            "high_52w":       round(high_52w, 2),
            "low_52w":        round(low_52w, 2) if low_52w else None,
            "pct_below_high": round(pct_below_high, 4),
            "de_ratio":       round(de_ratio, 3) if de_ratio is not None else None,
            "revenue_growth": round(revenue_growth * 100, 2) if revenue_growth is not None else None,
            "pe_current":     round(pe_current, 2) if pe_current else None,
            "pe_5yr_avg":     pe_5yr_avg,
            "roe":            round(roe * 100, 2) if roe is not None else None,
            "fcf":            fcf,
            "fcf_cr":         round(fcf / 1e7, 1) if fcf else None,  # in Crores
            "market_cap_cr":  round(market_cap / 1e7, 0) if market_cap else None,
            "rsi":            rsi,
        }

    except Exception as e:
        log.warning(f"{symbol}: fetch error — {e}")
        return None


def _calc_5yr_avg_pe(ticker: yf.Ticker, symbol: str) -> float | None:
    try:
        hist = ticker.history(period="5y", interval="3mo")
        if hist.empty:
            return None

        financials = ticker.quarterly_financials
        if financials is not None and not financials.empty:
            if "Net Income" in financials.index:
                net_income_q = financials.loc["Net Income"].dropna()
                shares = ticker.info.get("sharesOutstanding")
                if shares and shares > 0:
                    annual_eps = net_income_q.rolling(4).sum() / shares
                    annual_eps = annual_eps.dropna()
                    pe_vals = []
                    for dt, eps in annual_eps.items():
                        if eps <= 0:
                            continue
                        try:
                            price = hist["Close"].asof(dt)
                            if price and price > 0:
                                pe = price / eps
                                if 0 < pe < 200:
                                    pe_vals.append(pe)
                        except Exception:
                            continue
                    if len(pe_vals) >= 2:
                        return round(sum(pe_vals) / len(pe_vals), 2)

        # Fallback
        eps = ticker.info.get("trailingEps")
        if eps and eps > 0 and not hist.empty:
            pe_hist = (hist["Close"] / eps).dropna()
            pe_hist = pe_hist[pe_hist.between(1, 300)]
            if len(pe_hist) >= 4:
                return round(pe_hist.mean(), 2)

        return None
    except Exception as e:
        log.debug(f"{symbol}: 5yr PE error — {e}")
        return None


def _calc_rsi(ticker: yf.Ticker, symbol: str, period: int = 14) -> float | None:
    """Calculates 14-period RSI from daily price history."""
    try:
        hist = ticker.history(period="3mo", interval="1d")
        if hist.empty or len(hist) < period + 1:
            return None

        closes = hist["Close"].dropna()
        delta  = closes.diff()
        gain   = delta.clip(lower=0)
        loss   = (-delta).clip(lower=0)

        avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
        avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()

        rs  = avg_gain / avg_loss.replace(0, float('nan'))
        rsi = 100 - (100 / (1 + rs))

        val = round(float(rsi.iloc[-1]), 1)
        return val if 0 <= val <= 100 else None

    except Exception as e:
        log.debug(f"{symbol}: RSI error — {e}")
        return None


# ══════════════════════════════════════════════
# 3. SCORING ENGINE
# ══════════════════════════════════════════════
def calculate_score(data: dict, filters: dict) -> dict:
    """
    Scores a stock out of 100 across 6 weighted dimensions.
    Returns score breakdown and verdict.
    """
    scores = {}

    # ── PE Discount (25pts) ───────────────────────────────────────
    pe_disc = filters.get("pe_discount_pct")
    if pe_disc is not None:
        if pe_disc >= 40:   scores["pe"]  = 25
        elif pe_disc >= 30: scores["pe"]  = 20
        elif pe_disc >= 20: scores["pe"]  = 15
        elif pe_disc >= 10: scores["pe"]  = 8
        else:               scores["pe"]  = 0
    else:
        scores["pe"] = 0

    # ── ROE (20pts) ───────────────────────────────────────────────
    roe = data.get("roe")
    if roe is not None:
        if roe >= 30:   scores["roe"] = 20
        elif roe >= 20: scores["roe"] = 16
        elif roe >= 15: scores["roe"] = 12
        elif roe >= 10: scores["roe"] = 6
        else:           scores["roe"] = 0
    else:
        scores["roe"] = 0

    # ── Free Cash Flow (20pts) ────────────────────────────────────
    fcf = data.get("fcf")
    if fcf is not None:
        if fcf >= 50e9:    scores["fcf"] = 20   # > 5000 Cr
        elif fcf >= 10e9:  scores["fcf"] = 16   # > 1000 Cr
        elif fcf >= 1e9:   scores["fcf"] = 12   # > 100 Cr
        elif fcf > 0:      scores["fcf"] = 6    # positive
        else:              scores["fcf"] = 0    # negative FCF
    else:
        scores["fcf"] = 0

    # ── D/E Ratio (15pts) ─────────────────────────────────────────
    de = data.get("de_ratio")
    if de is not None:
        if de <= 0.05:   scores["de"] = 15
        elif de <= 0.10: scores["de"] = 12
        elif de <= 0.20: scores["de"] = 9
        elif de <= 0.33: scores["de"] = 5
        else:            scores["de"] = 0
    else:
        scores["de"] = 0

    # ── Revenue Growth (10pts) ────────────────────────────────────
    rg = data.get("revenue_growth")
    if rg is not None:
        if rg >= 20:    scores["revenue"] = 10
        elif rg >= 10:  scores["revenue"] = 8
        elif rg >= 5:   scores["revenue"] = 5
        elif rg > 0:    scores["revenue"] = 2
        else:           scores["revenue"] = 0
    else:
        scores["revenue"] = 0

    # ── RSI (10pts) ───────────────────────────────────────────────
    rsi = data.get("rsi")
    if rsi is not None:
        if rsi <= 30:   scores["rsi"] = 10   # deeply oversold
        elif rsi <= 40: scores["rsi"] = 8    # oversold
        elif rsi <= 50: scores["rsi"] = 5    # neutral-low
        elif rsi <= 60: scores["rsi"] = 2    # neutral
        else:           scores["rsi"] = 0    # overbought
    else:
        scores["rsi"] = 0

    total = sum(scores.values())

    if total >= 80:   verdict = "⭐ Top Pick"
    elif total >= 60: verdict = "🟢 Strong"
    elif total >= 40: verdict = "🟡 Moderate"
    else:             verdict = "🔴 Weak"

    return {
        "score_total":   total,
        "score_pe":      scores["pe"],
        "score_roe":     scores["roe"],
        "score_fcf":     scores["fcf"],
        "score_de":      scores["de"],
        "score_revenue": scores["revenue"],
        "score_rsi":     scores["rsi"],
        "verdict":       verdict,
    }


# ══════════════════════════════════════════════
# 4. SCREENING LOGIC
# ══════════════════════════════════════════════
def screen_stock(symbol: str) -> dict:
    result = {
        "symbol":          symbol,
        "timestamp":       datetime.now().strftime("%Y-%m-%d %H:%M"),
        "current_price":   None, "high_52w": None, "low_52w": None,
        "pct_below_high":  None, "de_ratio": None, "revenue_growth": None,
        "pe_current":      None, "pe_5yr_avg": None, "pe_discount_pct": None,
        "roe":             None, "fcf_cr": None, "market_cap_cr": None, "rsi": None,
        "pass_price":      False, "pass_de": False,
        "pass_revenue":    False, "pass_pe": False,
        "all_pass":        False,
        "score_total":     0, "score_pe": 0, "score_roe": 0,
        "score_fcf":       0, "score_de": 0, "score_revenue": 0, "score_rsi": 0,
        "verdict":         "🔴 Weak",
        "error":           None,
    }

    data = get_stock_data(symbol)
    if not data:
        result["error"] = "data_unavailable"
        return result

    result.update({k: data[k] for k in data if k in result or k not in result})
    result["roe"]          = data.get("roe")
    result["fcf_cr"]       = data.get("fcf_cr")
    result["market_cap_cr"]= data.get("market_cap_cr")
    result["rsi"]          = data.get("rsi")
    result["low_52w"]      = data.get("low_52w")

    # ── Filters ───────────────────────────────────────────────────
    result["pass_price"]   = data["pct_below_high"] >= PRICE_DROP_PCT
    result["pass_de"]      = (data["de_ratio"] is not None) and (data["de_ratio"] < MAX_DE_RATIO)
    result["pass_revenue"] = (data["revenue_growth"] is not None) and (data["revenue_growth"] > MIN_REVENUE_GROWTH)

    pe_cur = data.get("pe_current")
    pe_avg = data.get("pe_5yr_avg")
    if pe_cur and pe_avg and pe_avg > 0:
        discount = (pe_avg - pe_cur) / pe_avg
        result["pe_discount_pct"] = round(discount * 100, 2)
        result["pass_pe"]         = discount >= PE_DISCOUNT_PCT
    else:
        result["pe_discount_pct"] = None
        result["pass_pe"]         = False

    result["all_pass"] = all([
        result["pass_price"], result["pass_de"],
        result["pass_revenue"], result["pass_pe"],
    ])

    # ── Scoring (all stocks, not just passed) ─────────────────────
    scoring = calculate_score(data, result)
    result.update(scoring)

    return result


# ══════════════════════════════════════════════
# 5. GOOGLE SHEETS
# ══════════════════════════════════════════════
def update_google_sheets(all_results: list[dict], passed: list[dict]):
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        sheet = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)

        # ── Fundamentals tab ──────────────────────────────────────
        ws = sheet.worksheet(TAB_FUNDAMENTALS)
        ws.clear()
        headers = [
            "Symbol", "Timestamp", "Score /100", "Verdict",
            "Current Price (₹)", "52W High (₹)", "52W Low (₹)", "% Below High",
            "D/E Ratio", "Revenue Growth (%)", "PE Current", "PE 5yr Avg", "PE Discount (%)",
            "ROE (%)", "FCF (₹ Cr)", "Market Cap (₹ Cr)", "RSI",
            "Pass Price", "Pass D/E", "Pass Revenue", "Pass PE", "All Pass",
            "Score PE", "Score ROE", "Score FCF", "Score D/E", "Score Revenue", "Score RSI",
            "Error",
        ]
        rows = [headers]
        for r in all_results:
            rows.append([
                r["symbol"], r["timestamp"], r["score_total"], r["verdict"],
                r["current_price"], r["high_52w"], r.get("low_52w"),
                f"{r['pct_below_high']*100:.1f}%" if r["pct_below_high"] else "",
                r["de_ratio"], r["revenue_growth"], r["pe_current"],
                r["pe_5yr_avg"], r["pe_discount_pct"],
                r.get("roe"), r.get("fcf_cr"), r.get("market_cap_cr"), r.get("rsi"),
                str(r["pass_price"]), str(r["pass_de"]),
                str(r["pass_revenue"]), str(r["pass_pe"]), str(r["all_pass"]),
                r["score_pe"], r["score_roe"], r["score_fcf"],
                r["score_de"], r["score_revenue"], r["score_rsi"],
                r.get("error", ""),
            ])
        ws.update(rows, value_input_option="USER_ENTERED")
        log.info(f"Fundamentals tab: {len(all_results)} rows written.")

        # ── Results tab (passed + sorted by score) ────────────────
        ws = sheet.worksheet(TAB_RESULTS)
        ws.clear()
        res_headers = [
            "Rank", "Symbol", "Score /100", "Verdict", "Timestamp",
            "Current Price (₹)", "52W High (₹)", "% Below High",
            "D/E Ratio", "Revenue Growth (%)", "PE Current", "PE 5yr Avg", "PE Discount (%)",
            "ROE (%)", "FCF (₹ Cr)", "RSI",
        ]
        res_rows = [res_headers]
        sorted_passed = sorted(passed, key=lambda x: x["score_total"], reverse=True)
        for rank, r in enumerate(sorted_passed, 1):
            res_rows.append([
                rank, r["symbol"], r["score_total"], r["verdict"], r["timestamp"],
                r["current_price"], r["high_52w"],
                f"{r['pct_below_high']*100:.1f}%" if r["pct_below_high"] else "",
                r["de_ratio"], r["revenue_growth"], r["pe_current"],
                r["pe_5yr_avg"], r["pe_discount_pct"],
                r.get("roe"), r.get("fcf_cr"), r.get("rsi"),
            ])
        ws.update(res_rows, value_input_option="USER_ENTERED")
        log.info(f"Results tab: {len(passed)} stocks ranked by score.")

        # ── Alerts Log ────────────────────────────────────────────
        ws    = sheet.worksheet(TAB_ALERTS_LOG)
        today = date.today().isoformat()
        log_rows = []
        for r in sorted_passed:
            log_rows.append([
                today, r["symbol"], r["score_total"], r["verdict"],
                r["current_price"],
                f"{r['pct_below_high']*100:.1f}% below 52W high",
                f"ROE {r.get('roe')}%", f"FCF ₹{r.get('fcf_cr')}Cr",
                f"RSI {r.get('rsi')}",
            ])
        if log_rows:
            ws.append_rows(log_rows, value_input_option="USER_ENTERED")
            log.info(f"Alerts Log: {len(log_rows)} rows appended.")

    except Exception as e:
        log.error(f"Google Sheets update failed: {e}")


# ══════════════════════════════════════════════
# 6. TELEGRAM
# ══════════════════════════════════════════════
def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"},
            timeout=15,
        )
        r.raise_for_status()
    except Exception as e:
        log.error(f"Telegram send failed: {e}")


def send_alerts(passed: list[dict], total: int):
    today  = date.today().strftime("%d %b %Y")
    sorted_passed = sorted(passed, key=lambda x: x["score_total"], reverse=True)

    if not passed:
        send_telegram(
            f"🕌 *Shariah Screener — {today}*\n\n"
            f"Screened {total} halal stocks.\n"
            "No stocks passed all filters today."
        )
        return

    send_telegram(
        f"🕌 *Shariah Screener — {today}*\n"
        f"Screened {total} stocks • *{len(passed)} passed* • ranked by score\n"
        "─────────────────────"
    )

    for r in sorted_passed:
        drop = r["pct_below_high"] * 100 if r["pct_below_high"] else 0
        send_telegram(
            f"{r['verdict']} *{r['symbol']}*  —  *{r['score_total']}/100*\n"
            f"💰 ₹{r['current_price']} ({drop:.1f}% below 52W high)\n"
            f"📊 D/E: {r['de_ratio']} | ROE: {r.get('roe')}% | FCF: ₹{r.get('fcf_cr')}Cr\n"
            f"📉 PE disc: {r['pe_discount_pct']}% | RSI: {r.get('rsi')}\n"
            f"📈 Rev Growth: {r['revenue_growth']}%"
        )
        time.sleep(0.5)


# ══════════════════════════════════════════════
# 7. MAIN
# ══════════════════════════════════════════════
def run_screener():
    log.info("═" * 60)
    log.info("NSE Shariah Screener v4 (Scoring) — starting run")
    log.info("═" * 60)

    halal_symbols = get_halal_list()
    all_results   = []
    passed        = []

    for i, symbol in enumerate(halal_symbols, 1):
        log.info(f"[{i}/{len(halal_symbols)}] {symbol}")
        try:
            result = screen_stock(symbol)
            all_results.append(result)
            if result["all_pass"]:
                passed.append(result)
                log.info(f"  ✅ PASSED: {symbol} — {result['score_total']}/100 {result['verdict']}")
            time.sleep(0.5)
        except Exception as e:
            log.error(f"  ❌ Error [{symbol}]: {e}")
            all_results.append({
                "symbol": symbol, "error": str(e), "all_pass": False,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "score_total": 0, "verdict": "🔴 Weak",
                **{k: None for k in [
                    "de_ratio","revenue_growth","pe_current","pe_5yr_avg",
                    "pe_discount_pct","current_price","high_52w","low_52w",
                    "pct_below_high","roe","fcf_cr","market_cap_cr","rsi",
                ]},
                **{k: False for k in ["pass_de","pass_revenue","pass_pe","pass_price"]},
                **{k: 0 for k in ["score_pe","score_roe","score_fcf","score_de","score_revenue","score_rsi"]},
            })

    log.info(f"Done. {len(passed)}/{len(all_results)} passed. Top score: {max((r['score_total'] for r in passed), default=0)}/100")

    update_google_sheets(all_results, passed)
    send_alerts(passed, len(all_results))

    log.info("Run complete ✓")
    return passed


if __name__ == "__main__":
    run_screener()

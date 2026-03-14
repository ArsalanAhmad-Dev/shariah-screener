"""
NSE Shariah-Compliant Stock Screener (yfinance-Only Architecture)
=================================================================
Flow:
  1. Load hardcoded Shariah-compliant halal stock list
  2. Fetch ALL data via yfinance (price + fundamentals)
  3. Apply filters
  4. Update Google Sheets dashboard
  5. Send Telegram alerts

Filters:
  ✓ Debt/Equity < 0.33
  ✓ Revenue growth positive (yfinance revenueGrowth)
  ✓ PE ≥ 20% below 5-year average PE
  ✓ Price ≥ 20% below 52-week high

Schedule: Daily 4:30 PM IST (11:00 UTC) via GitHub Actions

Dependencies:
  pip install yfinance requests gspread google-auth pandas
"""

import os
import time
import logging
import requests
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
MAX_DE_RATIO       = 0.33   # Debt/Equity < 0.33
MIN_REVENUE_GROWTH = 0.0    # Revenue growth > 0%
PE_DISCOUNT_PCT    = 0.20   # PE ≥ 20% below 5yr average
PRICE_DROP_PCT     = 0.20   # Price ≥ 20% below 52W high

# Sheet tab names
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
    """
    Fetches all required data for a stock via yfinance.
    Returns dict with price, fundamentals, and 5yr avg PE.
    """
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        info   = ticker.info

        # ── Price data ────────────────────────────────────────────
        current_price = info.get("currentPrice") or info.get("regularMarketPrice")
        high_52w      = info.get("fiftyTwoWeekHigh")

        if not current_price or not high_52w:
            log.warning(f"{symbol}: missing price data")
            return None

        pct_below_high = (high_52w - current_price) / high_52w

        # ── Fundamental data ──────────────────────────────────────
        de_ratio       = info.get("debtToEquity")
        revenue_growth = info.get("revenueGrowth")      # yfinance: YoY %
        pe_current     = info.get("trailingPE")

        # Normalise D/E — yfinance returns it as percentage (e.g. 45 = 0.45)
        if de_ratio is not None:
            de_ratio = de_ratio / 100

        # ── 5-year average PE ─────────────────────────────────────
        pe_5yr_avg = _calc_5yr_avg_pe(ticker, symbol)

        return {
            "current_price":  round(current_price, 2),
            "high_52w":       round(high_52w, 2),
            "pct_below_high": round(pct_below_high, 4),
            "de_ratio":       round(de_ratio, 3) if de_ratio is not None else None,
            "revenue_growth": round(revenue_growth * 100, 2) if revenue_growth is not None else None,
            "pe_current":     round(pe_current, 2) if pe_current else None,
            "pe_5yr_avg":     pe_5yr_avg,
        }

    except Exception as e:
        log.warning(f"{symbol}: data fetch error — {e}")
        return None


def _calc_5yr_avg_pe(ticker: yf.Ticker, symbol: str) -> float | None:
    """
    Calculates 5-year average PE from annual earnings history.
    Method: for each past year, PE = price at year-end / EPS that year.
    """
    try:
        # Get 5 years of annual price history
        hist = ticker.history(period="5y", interval="3mo")
        if hist.empty:
            return None

        # Get earnings (EPS) history
        earnings = ticker.income_stmt
        if earnings is None or earnings.empty:
            return None

        # Find net income and shares to compute EPS per year
        pe_estimates = []

        # Simpler approach: use trailing PE from each quarterly snapshot
        # by looking at price / (annual EPS rolled over quarters)
        financials = ticker.quarterly_financials
        if financials is not None and not financials.empty:
            # Get annual EPS approximations from quarterly data
            if "Net Income" in financials.index:
                net_income_q = financials.loc["Net Income"].dropna()
                shares = ticker.info.get("sharesOutstanding")
                if shares and shares > 0:
                    # Rolling 4-quarter sum = annual net income
                    annual_eps_series = net_income_q.rolling(4).sum() / shares
                    annual_eps_series = annual_eps_series.dropna()

                    for dt, eps in annual_eps_series.items():
                        if eps <= 0:
                            continue
                        # Find price closest to that date
                        try:
                            price_at_date = hist["Close"].asof(dt)
                            if price_at_date and price_at_date > 0:
                                pe = price_at_date / eps
                                if 0 < pe < 200:  # sanity check
                                    pe_estimates.append(pe)
                        except Exception:
                            continue

        if len(pe_estimates) >= 2:
            avg = sum(pe_estimates) / len(pe_estimates)
            return round(avg, 2)

        # Fallback: use 5yr median of trailing PE approximation
        # price / current EPS scaled back with price ratio
        pe_current = ticker.info.get("trailingPE")
        eps        = ticker.info.get("trailingEps")
        if pe_current and eps and eps > 0 and not hist.empty:
            current_price = hist["Close"].iloc[-1]
            pe_history = (hist["Close"] / eps).dropna()
            pe_history = pe_history[pe_history.between(1, 300)]
            if len(pe_history) >= 4:
                return round(pe_history.mean(), 2)

        return None

    except Exception as e:
        log.debug(f"{symbol}: 5yr PE calc error — {e}")
        return None


# ══════════════════════════════════════════════
# 3. SCREENING LOGIC
# ══════════════════════════════════════════════
def screen_stock(symbol: str) -> dict:
    """Applies all 4 filters to a single halal-listed stock."""
    result = {
        "symbol":          symbol,
        "timestamp":       datetime.now().strftime("%Y-%m-%d %H:%M"),
        "current_price":   None,
        "high_52w":        None,
        "pct_below_high":  None,
        "de_ratio":        None,
        "revenue_growth":  None,
        "pe_current":      None,
        "pe_5yr_avg":      None,
        "pe_discount_pct": None,
        "pass_price":      False,
        "pass_de":         False,
        "pass_revenue":    False,
        "pass_pe":         False,
        "all_pass":        False,
        "error":           None,
    }

    data = get_stock_data(symbol)
    if not data:
        result["error"] = "data_unavailable"
        return result

    # Populate result
    result.update({k: data[k] for k in data})

    # ── Filter 1: Price ≥ 20% below 52W high ─────────────────────
    result["pass_price"] = data["pct_below_high"] >= PRICE_DROP_PCT

    # ── Filter 2: D/E < 0.33 ─────────────────────────────────────
    de = data["de_ratio"]
    result["pass_de"] = (de is not None) and (de < MAX_DE_RATIO)

    # ── Filter 3: Revenue growth > 0% ────────────────────────────
    rg = data["revenue_growth"]
    result["pass_revenue"] = (rg is not None) and (rg > MIN_REVENUE_GROWTH)

    # ── Filter 4: PE ≥ 20% below 5yr average ─────────────────────
    pe_cur = data["pe_current"]
    pe_avg = data["pe_5yr_avg"]
    if pe_cur and pe_avg and pe_avg > 0:
        discount = (pe_avg - pe_cur) / pe_avg
        result["pe_discount_pct"] = round(discount * 100, 2)
        result["pass_pe"]         = discount >= PE_DISCOUNT_PCT
    else:
        result["pe_discount_pct"] = None
        result["pass_pe"]         = False

    result["all_pass"] = all([
        result["pass_price"],
        result["pass_de"],
        result["pass_revenue"],
        result["pass_pe"],
    ])

    return result


# ══════════════════════════════════════════════
# 4. GOOGLE SHEETS
# ══════════════════════════════════════════════
def update_google_sheets(all_results: list[dict], passed: list[dict]):
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        sheet  = gspread.authorize(creds).open_by_key(GOOGLE_SHEET_ID)

        # ── Fundamentals tab ──────────────────────────────────────
        ws = sheet.worksheet(TAB_FUNDAMENTALS)
        ws.clear()
        headers = [
            "Symbol", "Timestamp", "Current Price (₹)", "52W High (₹)",
            "% Below High", "D/E Ratio", "Revenue Growth (%)",
            "PE Current", "PE 5yr Avg", "PE Discount (%)",
            "Pass Price", "Pass D/E", "Pass Revenue", "Pass PE", "All Pass", "Error",
        ]
        rows = [headers]
        for r in all_results:
            rows.append([
                r["symbol"], r["timestamp"], r["current_price"], r["high_52w"],
                f"{r['pct_below_high']*100:.1f}%" if r["pct_below_high"] else "",
                r["de_ratio"], r["revenue_growth"],
                r["pe_current"], r["pe_5yr_avg"], r["pe_discount_pct"],
                r["pass_price"], r["pass_de"], r["pass_revenue"], r["pass_pe"],
                r["all_pass"], r.get("error", ""),
            ])
        ws.update(rows, value_input_option="USER_ENTERED")
        log.info(f"Fundamentals tab: {len(all_results)} rows written.")

        # ── Results tab ───────────────────────────────────────────
        ws = sheet.worksheet(TAB_RESULTS)
        ws.clear()
        res_headers = [
            "Symbol", "Timestamp", "Current Price (₹)", "52W High (₹)",
            "% Below High", "D/E Ratio", "Revenue Growth (%)",
            "PE Current", "PE 5yr Avg", "PE Discount (%)",
        ]
        res_rows = [res_headers]
        for r in passed:
            res_rows.append([
                r["symbol"], r["timestamp"], r["current_price"], r["high_52w"],
                f"{r['pct_below_high']*100:.1f}%" if r["pct_below_high"] else "",
                r["de_ratio"], r["revenue_growth"],
                r["pe_current"], r["pe_5yr_avg"], r["pe_discount_pct"],
            ])
        ws.update(res_rows, value_input_option="USER_ENTERED")
        log.info(f"Results tab: {len(passed)} qualifying stocks.")

        # ── Alerts Log tab (append) ───────────────────────────────
        ws    = sheet.worksheet(TAB_ALERTS_LOG)
        today = date.today().isoformat()
        log_rows = []
        for r in passed:
            log_rows.append([
                today, r["symbol"], r["current_price"],
                f"{r['pct_below_high']*100:.1f}% below 52W high",
                f"D/E {r['de_ratio']}",
                f"Rev Growth {r['revenue_growth']}%",
                f"PE disc {r['pe_discount_pct']}%",
            ])
        if log_rows:
            ws.append_rows(log_rows, value_input_option="USER_ENTERED")
            log.info(f"Alerts Log: {len(log_rows)} rows appended.")

    except Exception as e:
        log.error(f"Google Sheets update failed: {e}")


# ══════════════════════════════════════════════
# 5. TELEGRAM
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
    today = date.today().strftime("%d %b %Y")

    if not passed:
        send_telegram(
            f"🕌 *Shariah Screener — {today}*\n\n"
            f"Screened {total} halal-listed stocks.\n"
            "No stocks passed all filters today."
        )
        return

    send_telegram(
        f"🕌 *Shariah Screener — {today}*\n"
        f"Screened {total} stocks • *{len(passed)} passed all filters*\n"
        "─────────────────────"
    )

    for r in passed:
        drop = r["pct_below_high"] * 100 if r["pct_below_high"] else 0
        send_telegram(
            f"✅ *{r['symbol']}*\n"
            f"💰 ₹{r['current_price']}  ({drop:.1f}% below 52W high of ₹{r['high_52w']})\n"
            f"📊 D/E: {r['de_ratio']}  |  Rev Growth: {r['revenue_growth']}%\n"
            f"📉 PE: {r['pe_current']} vs 5yr avg {r['pe_5yr_avg']} "
            f"({r['pe_discount_pct']}% discount)\n"
            f"☪️ Shariah: Verified (curated list)"
        )
        time.sleep(0.5)


# ══════════════════════════════════════════════
# 6. MAIN
# ══════════════════════════════════════════════
def run_screener():
    log.info("═" * 60)
    log.info("NSE Shariah Screener (yfinance) — starting run")
    log.info("═" * 60)

    halal_symbols = get_halal_list()

    all_results = []
    passed      = []

    for i, symbol in enumerate(halal_symbols, 1):
        log.info(f"[{i}/{len(halal_symbols)}] {symbol}")
        try:
            result = screen_stock(symbol)
            all_results.append(result)
            if result["all_pass"]:
                passed.append(result)
                log.info(f"  ✅ PASSED: {symbol}")
            # Small delay to avoid yfinance rate limiting
            time.sleep(0.5)
        except Exception as e:
            log.error(f"  ❌ Error [{symbol}]: {e}")
            all_results.append({
                "symbol": symbol, "error": str(e), "all_pass": False,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                **{k: None for k in [
                    "de_ratio","revenue_growth","pe_current","pe_5yr_avg",
                    "pe_discount_pct","current_price","high_52w","pct_below_high",
                ]},
                **{k: False for k in ["pass_de","pass_revenue","pass_pe","pass_price"]},
            })

    log.info(f"Done. {len(passed)}/{len(all_results)} passed all filters.")

    update_google_sheets(all_results, passed)
    send_alerts(passed, len(all_results))

    log.info("Run complete ✓")
    return passed


if __name__ == "__main__":
    run_screener()

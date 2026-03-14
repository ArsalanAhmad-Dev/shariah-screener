"""
NSE Shariah-Compliant Stock Screener (Zamzam-First Architecture)
=================================================================
Flow:
  1. Scrape Zamzam Capital halal list  →  cache to zamzam_cache.json
  2. Screen ONLY those stocks for fundamentals + price filters
  3. Update Google Sheets dashboard
  4. Send Telegram alerts

Zamzam scrape failure handling:
  - Uses last cached list (zamzam_cache.json) if available
  - Sends a Telegram warning either way
  - Aborts if no cache exists either

Filters:
  ✓ Debt/Equity < 0.33
  ✓ Revenue CAGR (3yr) > 0%
  ✓ PE ≥ 20% below 5-year average
  ✓ Price ≥ 20% below 52-week high

Schedule: Daily 4:30 PM IST (11:00 UTC) on PythonAnywhere free tier

Dependencies:
  pip install yfinance requests gspread google-auth beautifulsoup4 pandas lxml
"""

import os
import json
import time
import logging
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, date
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
TELEGRAM_TOKEN   = "8536746285:AAFqZg7j39T-4RcQM9Z-7ZcTn9y2juuqRMY"
TELEGRAM_CHAT_ID = "835287998"
GOOGLE_SHEET_ID  = os.environ.get("GOOGLE_SHEET_ID", "1VEt5UTJi8owOFA5fbmZd42a7n59sanOVPDLIscLe4Kw")       
CREDENTIALS_FILE = "credentials.json"            
ZAMZAM_CACHE     = "zamzam_cache.json"           

# Filter thresholds
MAX_DE_RATIO       = 0.33   # Debt/Equity must be below this
MIN_REVENUE_GROWTH = 0.0    # 3-yr CAGR must be > 0%
PE_DISCOUNT_PCT    = 0.20   # PE must be ≥ 20% below 5-yr average
PRICE_DROP_PCT     = 0.20   # Price must be ≥ 20% below 52-week high

# Sheet tab names
TAB_FUNDAMENTALS = "Fundamentals"
TAB_RESULTS      = "Results"
TAB_ALERTS_LOG   = "Alerts Log"

SCREENER_DELAY_SECONDS = 2   # Screener.in rate-limit courtesy delay

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
# 1. ZAMZAM CAPITAL — SCRAPE + CACHE
# ══════════════════════════════════════════════
def scrape_zamzam() -> list[str] | None:
    """
    Scrapes Zamzam Capital's Shariah-compliant stock list.
    Returns a list of NSE symbols (uppercase, no .NS suffix), or None on failure.
    """
    urls_to_try = [
        "https://zamzamcapital.in/shariah-compliant-stocks/",
        "https://zamzamcapital.in/halal-stocks/",
        "https://zamzamcapital.in/compliant-stocks/",
        "https://zamzamcapital.in/",
    ]
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    for url in urls_to_try:
        try:
            r = requests.get(url, headers=headers, timeout=25)
            if r.status_code != 200:
                log.warning(f"Zamzam: HTTP {r.status_code} for {url}")
                continue

            soup = BeautifulSoup(r.text, "lxml")
            symbols = set()

            # Strategy 1: scan all tables
            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
                    for cell in row.find_all(["td", "th"]):
                        text = cell.get_text(strip=True).upper()
                        if _is_nse_symbol(text):
                            symbols.add(text)

            # Strategy 2: scan list items, code/span/p tags
            if len(symbols) < 10:
                for tag in soup.find_all(["li", "code", "span", "p", "td"]):
                    text = tag.get_text(strip=True).upper()
                    if _is_nse_symbol(text):
                        symbols.add(text)

            # Strategy 3: scan entire page text word by word (broadest net)
            if len(symbols) < 10:
                for word in soup.get_text(" ").split():
                    word = word.strip(".,;:()[]").upper()
                    if _is_nse_symbol(word):
                        symbols.add(word)

            if len(symbols) >= 10:
                log.info(f"Zamzam scrape: {len(symbols)} symbols from {url}")
                return sorted(symbols)

        except Exception as e:
            log.warning(f"Zamzam scrape error ({url}): {e}")

    return None


def _is_nse_symbol(text: str) -> bool:
    """Heuristic: NSE symbols are 2–20 uppercase alphanumeric chars (allow & -)."""
    if not (2 <= len(text) <= 20):
        return False
    clean = text.replace("&", "").replace("-", "")
    return clean.isalnum() and clean.isupper() and not clean.isdigit()


def save_zamzam_cache(symbols: list[str]):
    cache = {
        "updated": datetime.now().isoformat(),
        "count":   len(symbols),
        "symbols": symbols,
    }
    with open(ZAMZAM_CACHE, "w") as f:
        json.dump(cache, f, indent=2)
    log.info(f"Zamzam cache saved: {len(symbols)} symbols → {ZAMZAM_CACHE}")


def load_zamzam_cache() -> list[str] | None:
    if not os.path.exists(ZAMZAM_CACHE):
        return None
    try:
        with open(ZAMZAM_CACHE) as f:
            cache = json.load(f)
        symbols = cache.get("symbols", [])
        updated = cache.get("updated", "unknown")
        log.info(f"Zamzam cache loaded: {len(symbols)} symbols (cached {updated})")
        return symbols if symbols else None
    except Exception as e:
        log.error(f"Cache load failed: {e}")
        return None


def get_halal_list() -> tuple[list[str], bool]:
    """
    Returns (halal_symbols_list, is_fresh_scrape).
    Handles scrape failure by falling back to cache + sending Telegram warning.
    Aborts (raises RuntimeError) if both scrape and cache fail.
    """
    scraped = scrape_zamzam()

    if scraped:
        save_zamzam_cache(scraped)
        return scraped, True

    # Scrape failed — alert immediately
    log.warning("Zamzam scrape failed. Attempting cache fallback.")
    send_telegram(
        "⚠️ *Shariah Screener Warning*\n\n"
        "Could not scrape Zamzam Capital today.\n"
        "Falling back to last cached halal list.\n"
        "Please verify Zamzam Capital's website manually."
    )

    cached = load_zamzam_cache()
    if cached:
        return cached, False

    # Both failed — abort
    send_telegram(
        "🚨 *Shariah Screener ABORTED*\n\n"
        "Zamzam scrape failed AND no cache found.\n"
        "Screening could not run today. Manual intervention required."
    )
    raise RuntimeError("Zamzam scrape failed and no cache available. Aborting.")


# ══════════════════════════════════════════════
# 2. SCREENER.IN FUNDAMENTALS
# ══════════════════════════════════════════════
def get_screener_fundamentals(symbol: str) -> dict | None:
    """
    Fetches D/E ratio, 3-yr revenue CAGR, current PE, 5-yr avg PE
    from Screener.in for a given NSE symbol (no .NS suffix).
    """
    url = f"https://www.screener.in/company/{symbol}/consolidated/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }

    for attempt in range(2):
        try:
            time.sleep(SCREENER_DELAY_SECONDS)
            r = requests.get(url, headers=headers, timeout=25)

            if r.status_code == 404 and attempt == 0:
                url = f"https://www.screener.in/company/{symbol}/"
                continue
            if r.status_code != 200:
                return None

            soup = BeautifulSoup(r.text, "lxml")

            return {
                "de_ratio":         _parse_ratio(soup, ["Debt / Equity", "D/E Ratio", "Debt to Equity"]),
                "pe_current":       _parse_ratio(soup, ["Stock P/E", "P/E", "Price to Earning"]),
                "pe_5yr_avg":       _parse_pe_5yr_avg(soup),
                "revenue_3yr_cagr": _parse_revenue_cagr(soup),
            }

        except Exception as e:
            log.debug(f"Screener error [{symbol}]: {e}")

    return None


def _parse_ratio(soup: BeautifulSoup, labels: list[str]) -> float | None:
    for li in soup.select("#top-ratios li, .company-ratios li, .ratio li"):
        text = li.get_text(" ", strip=True)
        for label in labels:
            if label.lower() in text.lower():
                for part in reversed(text.split()):
                    try:
                        return float(part.replace(",", "").replace("%", ""))
                    except ValueError:
                        continue
    return None


def _parse_pe_5yr_avg(soup: BeautifulSoup) -> float | None:
    for table in soup.find_all("table"):
        hdr = table.find("tr")
        if not hdr:
            continue
        if "p/e" in hdr.get_text().lower() or "price to earning" in hdr.get_text().lower():
            pe_values = []
            for row in table.find_all("tr")[1:][-5:]:
                for cell in row.find_all("td"):
                    try:
                        val = float(cell.get_text(strip=True).replace(",", ""))
                        if 0 < val < 1000:
                            pe_values.append(val)
                            break
                    except ValueError:
                        continue
            if len(pe_values) >= 3:
                return round(sum(pe_values) / len(pe_values), 2)
    return None


def _parse_revenue_cagr(soup: BeautifulSoup) -> float | None:
    for table in soup.find_all("table"):
        text = table.get_text().lower()
        if "sales" not in text and "revenue" not in text:
            continue
        for row in table.find_all("tr"):
            first = row.find("td") or row.find("th")
            if not first:
                continue
            if "sales" in first.get_text().lower() or "revenue" in first.get_text().lower():
                values = []
                for cell in row.find_all("td")[1:]:
                    try:
                        val = float(cell.get_text(strip=True).replace(",", ""))
                        if val > 0:
                            values.append(val)
                    except ValueError:
                        continue
                if len(values) >= 4:
                    cagr = (values[-1] / values[-4]) ** (1 / 3) - 1
                    return round(cagr, 4)
    return None


# ══════════════════════════════════════════════
# 3. YFINANCE PRICE DATA
# ══════════════════════════════════════════════
def get_price_data(symbol: str) -> dict | None:
    """Fetches current price and 52-week high for SYMBOL.NS via yfinance."""
    try:
        info          = yf.Ticker(f"{symbol}.NS").fast_info
        current_price = getattr(info, "last_price", None)
        high_52w      = getattr(info, "year_high", None)

        if not current_price or not high_52w or high_52w == 0:
            return None

        return {
            "current_price":  round(current_price, 2),
            "high_52w":       round(high_52w, 2),
            "pct_below_high": round((high_52w - current_price) / high_52w, 4),
        }
    except Exception as e:
        log.debug(f"yfinance error [{symbol}]: {e}")
        return None


# ══════════════════════════════════════════════
# 4. SCREENING LOGIC
# ══════════════════════════════════════════════
def screen_stock(symbol: str) -> dict:
    """
    Runs all fundamental + price filters on a single Shariah-listed stock.
    Shariah compliance is pre-guaranteed (stock comes from Zamzam list).
    """
    result = {
        "symbol":           symbol,
        "timestamp":        datetime.now().strftime("%Y-%m-%d %H:%M"),
        "de_ratio":         None,
        "revenue_cagr_3yr": None,
        "pe_current":       None,
        "pe_5yr_avg":       None,
        "pe_discount_pct":  None,
        "current_price":    None,
        "high_52w":         None,
        "pct_below_high":   None,
        "pass_de":          False,
        "pass_revenue":     False,
        "pass_pe":          False,
        "pass_price":       False,
        "all_pass":         False,
        "error":            None,
    }

    # ── Price filter (fast — do first) ───────────────────────────
    price = get_price_data(symbol)
    if not price:
        result["error"] = "price_unavailable"
        return result

    result.update(price)
    result["pass_price"] = price["pct_below_high"] >= PRICE_DROP_PCT

    # ── Fundamentals (Screener.in) ────────────────────────────────
    fund = get_screener_fundamentals(symbol)
    if not fund:
        result["error"] = "fundamentals_unavailable"
        return result

    # D/E
    de = fund.get("de_ratio")
    result["de_ratio"] = de
    result["pass_de"]  = (de is not None) and (de < MAX_DE_RATIO)

    # Revenue CAGR
    cagr = fund.get("revenue_3yr_cagr")
    result["revenue_cagr_3yr"] = round(cagr * 100, 2) if cagr is not None else None
    result["pass_revenue"]     = (cagr is not None) and (cagr > MIN_REVENUE_GROWTH)

    # PE discount
    pe_cur = fund.get("pe_current")
    pe_avg = fund.get("pe_5yr_avg")
    result["pe_current"] = pe_cur
    result["pe_5yr_avg"] = pe_avg
    if pe_cur and pe_avg and pe_avg > 0:
        discount = (pe_avg - pe_cur) / pe_avg
        result["pe_discount_pct"] = round(discount * 100, 2)
        result["pass_pe"]         = discount >= PE_DISCOUNT_PCT

    result["all_pass"] = all([
        result["pass_de"],
        result["pass_revenue"],
        result["pass_pe"],
        result["pass_price"],
    ])

    return result


# ══════════════════════════════════════════════
# 5. GOOGLE SHEETS
# ══════════════════════════════════════════════
def get_sheets_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)


def update_google_sheets(all_results: list[dict], passed: list[dict], is_fresh: bool):
    try:
        sheet = get_sheets_client().open_by_key(GOOGLE_SHEET_ID)

        # ── Fundamentals tab ──────────────────────────────────────
        ws = sheet.worksheet(TAB_FUNDAMENTALS)
        ws.clear()
        headers = [
            "Symbol", "Timestamp", "Current Price (₹)", "52W High (₹)",
            "% Below High", "D/E Ratio", "Revenue CAGR 3yr (%)",
            "PE Current", "PE 5yr Avg", "PE Discount (%)",
            "Pass D/E", "Pass Revenue", "Pass PE", "Pass Price", "All Pass", "Error",
        ]
        rows = [headers]
        for r in all_results:
            rows.append([
                r["symbol"], r["timestamp"], r["current_price"], r["high_52w"],
                r["pct_below_high"], r["de_ratio"], r["revenue_cagr_3yr"],
                r["pe_current"], r["pe_5yr_avg"], r["pe_discount_pct"],
                r["pass_de"], r["pass_revenue"], r["pass_pe"], r["pass_price"],
                r["all_pass"], r.get("error", ""),
            ])
        ws.update(rows, value_input_option="USER_ENTERED")
        log.info(f"Fundamentals tab: {len(all_results)} rows written.")

        # ── Results tab ───────────────────────────────────────────
        ws = sheet.worksheet(TAB_RESULTS)
        ws.clear()
        res_headers = [
            "Symbol", "Timestamp", "Current Price (₹)", "52W High (₹)",
            "% Below High", "D/E Ratio", "Revenue CAGR 3yr (%)",
            "PE Current", "PE 5yr Avg", "PE Discount (%)",
        ]
        res_rows = [res_headers]
        for r in passed:
            res_rows.append([
                r["symbol"], r["timestamp"], r["current_price"], r["high_52w"],
                r["pct_below_high"], r["de_ratio"], r["revenue_cagr_3yr"],
                r["pe_current"], r["pe_5yr_avg"], r["pe_discount_pct"],
            ])
        ws.update(res_rows, value_input_option="USER_ENTERED")
        log.info(f"Results tab: {len(passed)} qualifying stocks.")

        # ── Alerts Log tab (append) ───────────────────────────────
        ws  = sheet.worksheet(TAB_ALERTS_LOG)
        today = date.today().isoformat()
        log_rows = []
        for r in passed:
            log_rows.append([
                today, r["symbol"], r["current_price"],
                f"{r['pct_below_high']*100:.1f}% below 52W high",
                f"D/E {r['de_ratio']}",
                f"Rev CAGR {r['revenue_cagr_3yr']}%",
                f"PE disc {r['pe_discount_pct']}%",
                "Fresh scrape" if is_fresh else "Cached Zamzam list",
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


def send_alerts(passed: list[dict], total: int, is_fresh: bool):
    today      = date.today().strftime("%d %b %Y")
    cache_note = "" if is_fresh else "\n⚠️ _Using cached Zamzam list (scrape failed today)_"

    if not passed:
        send_telegram(
            f"🕌 *Shariah Screener — {today}*{cache_note}\n\n"
            f"Screened {total} halal-listed stocks.\n"
            "No stocks passed all filters today."
        )
        return

    send_telegram(
        f"🕌 *Shariah Screener — {today}*{cache_note}\n"
        f"Screened {total} halal stocks • *{len(passed)} passed all filters*\n"
        "─────────────────────"
    )

    for r in passed:
        drop = r["pct_below_high"] * 100 if r["pct_below_high"] else 0
        send_telegram(
            f"✅ *{r['symbol']}*\n"
            f"💰 ₹{r['current_price']}  ({drop:.1f}% below 52W high of ₹{r['high_52w']})\n"
            f"📊 D/E: {r['de_ratio']}  |  Rev CAGR: {r['revenue_cagr_3yr']}%\n"
            f"📉 PE: {r['pe_current']} vs 5yr avg {r['pe_5yr_avg']} "
            f"({r['pe_discount_pct']}% discount)\n"
            f"☪️ Shariah: Zamzam-verified"
        )
        time.sleep(0.5)


# ══════════════════════════════════════════════
# 7. MAIN
# ══════════════════════════════════════════════
def run_screener():
    log.info("═" * 60)
    log.info("NSE Shariah Screener (Zamzam-First) — starting run")
    log.info("═" * 60)

    # Step 1: Get Zamzam halal list (scrape → cache → abort)
    halal_symbols, is_fresh = get_halal_list()
    log.info(
        f"Halal universe: {len(halal_symbols)} stocks "
        f"({'live scrape' if is_fresh else 'from cache'})"
    )

    # Step 2: Screen each halal stock
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
        except Exception as e:
            log.error(f"  ❌ Error [{symbol}]: {e}")
            all_results.append({
                "symbol": symbol, "error": str(e), "all_pass": False,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                **{k: None for k in [
                    "de_ratio","revenue_cagr_3yr","pe_current","pe_5yr_avg",
                    "pe_discount_pct","current_price","high_52w","pct_below_high",
                ]},
                **{k: False for k in ["pass_de","pass_revenue","pass_pe","pass_price"]},
            })

    log.info(f"Done. {len(passed)}/{len(all_results)} passed all filters.")

    # Step 3: Google Sheets
    update_google_sheets(all_results, passed, is_fresh)

    # Step 4: Telegram
    send_alerts(passed, len(all_results), is_fresh)

    log.info("Run complete ✓")
    return passed


if __name__ == "__main__":
    run_screener()

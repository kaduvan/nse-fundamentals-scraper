import asyncio
import io
import os
import csv
import logging
from datetime import date
import duckdb
import pandas as pd
import aiohttp

# =========================
# 1. CONFIGURATION & LOGGING
# =========================
CONCURRENCY = 10
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

STATEMENT_REGISTRY = {
    "income": {"url_suffix": "/"},
    "balance_sheet": {"url_suffix": "/balance-sheet/"},
    "cash_flow": {"url_suffix": "/cash-flow-statement/"},
    "ratios": {"url_suffix": "/ratios/"}
}

class ProgressTracker:
    def __init__(self, total):
        self.current = 0
        self.total = total

    def increment(self):
        self.current += 1
        return self.current

# =========================
# 2. DYNAMIC TICKER MATCHER (With ISIN Deduplication)
# =========================
async def fetch_all_tickers(session):
    targets = []
    seen_isins = set()

    # 1. Fetch NSE Mainboard & NSE-SME (Primary Source)
    nse_urls = [
        ("https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv", "NSE"),
        ("https://nsearchives.nseindia.com/content/sme/SME_EQUITY_L.csv", "NSE")
    ]
    nse_headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/csv"}

    for url, exch in nse_urls:
        try:
            async with session.get(url, headers=nse_headers, timeout=15) as response:
                if response.status == 200:
                    text = await response.text()
                    reader = csv.reader(io.StringIO(text))
                    header = next(reader, None)
                    
                    sym_idx, isin_idx = 0, 6
                    if header:
                        for i, col in enumerate(header):
                            if "SYMBOL" in col.upper(): sym_idx = i
                            if "ISIN" in col.upper(): isin_idx = i

                    for row in reader:
                        if row and len(row) > max(sym_idx, isin_idx):
                            sym = row[sym_idx].strip()
                            isin = row[isin_idx].strip()
                            if sym:
                                targets.append({"symbol": sym, "exchange": exch, "isin": isin})
                                if isin:
                                    seen_isins.add(isin)
        except Exception as e:
            logging.error(f"Error fetching {exch} tickers from {url}: {e}")

    logging.info(f"Loaded {len(targets)} NSE/SME tickers ({len(seen_isins)} unique ISINs).")

    # 2. Fetch BSE and Filter for BSE-Exclusive Scrips Only
    bse_urls = [
        "https://api.bseindia.com/BseIndiaAPI/api/LitsOfScripCSVDownload/w?Group=&Scripcode=&segment=Equity&status=Active",
        "https://api.bseindia.com/BseIndiaAPI/api/LitsOfScripCSVDownload/w?Group=&Scripcode=&segment=EQT0&status=Active"
    ]
    bse_headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.bseindia.com/",
        "Accept": "text/csv"
    }

    bse_added, bse_skipped = 0, 0

    for url in bse_urls:
        try:
            async with session.get(url, headers=bse_headers, timeout=15) as response:
                if response.status == 200:
                    text = await response.text()
                    reader = csv.reader(io.StringIO(text))
                    header = next(reader, None)

                    code_idx, isin_idx = 0, -1
                    if header:
                        for i, col in enumerate(header):
                            if "SCRIP" in col.upper() or "CODE" in col.upper(): code_idx = i
                            if "ISIN" in col.upper(): isin_idx = i

                    for row in reader:
                        if row and len(row) > code_idx:
                            scrip_code = row[code_idx].strip()
                            bse_isin = row[isin_idx].strip() if isin_idx != -1 and len(row) > isin_idx else ""

                            if bse_isin and bse_isin in seen_isins:
                                bse_skipped += 1
                                continue

                            if scrip_code:
                                targets.append({"symbol": scrip_code, "exchange": "BSE", "isin": bse_isin})
                                if bse_isin:
                                    seen_isins.add(bse_isin)
                                bse_added += 1
        except Exception as e:
            logging.error(f"Error fetching BSE tickers from {url}: {e}")

    logging.info(f"⚡ Smart Deduplication: Added {bse_added} BSE-exclusive tickers; skipped {bse_skipped} dual-listed duplicates.")
    return targets

# =========================
# 3. R2 COVERAGE PRE-CHECK (Smart Delta Planner)
# =========================
def get_expected_filing_cutoffs():
    today = date.today()
    expected_annual = f"{today.year}-03-31" if today.month >= 7 else f"{today.year - 1}-03-31"

    m, y = today.month, today.year
    if m >= 11 or (m == 11 and today.day >= 15): expected_quarterly = f"{y}-09-30"
    elif m >= 8 or (m == 8 and today.day >= 15): expected_quarterly = f"{y}-06-30"
    elif m >= 5 or (m == 5 and today.day >= 30): expected_quarterly = f"{y}-03-31"
    elif m >= 2 or (m == 2 and today.day >= 15): expected_quarterly = f"{y - 1}-12-31"
    else: expected_quarterly = f"{y - 1}-09-30"

    return expected_annual, expected_quarterly

def get_existing_r2_coverage():
    r2_access = os.getenv("R2_ACCESS_KEY")
    r2_secret = os.getenv("R2_SECRET_KEY")
    r2_account = os.getenv("R2_ACCOUNT_ID")
    r2_bucket = os.getenv("R2_BUCKET_NAME", "financial-data-lake")

    if not all([r2_access, r2_secret, r2_account]):
        logging.warning("R2 environment variables not fully set. Proceeding without pre-check.")
        return {}

    master_file = f"r2://{r2_bucket}/fundamentals_master.parquet"
    con = duckdb.connect()
    con.sql(f"CREATE SECRET (TYPE r2, KEY_ID '{r2_access}', SECRET '{r2_secret}', ACCOUNT_ID '{r2_account}');")

    try:
        logging.info("Checking existing coverage in Cloudflare R2...")
        query = f"""
            SELECT ticker, exchange, period, MAX(period_date) as max_date
            FROM '{master_file}'
            WHERE period_date != 'TTM' AND period_date IS NOT NULL
            GROUP BY ticker, exchange, period
        """
        df = con.sql(query).df()
        
        coverage = {}
        for _, row in df.iterrows():
            key = (str(row["ticker"]).upper(), str(row["exchange"]).upper())
            if key not in coverage: coverage[key] = {}
            coverage[key][str(row["period"]).lower()] = str(row["max_date"])
        return coverage
    except Exception as e:
        logging.warning(f"Could not inspect R2 master file (initial load): {e}")
        return {}

def plan_targets_for_stock(symbol, exchange, coverage_map, expected_annual, expected_quarterly):
    key = (symbol.upper(), exchange.upper())
    stock_cov = coverage_map.get(key, {})

    need_annual = not (stock_cov.get("annual") and stock_cov.get("annual") >= expected_annual)
    need_quarterly = not (stock_cov.get("quarterly") and stock_cov.get("quarterly") >= expected_quarterly)

    planned_tasks = []
    if need_annual:
        planned_tasks.extend([("annual", r) for r in STATEMENT_REGISTRY.keys()])
    if need_quarterly:
        planned_tasks.extend([("quarterly", r) for r in STATEMENT_REGISTRY.keys()])

    return planned_tasks

# =========================
# 4. SVELTEKIT JSON PARSER
# =========================
def parse_svelte_json(json_data, symbol, exchange, isin, report_type, period):
    nodes = json_data.get("nodes", [])
    root, flat_data = None, []

    for node in nodes:
        if not isinstance(node, dict) or "data" not in node: continue
        current_flat_data = node["data"]
        for item in current_flat_data:
            if isinstance(item, dict) and "financialData" in item and "map" in item:
                root = item
                flat_data = current_flat_data
                break
        if root: break

    if not root or not flat_data: return []

    def resolve(val):
        return flat_data[val] if (isinstance(val, int) and 0 <= val < len(flat_data)) else val

    financial_data = resolve(root["financialData"])
    metric_map_refs = resolve(root["map"])
    date_refs = resolve(financial_data.get("datekey", []))
    dates = [resolve(idx) for idx in date_refs]

    final_rows = []
    for ref in metric_map_refs:
        metric_obj = resolve(ref)
        if not isinstance(metric_obj, dict): continue

        metric_id = resolve(metric_obj.get("id"))
        metric_title = resolve(metric_obj.get("title"))

        if metric_id in financial_data:
            values_ref = resolve(financial_data[metric_id])
            if isinstance(values_ref, list):
                values = [resolve(idx) for idx in values_ref]
                for i, date_val in enumerate(dates):
                    if i < len(values):
                        val = values[i] if values[i] is not None else ""
                        final_rows.append({
                            "ticker": str(symbol).upper(),
                            "isin": str(isin).upper(),
                            "metric": str(metric_title),
                            "period_date": str(date_val),
                            "value": str(val),
                            "exchange": exchange.upper(),
                            "statement_type": report_type.replace("_", " ").title(),
                            "period": period.title()
                        })
    return final_rows

# =========================
# 5. TARGET PROCESSOR
# =========================
async def process_target(target, tasks_config, session, semaphore, tracker, is_retry=False):
    symbol = target["symbol"]
    exchange = target["exchange"].upper()
    isin = target.get("isin", "")
    
    # Map 'BSE' to 'bom' for StockAnalysis routing
    exchange_slug = "bom" if exchange == "BSE" else exchange.lower()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    }

    all_rows, failed_config = [], []

    async with semaphore:
        for period, report_type in tasks_config:
            statement_cfg = STATEMENT_REGISTRY[report_type]
            suffix = statement_cfg["url_suffix"].strip("/")

            url_path = f"/quote/{exchange_slug}/{symbol.lower()}/financials"
            if suffix: url_path += f"/{suffix}"
            url = f"https://stockanalysis.com{url_path}/__data.json?x-sveltekit-trailing-slash=1&x-sveltekit-invalidated=011"
            if period == "quarterly": url += "&p=quarterly"

            try:
                async with session.get(url, headers=headers, timeout=20) as response:
                    if response.status == 200:
                        json_data = await response.json()
                        rows = parse_svelte_json(json_data, symbol, exchange, isin, report_type, period)
                        all_rows.extend(rows)
                    elif response.status in [403, 429]:
                        failed_config.append((period, report_type))
            except Exception:
                failed_config.append((period, report_type))

            await asyncio.sleep(0.4)

    tag = f"[{tracker.increment()}/{tracker.total}]" if not is_retry else "[RETRY]"

    if all_rows:
        logging.info(f"{tag} [{exchange}] {symbol} → {len(all_rows)} metrics extracted")
    elif failed_config:
        logging.warning(f"{tag} [{exchange}] {symbol} → {len(failed_config)} requests blocked/queued")
    else:
        logging.info(f"{tag} [{exchange}] {symbol} → 0 rows (No data)")

    return target, all_rows, failed_config

# =========================
# 6. MAIN ORCHESTRATOR & SYNC
# =========================
async def main():
    semaphore = asyncio.Semaphore(CONCURRENCY)
    master_data = []

    async with aiohttp.ClientSession() as session:
        all_targets = await fetch_all_tickers(session)
        expected_annual, expected_quarterly = get_expected_filing_cutoffs()
        coverage_map = get_existing_r2_coverage()

        scrape_queue = []
        skipped_count = 0

        for target in all_targets:
            planned = plan_targets_for_stock(
                target["symbol"], target["exchange"], coverage_map, expected_annual, expected_quarterly
            )
            if planned: scrape_queue.append((target, planned))
            else: skipped_count += 1

        logging.info(f"⚡ Smart Delta: {skipped_count} tickers up to date. Scraping {len(scrape_queue)} targets.")

        tracker = ProgressTracker(len(scrape_queue))
        tasks = [process_target(target, task_cfg, session, semaphore, tracker) for target, task_cfg in scrape_queue]
        results = await asyncio.gather(*tasks)

        failed_queue = {}
        for target, rows, fails in results:
            master_data.extend(rows)
            if fails: failed_queue[(target["symbol"], target["exchange"])] = (target, fails)

        if failed_queue:
            logging.warning(f"⚠️ {len(failed_queue)} targets had blocks. Waiting 10s before Retry Pass...")
            await asyncio.sleep(10)

            retry_tasks = [
                process_target(target, fails, session, semaphore, tracker, is_retry=True)
                for _, (target, fails) in failed_queue.items()
            ]
            retry_results = await asyncio.gather(*retry_tasks)
            for _, rows, _ in retry_results:
                master_data.extend(rows)

    logging.info("Scraping finished. Starting Cloudflare R2 merge...")

    if master_data:
        try:
            df = pd.DataFrame(master_data)
            r2_access = os.getenv("R2_ACCESS_KEY")
            r2_secret = os.getenv("R2_SECRET_KEY")
            r2_account = os.getenv("R2_ACCOUNT_ID")
            r2_bucket = os.getenv("R2_BUCKET_NAME", "financial-data-lake")

            if not all([r2_access, r2_secret, r2_account]):
                logging.error("Missing R2 Environment Variables! Skipping upload.")
                return

            master_file = f"r2://{r2_bucket}/fundamentals_master.parquet"
            con = duckdb.connect()
            con.execute("PRAGMA memory_limit='4GB';")
            con.register("raw_new_data", df)

            con.sql(f"CREATE SECRET (TYPE r2, KEY_ID '{r2_access}', SECRET '{r2_secret}', ACCOUNT_ID '{r2_account}');")

            logging.info(f"Merging {len(master_data)} rows into R2 Parquet...")

            upsert_query = f"""
                CREATE OR REPLACE TEMP TABLE updated_master AS
                WITH safe_new_data AS (
                    SELECT 
                        CAST(ticker AS VARCHAR) AS ticker,
                        CAST(isin AS VARCHAR) AS isin,
                        CAST(metric AS VARCHAR) AS metric,
                        CAST(period_date AS VARCHAR) AS period_date,
                        CAST(value AS VARCHAR) AS value,
                        CAST(exchange AS VARCHAR) AS exchange,
                        CAST(statement_type AS VARCHAR) AS statement_type,
                        CAST(period AS VARCHAR) AS period
                    FROM raw_new_data
                ),
                combined_data AS (
                    SELECT * FROM '{master_file}'
                    UNION ALL BY NAME
                    SELECT * FROM safe_new_data
                ),
                deduplicated AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY ticker, metric, period_date, period, exchange 
                            ORDER BY period_date DESC
                        ) as rn
                    FROM combined_data
                )
                SELECT * EXCLUDE (rn) FROM deduplicated WHERE rn = 1;
            """
            con.sql(upsert_query)
            con.sql(f"COPY updated_master TO '{master_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD');")
            logging.info("✅ Cloudflare R2 Data Lake successfully updated!")

        except Exception as e:
            logging.error(f"❌ R2 Sync Failed: {str(e)}")
    else:
        logging.info("ℹ️ No new metrics needed to be written. All records up to date.")

if __name__ == "__main__":
    asyncio.run(main())

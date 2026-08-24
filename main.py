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
CONCURRENCY = 10  # Optimal balance between speed and proxy longevity
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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
# 2. DYNAMIC TICKER MATCHER (NSE + SME + BSE)
# =========================
async def fetch_all_tickers(session):
    """Fetches live active equities for NSE, NSE-SME, and BSE."""
    targets = {}

    # 1. NSE Mainboard & NSE SME
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
                    next(reader, None)
                    for row in reader:
                        if row and row[0].strip():
                            sym = row[0].strip()
                            targets[(sym, exch)] = {"symbol": sym, "exchange": exch}
        except Exception as e:
            logging.error(f"Error fetching {exch} tickers from {url}: {e}")

    # 2. BSE Live API
    bse_urls = [
        "https://api.bseindia.com/BseIndiaAPI/api/LitsOfScripCSVDownload/w?Group=&Scripcode=&segment=Equity&status=Active",
        "https://api.bseindia.com/BseIndiaAPI/api/LitsOfScripCSVDownload/w?Group=&Scripcode=&segment=EQT0&status=Active"
    ]
    bse_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://www.bseindia.com/",
        "Accept": "text/csv"
    }

    for url in bse_urls:
        try:
            async with session.get(url, headers=bse_headers, timeout=15) as response:
                if response.status == 200:
                    text = await response.text()
                    reader = csv.reader(io.StringIO(text))
                    next(reader, None)
                    for row in reader:
                        if row and row[0].strip():
                            sym = row[0].strip()
                            targets[(sym, "BSE")] = {"symbol": sym, "exchange": "BSE"}
        except Exception as e:
            logging.error(f"Error fetching BSE tickers from {url}: {e}")

    # 3. BSE Local CSV Fallback
    if not any(t["exchange"] == "BSE" for t in targets.values()):
        bse_fallback = "bse_tickers.csv"
        if os.path.exists(bse_fallback):
            try:
                with open(bse_fallback, "r", encoding="utf-8") as f:
                    reader = csv.reader(f)
                    next(reader, None)
                    for row in reader:
                        if row and row[0].strip():
                            sym = row[0].strip()
                            targets[(sym, "BSE")] = {"symbol": sym, "exchange": "BSE"}
                logging.info("Loaded BSE tickers from local fallback file.")
            except Exception as e:
                logging.error(f"Error reading local BSE file: {e}")

    target_list = list(targets.values())
    if target_list:
        logging.info(f"✅ Successfully loaded {len(target_list)} cross-exchange tickers.")
        return target_list

    return [
        {"symbol": "RELIANCE", "exchange": "NSE"},
        {"symbol": "500510", "exchange": "BSE"}
    ]

# =========================
# 3. R2 COVERAGE PRE-CHECK (Smart Delta Planner)
# =========================
def get_expected_filing_cutoffs():
    """Calculates expected reporting period cutoffs for Indian equities."""
    today = date.today()
    
    # Annual: If past July, expect current year FY (YYYY-03-31); otherwise previous FY
    if today.month >= 7:
        expected_annual = f"{today.year}-03-31"
    else:
        expected_annual = f"{today.year - 1}-03-31"

    # Quarterly: Indian quarterly reporting rules (published within ~45 days)
    m, y = today.month, today.year
    if m >= 11 or (m == 11 and today.day >= 15):
        expected_quarterly = f"{y}-09-30"
    elif m >= 8 or (m == 8 and today.day >= 15):
        expected_quarterly = f"{y}-06-30"
    elif m >= 5 or (m == 5 and today.day >= 30):
        expected_quarterly = f"{y}-03-31"
    elif m >= 2 or (m == 2 and today.day >= 15):
        expected_quarterly = f"{y - 1}-12-31"
    else:
        expected_quarterly = f"{y - 1}-09-30"

    return expected_annual, expected_quarterly

def get_existing_r2_coverage():
    """Queries Cloudflare R2 Parquet in seconds to determine the latest period date per ticker."""
    r2_access = os.getenv("R2_ACCESS_KEY")
    r2_secret = os.getenv("R2_SECRET_KEY")
    r2_account = os.getenv("R2_ACCOUNT_ID")
    r2_bucket = os.getenv("R2_BUCKET_NAME", "financial-data-lake")

    if not all([r2_access, r2_secret, r2_account]):
        logging.warning("R2 environment variables not fully set. Proceeding without pre-check.")
        return {}

    master_file = f"r2://{r2_bucket}/fundamentals_master.parquet"
    con = duckdb.connect()

    con.sql(f"""
        CREATE SECRET (
            TYPE r2,
            KEY_ID '{r2_access}',
            SECRET '{r2_secret}',
            ACCOUNT_ID '{r2_account}'
        );
    """)

    try:
        logging.info("Checking existing coverage in Cloudflare R2...")
        query = f"""
            SELECT 
                ticker, 
                exchange, 
                period, 
                MAX(period_date) as max_date
            FROM '{master_file}'
            WHERE period_date != 'TTM' AND period_date IS NOT NULL
            GROUP BY ticker, exchange, period
        """
        df = con.sql(query).df()
        
        coverage = {}
        for _, row in df.iterrows():
            key = (str(row["ticker"]).upper(), str(row["exchange"]).upper())
            if key not in coverage:
                coverage[key] = {}
            coverage[key][str(row["period"]).lower()] = str(row["max_date"])
        
        logging.info(f"✅ Retrieved coverage for {len(coverage)} stocks from R2.")
        return coverage
    except Exception as e:
        logging.warning(f"Could not inspect R2 master file (initial load or empty lake): {e}")
        return {}

def plan_targets_for_stock(symbol, exchange, coverage_map, expected_annual, expected_quarterly):
    """Determines which statements and periods need scraping."""
    key = (symbol.upper(), exchange.upper())
    stock_cov = coverage_map.get(key, {})

    max_ann = stock_cov.get("annual")
    max_qtr = stock_cov.get("quarterly")

    need_annual = True
    need_quarterly = True

    if max_ann and max_ann >= expected_annual:
        need_annual = False

    if max_qtr and max_qtr >= expected_quarterly:
        need_quarterly = False

    planned_tasks = []
    if need_annual:
        for r in STATEMENT_REGISTRY.keys():
            planned_tasks.append(("annual", r))
    if need_quarterly:
        for r in STATEMENT_REGISTRY.keys():
            planned_tasks.append(("quarterly", r))

    return planned_tasks

# =========================
# 4. SVELTEKIT JSON PARSER
# =========================
def parse_svelte_json(json_data, symbol, exchange, report_type, period):
    """Extracts EAV structured rows by navigating SvelteKit's indexed node structure."""
    nodes = json_data.get("nodes", [])
    root, flat_data = None, []

    for node in nodes:
        if not isinstance(node, dict) or "data" not in node:
            continue
        current_flat_data = node["data"]
        for item in current_flat_data:
            if isinstance(item, dict) and "financialData" in item and "map" in item:
                root = item
                flat_data = current_flat_data
                break
        if root:
            break

    if not root or not flat_data:
        return []

    def resolve(val):
        if isinstance(val, int) and 0 <= val < len(flat_data):
            return flat_data[val]
        return val

    financial_data = resolve(root["financialData"])
    metric_map_refs = resolve(root["map"])

    date_refs = resolve(financial_data.get("datekey", []))
    dates = [resolve(idx) for idx in date_refs]

    final_rows = []
    for ref in metric_map_refs:
        metric_obj = resolve(ref)
        if not isinstance(metric_obj, dict):
            continue

        metric_id = resolve(metric_obj.get("id"))
        metric_title = resolve(metric_obj.get("title"))

        if metric_id in financial_data:
            values_ref = resolve(financial_data[metric_id])
            if isinstance(values_ref, list):
                values = [resolve(idx) for idx in values_ref]
                for i, date_val in enumerate(dates):
                    if i < len(values):
                        val = values[i]
                        if val is None:
                            val = ""
                        final_rows.append({
                            "ticker": str(symbol).upper(),
                            "metric": str(metric_title),
                            "period_date": str(date_val),
                            "value": str(val),
                            "exchange": exchange.upper(),
                            "statement_type": report_type.replace("_", " ").title(),
                            "period": period.title()
                        })
    return final_rows

# =========================
# 5. TARGET PROCESSOR (With BOM Slug Mapping)
# =========================
async def process_target(target, tasks_config, session, semaphore, tracker, is_retry=False):
    symbol = target["symbol"]
    exchange = target["exchange"].upper()
    
    # StockAnalysis routes BSE pages under 'bom'
    exchange_slug = "bom" if exchange == "BSE" else exchange.lower()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin"
    }

    all_rows = []
    failed_config = []

    async with semaphore:
        for period, report_type in tasks_config:
            statement_cfg = STATEMENT_REGISTRY[report_type]
            suffix = statement_cfg["url_suffix"].strip("/")

            url_path = f"/quote/{exchange_slug}/{symbol.lower()}/financials"
            if suffix:
                url_path += f"/{suffix}"

            url = f"https://stockanalysis.com{url_path}/__data.json?x-sveltekit-trailing-slash=1&x-sveltekit-invalidated=011"
            if period == "quarterly":
                url += "&p=quarterly"

            try:
                async with session.get(url, headers=headers, timeout=20) as response:
                    if response.status == 200:
                        json_data = await response.json()
                        # Pass clean exchange name ('BSE' / 'NSE') to keep Parquet partitioned cleanly
                        rows = parse_svelte_json(json_data, symbol, exchange, report_type, period)
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
        # 1. Fetch live ticker universe
        all_targets = await fetch_all_tickers(session)

        # 2. Inspect R2 for Smart Delta filtering
        expected_annual, expected_quarterly = get_expected_filing_cutoffs()
        coverage_map = get_existing_r2_coverage()

        scrape_queue = []
        skipped_count = 0

        for target in all_targets:
            planned = plan_targets_for_stock(
                target["symbol"],
                target["exchange"],
                coverage_map,
                expected_annual,
                expected_quarterly
            )
            if planned:
                scrape_queue.append((target, planned))
            else:
                skipped_count += 1

        logging.info(f"⚡ Smart Delta: {skipped_count} tickers up to date. Scraping {len(scrape_queue)} active queues.")

        tracker = ProgressTracker(len(scrape_queue))

        # 3. Main Scraping Pass
        tasks = [
            process_target(target, task_cfg, session, semaphore, tracker)
            for target, task_cfg in scrape_queue
        ]
        results = await asyncio.gather(*tasks)

        # 4. Handle Targeted Retries
        failed_queue = {}
        for target, rows, fails in results:
            master_data.extend(rows)
            if fails:
                key = (target["symbol"], target["exchange"])
                failed_queue[key] = fails

        if failed_queue:
            logging.warning(f"⚠️ {len(failed_queue)} targets had partial blocks. Waiting 10s before Retry Pass...")
            await asyncio.sleep(10)

            retry_tasks = [
                process_target({"symbol": sym, "exchange": exch}, fails, session, semaphore, tracker, is_retry=True)
                for (sym, exch), fails in failed_queue.items()
            ]
            retry_results = await asyncio.gather(*retry_tasks)

            for _, rows, _ in retry_results:
                master_data.extend(rows)

    # 5. Cloudflare R2 Sync via DuckDB
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

            # Prevent Out-Of-Memory spills
            con.execute("PRAGMA memory_limit='4GB';")
            con.register("raw_new_data", df)

            con.sql(f"""
                CREATE SECRET (
                    TYPE r2,
                    KEY_ID '{r2_access}',
                    SECRET '{r2_secret}',
                    ACCOUNT_ID '{r2_account}'
                );
            """)

            logging.info(f"Merging {len(master_data)} rows into R2 Parquet...")

            upsert_query = f"""
                CREATE OR REPLACE TEMP TABLE updated_master AS
                WITH safe_new_data AS (
                    SELECT 
                        CAST(ticker AS VARCHAR) AS ticker,
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

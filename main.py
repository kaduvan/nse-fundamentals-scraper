import asyncio
import io
import os
import csv
import logging
import duckdb
import pandas as pd
import aiohttp

# =========================
# 1. CONFIGURATION
# =========================
CONCURRENCY = 10 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
# 2. MULTI-EXCHANGE DYNAMIC TICKER MATCHER
# =========================
async def fetch_all_tickers(session):
    """Fetches live active equities for NSE, NSE-SME, and BSE."""
    targets = []
    
    # 1. Fetch NSE & SME
    nse_urls = [
        ("https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv", "NSE"),
        ("https://nsearchives.nseindia.com/content/sme/SME_EQUITY_L.csv", "NSE")
    ]
    
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/csv"}
    
    for url, exch in nse_urls:
        try:
            async with session.get(url, headers=headers, timeout=15) as response:
                if response.status == 200:
                    text = await response.text()
                    reader = csv.reader(io.StringIO(text))
                    next(reader) 
                    for row in reader:
                        if row and row[0].strip():
                            targets.append({"symbol": row[0].strip(), "exchange": exch})
        except Exception as e:
            logging.error(f"Error fetching NSE tickers from {url}: {e}")

    # 2. Fetch BSE Tickers
    bse_url = "https://www.bseindia.com/downloads/BSE_StockList.csv"
    try:
        async with session.get(bse_url, headers=headers, timeout=15) as response:
            if response.status == 200:
                text = await response.text()
                reader = csv.reader(io.StringIO(text))
                next(reader) 
                for row in reader:
                    if row and row[0].strip():
                        targets.append({"symbol": row[0].strip(), "exchange": "BSE"})
    except Exception as e:
        logging.error(f"Error fetching BSE tickers: {e}")

    if targets:
        logging.info(f"✅ Successfully loaded {len(targets)} total cross-exchange tickers (NSE, SME, BSE).")
        return targets
        
    # Fallback if exchanges are down
    return [
        {"symbol": "RELIANCE", "exchange": "NSE"},
        {"symbol": "500510", "exchange": "BSE"}
    ]

# =========================
# 3. SVELTEKIT JSON PARSER
# =========================
def parse_svelte_json(json_data, symbol, exchange, report_type, period):
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
        if root: break
            
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
        if not isinstance(metric_obj, dict): continue
            
        metric_id = resolve(metric_obj.get("id"))
        metric_title = resolve(metric_obj.get("title"))
        
        if metric_id in financial_data:
            values_ref = resolve(financial_data[metric_id])
            if isinstance(values_ref, list):
                values = [resolve(idx) for idx in values_ref]
                for i, date in enumerate(dates):
                    if i < len(values):
                        val = values[i]
                        if val is None: val = ""
                        final_rows.append({
                            "ticker": str(symbol).upper(),
                            "metric": str(metric_title),
                            "period_date": str(date),
                            "value": str(val),
                            "exchange": exchange.upper(),
                            "statement_type": report_type.replace("_", " ").title(),
                            "period": period.title()
                        })
    return final_rows

# =========================
# 4. TARGET PROCESSOR
# =========================
async def process_target(target, tasks_config, session, semaphore, tracker, is_retry=False):
    symbol = target["symbol"]
    exchange = target["exchange"].lower()
    
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
            
            url_path = f"/quote/{exchange}/{symbol.lower()}/financials"
            if suffix: url_path += f"/{suffix}"
                
            url = f"https://stockanalysis.com{url_path}/__data.json?x-sveltekit-trailing-slash=1&x-sveltekit-invalidated=011"
            if period == "quarterly": url += "&p=quarterly"

            try:
                async with session.get(url, headers=headers, timeout=20) as response:
                    if response.status == 200:
                        json_data = await response.json()
                        rows = parse_svelte_json(json_data, symbol, exchange, report_type, period)
                        all_rows.extend(rows)
                    elif response.status in [403, 429]:
                        failed_config.append((period, report_type))
            except Exception:
                failed_config.append((period, report_type))

            await asyncio.sleep(0.5)

    if not is_retry:
        count = tracker.increment()
        if all_rows:
            logging.info(f"[{count}/{tracker.total}] [{exchange.upper()}] {symbol} → {len(all_rows)} metrics extracted")
        elif not failed_config:
            logging.info(f"[{count}/{tracker.total}] [{exchange.upper()}] {symbol} → 0 rows")

    return target, all_rows, failed_config

# =========================
# 5. MAIN ORCHESTRATOR & R2 MERGE
# =========================
async def main():
    semaphore = asyncio.Semaphore(CONCURRENCY)
    master_data = []

    async with aiohttp.ClientSession() as session:
        targets = await fetch_all_tickers(session)
        tracker = ProgressTracker(len(targets))
        
        base_tasks = [(p, r) for p in ["annual", "quarterly"] for r in STATEMENT_REGISTRY.keys()]

        logging.info(f"Starting Main Scrape Pass across {len(targets)} targets...")
        tasks = [process_target(t, base_tasks, session, semaphore, tracker) for t in targets]
        results = await asyncio.gather(*tasks)

        failed_queue = {}
        for target, rows, fails in results:
            master_data.extend(rows)
            if fails: 
                # Use symbol + exchange tuple as dictionary key
                key = (target["symbol"], target["exchange"])
                failed_queue[key] = fails
        
        if failed_queue:
            logging.warning(f"⚠️ {len(failed_queue)} targets had partial blocks. Retrying...")
            await asyncio.sleep(10)
            
            retry_tasks = [
                process_target({"symbol": sym, "exchange": exch}, fails, session, semaphore, tracker, is_retry=True)
                for (sym, exch), fails in failed_queue.items()
            ]
            retry_results = await asyncio.gather(*retry_tasks)
            
            for target, rows, fails in retry_results:
                master_data.extend(rows)

    logging.info("✅ Scraping complete. Initiating Cloudflare R2 Sync...")
    
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
            con.register("raw_new_data", df)
            
            con.sql(f"""
                CREATE SECRET (
                    TYPE r2,
                    KEY_ID '{r2_access}',
                    SECRET '{r2_secret}',
                    ACCOUNT_ID '{r2_account}'
                );
            """)

            logging.info(f"Merging {len(master_data)} total rows into R2 Parquet...")
            
            # DuckDB upsert handles deduplication across ticker, metric, period, date, AND exchange
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

if __name__ == "__main__":
    asyncio.run(main())

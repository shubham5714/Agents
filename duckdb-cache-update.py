import boto3
import duckdb
import logging

# --- Configuration ---
AWS_ACCESS_KEY = "AKIATM2WCSDSGMKRKJ74"
AWS_SECRET_KEY = ""
AWS_REGION = "ap-south-1"
DATABASE_NAME = "nexus-mall"
TABLE_NAME = "fortinet"

# Cache Settings
CACHE_FILE = "nexus_mall_caches.duckdb"
LOCAL_TABLE_NAME = "fortinet_cache"
CACHE_RETENTION_DAYS = 7

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_metadata_location():
    glue = boto3.client(
        "glue",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    resp = glue.get_table(DatabaseName=DATABASE_NAME, Name=TABLE_NAME)
    metadata_location = resp["Table"]["Parameters"].get("metadata_location")
    if not metadata_location:
        raise RuntimeError("Glue table has no metadata_location parameter.")
    return metadata_location

def setup_duckdb(con):
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}';")
    con.execute(f"SET s3_region='{AWS_REGION}';")

def evolve_schema(con, metadata_location):
    """
    Checks for new columns in the remote Iceberg table and adds them to the local cache.
    """
    logging.info("Checking for schema changes (new columns)...")
    
    # 1. Get Remote Schema
    # We use a subquery to ensure DESCRIBE works correctly with the iceberg_scan function
    remote_df = con.execute(f"DESCRIBE SELECT * FROM iceberg_scan('{metadata_location}')").fetchdf()
    remote_cols = {row['column_name']: row['column_type'] for _, row in remote_df.iterrows()}

    # 2. Get Local Schema
    try:
        local_df = con.execute(f"DESCRIBE SELECT * FROM {LOCAL_TABLE_NAME}").fetchdf()
        local_cols = {row['column_name']: row['column_type'] for _, row in local_df.iterrows()}
    except Exception:
        # Table might not exist yet, handled in ensure_schema, return here
        return

    # 3. Identify Missing Columns (Exist in Remote, missing in Local)
    # Note: This assumes additive schema changes (new columns added).
    # It does NOT handle column deletions or type changes (which require table rebuild).
    missing_cols = []
    for col_name, col_type in remote_cols.items():
        if col_name not in local_cols:
            missing_cols.append((col_name, col_type))

    # 4. Add Missing Columns
    if missing_cols:
        logging.info(f"Found {len(missing_cols)} new columns. Evolving schema...")
        for col_name, col_type in missing_cols:
            # Use quotes to handle column names with special characters or spaces
            sql = f'ALTER TABLE {LOCAL_TABLE_NAME} ADD COLUMN "{col_name}" {col_type}'
            logging.debug(f"Executing: {sql}")
            try:
                con.execute(sql)
            except Exception as e:
                logging.warning(f"Failed to add column {col_name}: {e}")
    else:
        logging.info("Schema is up to date.")

def ensure_schema(con, metadata_location):
    """
    Ensures the local table exists and is not corrupted (1 column issue).
    """
    table_exists = con.execute(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{LOCAL_TABLE_NAME}')"
    ).fetchone()[0]

    needs_recreation = False

    if table_exists:
        col_count = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns 
            WHERE table_name = '{LOCAL_TABLE_NAME}'
        """).fetchone()[0]
        
        logging.info(f"Existing table '{LOCAL_TABLE_NAME}' has {col_count} columns.")
        
        if col_count == 1:
            logging.warning("Detected corrupted schema (1 column). Dropping table to fix...")
            con.execute(f"DROP TABLE {LOCAL_TABLE_NAME}")
            needs_recreation = True
    else:
        needs_recreation = True

    if needs_recreation:
        logging.info(f"Creating table '{LOCAL_TABLE_NAME}' using Iceberg schema...")
        con.execute(f"CREATE TABLE {LOCAL_TABLE_NAME} AS SELECT * FROM iceberg_scan('{metadata_location}') LIMIT 0")

def run_update_job():
    logging.info("Starting Cache Update Job...")
    
    con = duckdb.connect(CACHE_FILE)
    setup_duckdb(con)
    metadata_location = get_metadata_location()

    # --- STEP 1: ENSURE SCHEMA (Repair or Create) ---
    ensure_schema(con, metadata_location)
    
    # --- STEP 2: EVOLVE SCHEMA (Add new columns) ---
    evolve_schema(con, metadata_location)

    # --- STEP 3: INCREMENTAL UPDATE ---
    
    res = con.execute(f"SELECT MAX(drxtime) FROM {LOCAL_TABLE_NAME}").fetchone()
    max_cached_time = res[0]
    
    if not max_cached_time:
        # Initial Load
        logging.info("Cache is empty. Performing initial load of last 7 days.")
        cutoff_date = con.execute(f"SELECT NOW() - INTERVAL '{CACHE_RETENTION_DAYS} DAY'").fetchone()[0]
        fetch_start = cutoff_date
        
        logging.info(f"Fetching data from S3: {fetch_start} to NOW")
        con.execute(f"""
            INSERT INTO {LOCAL_TABLE_NAME}
            SELECT * FROM iceberg_scan('{metadata_location}')
            WHERE drxtime >= '{fetch_start}'
        """)
    else:
        # Incremental Load
        logging.info(f"Cache found. Max time: {max_cached_time}. Fetching new data...")
        fetch_start = max_cached_time
        
        # Delete Tip (Overlap handling)
        con.execute(f"DELETE FROM {LOCAL_TABLE_NAME} WHERE drxtime >= '{fetch_start}'")
        
        # Insert Fresh
        con.execute(f"""
            INSERT INTO {LOCAL_TABLE_NAME}
            SELECT * FROM iceberg_scan('{metadata_location}')
            WHERE drxtime >= '{fetch_start}'
        """)

    # --- STEP 4: PRUNING ---
    logging.info(f"Pruning data older than {CACHE_RETENTION_DAYS} days.")
    delete_count = con.execute(f"""
        DELETE FROM {LOCAL_TABLE_NAME}
        WHERE drxtime < (NOW() - INTERVAL '{CACHE_RETENTION_DAYS} DAY')
    """).fetchone()[0]

    count = con.execute(f"SELECT COUNT(*) FROM {LOCAL_TABLE_NAME}").fetchone()[0]
    logging.info(f"Update complete. Total rows in cache: {count}. Pruned rows: {delete_count}")
    
    con.close()

if __name__ == "__main__":
    run_update_job()

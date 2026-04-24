import boto3
import duckdb

# --- Configuration ---
AWS_ACCESS_KEY = "AKIATM2WCSDSGMKRKJ74"
AWS_SECRET_KEY = "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"
AWS_REGION = "ap-south-1"
DATABASE_NAME = "nexus-mall"
TABLE_NAME = "fortinet"

# Local cache file settings (Must match updater)
CACHE_FILE = "nexus_mall_caches.duckdb"
LOCAL_TABLE_NAME = "fortinet_cache"

# User Requested Range
FROM_DATETIME = "2026-04-20 09:00:00"
TO_DATETIME = "2026-04-22 10:59:59"


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

def execute_smart_query(req_from, req_to):
    # 1. Connect in Read-Only mode
    # This prevents accidental writes and reduces lock contention
    try:
        con = duckdb.connect(CACHE_FILE, read_only=True)
    except Exception as e:
        # Fallback if file doesn't exist yet
        print(f"Cache file not found ({e}). Querying S3 directly.")
        con = duckdb.connect()
    
    setup_duckdb(con)

    # 2. Determine Request Range
    req_start = con.execute(f"SELECT strptime('{req_from}', '%Y-%m-%d %H:%M:%S')").fetchone()[0]
    req_end   = con.execute(f"SELECT strptime('{req_to}', '%Y-%m-%d %H:%M:%S')").fetchone()[0]

    # 3. Determine Cache Range (Handle case where table doesn't exist)
    cache_min, cache_max = None, None
    if "fortinet_cache" in con.execute("SHOW TABLES").fetchdf()["name"].values:
        bounds = con.execute(f"SELECT MIN(drxtime), MAX(drxtime) FROM {LOCAL_TABLE_NAME}").fetchone()
        cache_min, cache_max = bounds

    queries_to_union = []

    # Helper to add cache query part
    def add_cache_query(start, end):
        print(f" -> Adding Cache Segment: {start} to {end}")
        queries_to_union.append(
            f"SELECT * FROM {LOCAL_TABLE_NAME} WHERE drxtime BETWEEN '{start}' AND '{end}'"
        )

    # Helper to add s3 query part
    def add_s3_query(start, end):
        print(f" -> Adding S3 Segment: {start} to {end}")
        queries_to_union.append(
            f"SELECT * FROM iceberg_scan('{metadata_location}') WHERE drxtime BETWEEN '{start}' AND '{end}'"
        )

    metadata_location = get_metadata_location()

    # 4. Build Union Logic (The "Smart" Part)
    if cache_min is None:
        # Case: No Cache Available
        print("Cache is empty. Querying S3 for entire range.")
        add_s3_query(req_start, req_end)
    else:
        # Calculate Overlaps
        # Left Gap (S3)
        if req_start < cache_min:
            s3_end = min(req_end, cache_min)
            add_s3_query(req_start, s3_end)
        
        # Middle (Cache)
        overlap_start = max(req_start, cache_min)
        overlap_end = min(req_end, cache_max)
        if overlap_start < overlap_end:
            add_cache_query(overlap_start, overlap_end)

        # Right Gap (S3)
        if req_end > cache_max:
            s3_start = max(req_start, cache_max)
            add_s3_query(s3_start, req_end)

    # 5. Execute Final Query
    final_sql = " UNION ALL ".join(queries_to_union) + " ORDER BY drxtime DESC"
    
    print("Executing Hybrid Query (Cache + S3)...")
    df = con.execute(final_sql).fetchdf()
    
    print(f"Total Rows Returned: {len(df)}")
    print(df)
    con.close()

if __name__ == "__main__":
    execute_smart_query(FROM_DATETIME, TO_DATETIME)

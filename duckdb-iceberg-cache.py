import boto3
import duckdb
import os

# --- Configuration ---
AWS_ACCESS_KEY = "AKIATM2WCSDSGMKRKJ74"
AWS_SECRET_KEY = ""
AWS_REGION = "ap-south-1"

DATABASE_NAME = "nexus-mall"
TABLE_NAME = "fortinet"

# Local cache file settings
CACHE_FILE = "nexus_mall_caches.duckdb"
LOCAL_TABLE_NAME = "fortinet_cache"

# Time range for the final query output
FROM_DATETIME = "2026-04-24 00:00:00"
TO_DATETIME = "2026-04-24 23:00:00"


def get_metadata_location(glue_client, db_name, table_name):
    """Fetches the Iceberg metadata location from AWS Glue."""
    resp = glue_client.get_table(DatabaseName=db_name, Name=table_name)
    metadata_location = resp["Table"]["Parameters"].get("metadata_location")
    if not metadata_location:
        raise RuntimeError(
            "Glue table has no metadata_location parameter; cannot iceberg_scan."
        )
    return metadata_location


def setup_duckdb(con):
    """Installs and loads required extensions and sets S3 credentials."""
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}';")
    con.execute(f"SET s3_region='{AWS_REGION}';")


def update_cache(con, metadata_location):
    """
    Updates the local cache table.
    1. If table doesn't exist, pulls last 1 day of data from remote.
    2. If table exists, pulls only data newer than the local max(drxtime).
    3. Prunes data older than 1 day to keep the cache size managed.
    """
    
    # Check if table exists
    table_exists = con.execute(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{LOCAL_TABLE_NAME}')"
    ).fetchone()[0]

    if not table_exists:
        print("Local cache not found. Initializing with last 1 day of data...")
        con.execute(f"""
            CREATE OR REPLACE TABLE {LOCAL_TABLE_NAME} AS
            SELECT *
            FROM iceberg_scan('{metadata_location}')
            WHERE drxtime >= NOW() - INTERVAL '7 DAY'
        """)
    else:
        print("Local cache found. Checking for incremental updates...")
        
        # Find the latest timestamp in the local cache
        last_cached_time = con.execute(f"SELECT MAX(drxtime) FROM {LOCAL_TABLE_NAME}").fetchone()[0]
        
        if last_cached_time:
            print(f"Last cached timestamp: {last_cached_time}. Fetching newer data...")
            # Fetch only rows newer than what we have
            con.execute(f"""
                INSERT INTO {LOCAL_TABLE_NAME}
                SELECT *
                FROM iceberg_scan('{metadata_location}')
                WHERE drxtime > '{last_cached_time}'
            """)
        else:
            # Edge case: Table exists but is empty
            print("Cache is empty. Fetching initial data...")
            con.execute(f"""
                INSERT INTO {LOCAL_TABLE_NAME}
                SELECT *
                FROM iceberg_scan('{metadata_location}')
                WHERE drxtime >= NOW() - INTERVAL '1 DAY'
            """)

        # Prune: Delete data older than 1 day to maintain the "rolling window"
        print("Pruning data older than 1 day...")
        con.execute(f"""
            DELETE FROM {LOCAL_TABLE_NAME}
            WHERE drxtime < NOW() - INTERVAL '1 DAY'
        """)


def main() -> None:
    # 1. Initialize Glue Client
    glue = boto3.client(
        "glue",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    
    # 2. Get Iceberg Metadata Location
    metadata_location = get_metadata_location(glue, DATABASE_NAME, TABLE_NAME)

    # 3. Connect to Local Cache (Persistent File)
    print(f"Connecting to local cache: {CACHE_FILE}")
    con = duckdb.connect(CACHE_FILE)
    setup_duckdb(con)

    # 4. Update Local Cache (Delta Load + Pruning)
    update_cache(con, metadata_location)

    # 5. Query the Local Cache (Fast)
    print(f"Querying local cache for range: {FROM_DATETIME} to {TO_DATETIME}")
    
    # Note: We query the LOCAL_TABLE_NAME, not iceberg_scan directly
    query = f"""
        SELECT *
        FROM {LOCAL_TABLE_NAME}
        WHERE drxtime BETWEEN strptime('{FROM_DATETIME}', '%Y-%m-%d %H:%M:%S')
            AND strptime('{TO_DATETIME}', '%Y-%m-%d %H:%M:%S')
        ORDER BY drxtime DESC
        LIMIT 300;
    """
    
    df = con.execute(query).fetchdf()
    
    print(f"Query complete. Rows returned: {len(df)}")
    print(df)


if __name__ == "__main__":
    main()

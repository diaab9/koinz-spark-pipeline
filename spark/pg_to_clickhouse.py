import logging
from pyspark.sql import SparkSession
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Logging Setup
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Configuration
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PG_JDBC_URL = "jdbc:postgresql://<pg_host>:5432/<pg_dbname>"
PG_USER     = "<pg_user>"
PG_PASSWORD = "<pg_password>"
PG_TABLE    = "public.app_user_visits_fact"

PG_HOST     = "<pg_host>"
PG_PORT     = 5432
PG_DBNAME   = "<pg_dbname>"

CH_JDBC_URL = "jdbc:clickhouse://<ch_host>:8123/<ch_dbname>"
CH_USER     = "<ch_user>"
CH_PASSWORD = "<ch_password>"
CH_TABLE    = "default.app_user_visits_fact"

WATERMARK_TABLE = "spark_sync_watermark"
SOURCE_TABLE    = "app_user_visits_fact"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PostgreSQL Connection
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Watermark Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_watermark() -> int:
    """
    Reads the last processed updated_at timestamp from the watermark table.
    Returns 0 if no watermark exists yet (first run).
    """
    with get_pg_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT last_updated FROM spark_sync_watermark WHERE table_name = %s",
                (SOURCE_TABLE,)
            )
            row = cur.fetchone()
            val = int(row["last_updated"]) if row else 0
            log.info(f"Watermark read: {val}")
            return val


def update_watermark(new_ts: int):
    """
    Updates the watermark table with the latest processed updated_at timestamp.
    Uses upsert to handle both first run and subsequent runs.
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO spark_sync_watermark (table_name, last_updated, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (table_name)
                DO UPDATE SET
                    last_updated = EXCLUDED.last_updated,
                    updated_at   = NOW()
                """,
                (SOURCE_TABLE, new_ts)
            )
        conn.commit()
    log.info(f"Watermark updated to: {new_ts}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main Pipeline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    log.info("Starting Spark session...")

    spark = SparkSession.builder \
        .appName("koinz_pg_to_clickhouse") \
        .config(
            "spark.jars",
            "/opt/spark/jars/postgresql-42.7.3.jar,"
            "/opt/spark/jars/clickhouse-jdbc-0.6.3-shaded.jar"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Step 1: Read watermark
    last_updated = get_watermark()
    log.info(f"Fetching records with updated_at > {last_updated}")

    # Step 2: Read new records from PostgreSQL
    query = f"""
        (
            SELECT *
            FROM   {PG_TABLE}
            WHERE  updated_at > {last_updated}
        ) AS new_records
    """

    df = spark.read \
        .format("jdbc") \
        .option("url",      PG_JDBC_URL) \
        .option("dbtable",  query) \
        .option("user",     PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver",   "org.postgresql.Driver") \
        .load()

    record_count = df.count()
    log.info(f"Records fetched from PostgreSQL: {record_count}")

    if record_count == 0:
        log.info("No new records found. Exiting.")
        spark.stop()
        return

    # Step 3: Write to ClickHouse
    log.info("Writing records to ClickHouse...")

    df.write \
        .format("jdbc") \
        .option("url",      CH_JDBC_URL) \
        .option("dbtable",  CH_TABLE) \
        .option("user",     CH_USER) \
        .option("password", CH_PASSWORD) \
        .option("driver",   "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

    log.info(f"Successfully written {record_count} records to ClickHouse.")

    # Step 4: Update watermark
    new_watermark = df.agg({"updated_at": "max"}).collect()[0][0]
    if new_watermark:
        update_watermark(int(new_watermark))

    log.info(f"Pipeline completed successfully at {datetime.utcnow()} UTC")
    spark.stop()


if __name__ == "__main__":
    main()

# Koinz Spark Pipeline

A batch pipeline that syncs visit records from PostgreSQL to ClickHouse every 30 minutes using Apache Spark.

---

## Architecture

Every run, Spark reads only new or updated records from PostgreSQL
using an `updated_at` watermark, then appends them to ClickHouse.

---

## Project Structure 

koinz-spark-pipeline/
├── README.md
├── requirements.txt
├── config/
│   └── watermark_init.sql       # Watermark table DDL + initial insert
├── sql/
│   └── clickhouse_ddl.sql       # ClickHouse table DDL
└── spark/
└── pg_to_clickhouse.py      # Main Spark application


---

## How It Works

1. Spark reads the last processed `updated_at` from the watermark table in PostgreSQL
2. Spark fetches all records from `app_user_visits_fact` where `updated_at > watermark`
3. Records are written to ClickHouse via JDBC
4. Watermark is updated to the max `updated_at` from the current batch

---

## Prerequisites

- Apache Spark 3.5.1
- Python 3.9+
- PostgreSQL 13+
- ClickHouse 23+

### JDBC Drivers

Download and place in `/opt/spark/jars/`:

```bash
# PostgreSQL driver
wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
     -P /opt/spark/jars/

# ClickHouse driver
wget https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.3/clickhouse-jdbc-0.6.3-shaded.jar \
     -P /opt/spark/jars/
```

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/<your-username>/koinz-spark-pipeline.git
cd koinz-spark-pipeline
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Create the ClickHouse table

Run `sql/clickhouse_ddl.sql` on your ClickHouse instance.

### 4. Create the watermark table

Run `config/watermark_init.sql` on your PostgreSQL instance.

### 5. Configure connections

Edit `spark/pg_to_clickhouse.py` and replace the placeholders:

<pg_host>     → PostgreSQL host
<pg_dbname>   → PostgreSQL database name
<pg_user>     → PostgreSQL username
<pg_password> → PostgreSQL password
<ch_host>     → ClickHouse host
<ch_dbname>   → ClickHouse database name
<ch_user>     → ClickHouse username
<ch_password> → ClickHouse password


---

## Running the Pipeline

```bash
spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/postgresql-42.7.3.jar,\
/opt/spark/jars/clickhouse-jdbc-0.6.3-shaded.jar \
  --driver-memory 2g \
  --executor-memory 4g \
  spark/pg_to_clickhouse.py 
```

---

## Scheduling

To run every 30 minutes, add this to crontab:

```bash
crontab -e
```


/30 * * * * spark-submit 
--master local[] 
--jars /opt/spark/jars/postgresql-42.7.3.jar,
/opt/spark/jars/clickhouse-jdbc-0.6.3-shaded.jar 
spark/pg_to_clickhouse.py >> /var/log/koinz-spark.log 2>&1



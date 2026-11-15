import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, when, lit, window, count,
    sum as _sum, max as _max, min as _min, concat, approx_count_distinct
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER = "kafka:29092"
TOPIC_NAME = "bank_transactions"

PG_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "fraud_db",
    "user": "admin",
    "password": "password"
}

tx_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("client_name", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("avg_client_spend", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("country", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("category", StringType(), True)
])

def process_metrics_batch(batch_df, batch_id):
    stats = batch_df.agg(count("transaction_id").alias("cnt"), _sum("amount").alias("vol")).collect()[0]
    cnt = stats["cnt"] or 0
    vol = float(stats["vol"] or 0.0)
    if cnt == 0:
        return
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO stream_metrics (batch_id, processed_tx_count, total_volume_dh)
        VALUES (%s, %s, %s)
        ON CONFLICT (batch_id) DO UPDATE SET
            processed_tx_count = EXCLUDED.processed_tx_count,
            total_volume_dh = EXCLUDED.total_volume_dh;
    """, (batch_id, cnt, vol))
    conn.commit()
    cur.close()
    conn.close()

def write_alerts_to_postgres(batch_df, batch_id):
    records = batch_df.collect()
    if not records:
        return

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    insert_query = """
        INSERT INTO fraud_alerts (
            transaction_id, client_id, client_name, account_id, amount,
            avg_client_spend, event_timestamp, country, merchant,
            anomaly_type, risk_score, risk_level
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id, anomaly_type) DO NOTHING;
    """
    inserted = 0
    for row in records:
        cur.execute(insert_query, (
            row["transaction_id"],
            row["client_id"],
            row["client_name"],
            row["account_id"],
            row["amount"],
            row["avg_client_spend"],
            row["event_timestamp"],
            row["country"],
            row["merchant"],
            row["anomaly_type"],
            int(row["risk_score"]),
            row["risk_level"]
        ))
        inserted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()
    print(f"[Batch {batch_id}] {len(records)} alerte(s) détectée(s) | {inserted} nouvelle(s) enregistrée(s).")

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("ProductionBankFraudDetection")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    # Nettoyage strict : filtre les JSON corrompus, montants négatifs et champs nulls
    clean_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), tx_schema).alias("data"))
        .select("data.*")
        .filter(
            col("transaction_id").isNotNull() &
            col("client_id").isNotNull() &
            col("amount").isNotNull() &
            (col("amount") > 0)
        )
        .withColumn("event_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
        .filter(col("event_timestamp").isNotNull())
        .withWatermark("event_timestamp", "2 minutes")
        .dropDuplicates(["transaction_id", "event_timestamp"])
    )

    # Flux 0 : Enregistrement des métriques globales (volume total et nombre de tx valides)
    metrics_query = (
        clean_stream.writeStream
        .foreachBatch(process_metrics_batch)
        .option("checkpointLocation", "/tmp/chk_metrics")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Règle 1 : Montant élevé (> 20 000 DH)
    r1_high_amount = (
        clean_stream
        .filter(col("amount") > 20000)
        .withColumn("anomaly_type", lit("HIGH_AMOUNT"))
        .withColumn("risk_score", when(col("amount") > 60000, lit(95)).otherwise(lit(80)))
        .withColumn("risk_level", when(col("amount") > 60000, lit("CRITICAL")).otherwise(lit("HIGH")))
        .select("transaction_id", "client_id", "client_name", "account_id", "amount", "avg_client_spend", "event_timestamp", "country", "merchant", "anomaly_type", "risk_score", "risk_level")
    )

    # Règle 2 : Déviation comportementale (> 5x la moyenne habituelle du client)
    r2_behavioral = (
        clean_stream
        .filter((col("avg_client_spend") > 0) & (col("amount") > col("avg_client_spend") * 5) & (col("amount") <= 20000))
        .withColumn("anomaly_type", lit("BEHAVIORAL_SPIKE"))
        .withColumn("risk_score", lit(75))
        .withColumn("risk_level", lit("HIGH"))
        .select("transaction_id", "client_id", "client_name", "account_id", "amount", "avg_client_spend", "event_timestamp", "country", "merchant", "anomaly_type", "risk_score", "risk_level")
    )

    # Règle 3 : Pays inhabituel (!= Morocco)
    r3_country = (
        clean_stream
        .filter(col("country") != "Morocco")
        .withColumn("anomaly_type", lit("UNUSUAL_COUNTRY"))
        .withColumn("risk_score", when(col("country").isin("Nigeria", "Russia", "Cayman Islands"), lit(92)).otherwise(lit(78)))
        .withColumn("risk_level", when(col("country").isin("Nigeria", "Russia", "Cayman Islands"), lit("CRITICAL")).otherwise(lit("HIGH")))
        .select("transaction_id", "client_id", "client_name", "account_id", "amount", "avg_client_spend", "event_timestamp", "country", "merchant", "anomaly_type", "risk_score", "risk_level")
    )

    # Règle 4 : Fréquence élevée (Sliding Window 2 min / pas 30s -> > 5 tx)
    r4_frequency = (
        clean_stream
        .groupBy(
            window(col("event_timestamp"), "2 minutes", "30 seconds"),
            col("client_id"),
            col("client_name"),
            col("account_id")
        )
        .agg(
            count("transaction_id").alias("tx_count"),
            _sum("amount").alias("amount"),
            _max("avg_client_spend").alias("avg_client_spend"),
            _max("event_timestamp").alias("event_timestamp")
        )
        .filter(col("tx_count") > 5)
        .withColumn("transaction_id", concat(lit("BURST-"), col("client_id"), lit("-"), col("window.start").cast("long")))
        .withColumn("country", lit("Morocco"))
        .withColumn("merchant", concat(lit("Rafale ("), col("tx_count"), lit(" tx / 2min)")))
        .withColumn("anomaly_type", lit("HIGH_FREQUENCY"))
        .withColumn("risk_score", lit(90))
        .withColumn("risk_level", lit("CRITICAL"))
        .select("transaction_id", "client_id", "client_name", "account_id", "amount", "avg_client_spend", "event_timestamp", "country", "merchant", "anomaly_type", "risk_score", "risk_level")
    )

    # Règle 5 : Voyage impossible (2 pays distincts en moins de 10 min)
    r5_impossible_travel = (
        clean_stream
        .groupBy(
            window(col("event_timestamp"), "10 minutes", "1 minute"),
            col("client_id"),
            col("client_name"),
            col("account_id")
        )
        .agg(
            approx_count_distinct("country").alias("country_cnt"),
            _max("country").alias("last_country"),
            _min("country").alias("first_country"),
            _sum("amount").alias("amount"),
            _max("avg_client_spend").alias("avg_client_spend"),
            _max("event_timestamp").alias("event_timestamp")
        )
        .filter(col("country_cnt") > 1)
        .withColumn("transaction_id", concat(lit("TRAVEL-"), col("client_id"), lit("-"), col("window.start").cast("long")))
        .withColumn("country", concat(col("first_country"), lit(" -> "), col("last_country")))
        .withColumn("merchant", lit("Cross-Border Activity"))
        .withColumn("anomaly_type", lit("IMPOSSIBLE_TRAVEL"))
        .withColumn("risk_score", lit(98))
        .withColumn("risk_level", lit("CRITICAL"))
        .select("transaction_id", "client_id", "client_name", "account_id", "amount", "avg_client_spend", "event_timestamp", "country", "merchant", "anomaly_type", "risk_score", "risk_level")
    )

    all_alerts = (
        r1_high_amount
        .union(r2_behavioral)
        .union(r3_country)
        .union(r4_frequency)
        .union(r5_impossible_travel)
    )

    alerts_query = (
        all_alerts.writeStream
        .outputMode("update")
        .foreachBatch(write_alerts_to_postgres)
        .option("checkpointLocation", "/tmp/chk_alerts")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("Moteur Spark Production démarré (5 règles actives + métriques)...")
    spark.streams.awaitAnyTermination()

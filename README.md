# Real-Time Financial Fraud Detection Pipeline (Kafka, Spark Streaming & PostgreSQL)

Pipeline Data Engineering de bout en bout permettant d'ingerer, analyser et detecter en temps reel des transactions bancaires suspectes a l'aide d'Apache Kafka, Spark Structured Streaming, PostgreSQL et Streamlit.

## Architecture Technique

Python Generator (250 profils clients Faker)
       |  (JSON / cle = client_id)
       v
Apache Kafka (Topic: bank_transactions | 3 Partitions)
       |  (Micro-batches 5s)
       v
Spark Structured Streaming (Nettoyage, Watermark, Sliding Windows, 5 Regles)
       |  (psycopg2 + ON CONFLICT anti-doublons)
       v
PostgreSQL (Tables: fraud_alerts, stream_metrics)
       |  (Requetes SQL temps reel)
       v
Dashboard Streamlit (KPIs, Graphiques Plotly, Journal des alertes)

## Regles de Detection Implementees

1. HIGH_AMOUNT : Transaction superieure a 20 000 DH (Score: 80-95).
2. BEHAVIORAL_SPIKE : Montant superieur a 5x la depense moyenne habituelle du client (Score: 75).
3. UNUSUAL_COUNTRY : Operation effectuee hors du pays de residence (Score: 78-92).
4. HIGH_FREQUENCY : Plus de 5 transactions en moins de 2 minutes via fenetre glissante (Score: 90).
5. IMPOSSIBLE_TRAVEL : Transactions dans deux pays distincts en moins de 10 minutes pour un meme client (Score: 98).

## Execution Rapide

1. Demarrer l'infrastructure Docker :
   docker compose up -d
   docker exec kafka kafka-topics --create --topic bank_transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   Get-Content init_db.sql | docker exec -i postgres_fraud psql -U admin -d fraud_db

2. Lancer le traitement Spark Structured Streaming :
   docker run -d -u root --name spark_streaming --network fraud-detection-streaming_default -v "${PWD}/spark:/opt/spark-apps" apache/spark:3.5.0 bash -c "pip install psycopg2-binary && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/streaming_fraud_detector.py"

3. Demarrer le producteur et le dashboard :
   python producer/producer.py
   python -m streamlit run dashboard/app.py
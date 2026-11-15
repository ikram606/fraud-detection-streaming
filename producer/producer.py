import json
import time
import random
import uuid
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

fake = Faker("fr_FR")
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "bank_transactions"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else json.dumps(v).encode("utf-8"),
    acks="all"
)

MOROCCAN_CITIES = ["Casablanca", "Rabat", "Tanger", "Marrakech", "Fès", "Agadir"]
MERCHANTS_BY_CAT = {
    "RETAIL": ["Marjane", "Carrefour", "BIM", "Decathlon", "Zara"],
    "ONLINE": ["Amazon", "Jumia", "AliExpress", "Booking.com"],
    "SERVICES": ["ONCF", "Glovo", "Inwi", "Maroc Telecom", "RAM"],
    "LUXURY": ["Luxury Watch Store", "Louis Vuitton", "Apple Store", "Bijouterie Royale"],
    "ATM": ["ATM Attijariwafa", "ATM BMCE", "ATM BCP", "ATM CIH"]
}
FOREIGN_COUNTRIES = ["France", "Spain", "UAE", "Nigeria", "Russia", "Cayman Islands", "Brazil"]

# Génération de 250 profils clients réalistes avec leur comportement moyen
random.seed(42)
CLIENTS = []
for i in range(1, 251):
    avg_spend = round(random.uniform(150, 2500), 2)
    CLIENTS.append({
        "client_id": f"C{1000 + i}",
        "client_name": fake.name(),
        "account_id": f"ACC{1000 + i}",
        "city": random.choice(MOROCCAN_CITIES),
        "home_country": "Morocco",
        "avg_spend": avg_spend
    })

def build_tx(client, amount, country, merchant, category):
    return {
        "transaction_id": f"TX-{uuid.uuid4().hex[:10].upper()}",
        "client_id": client["client_id"],
        "client_name": client["client_name"],
        "account_id": client["account_id"],
        "city": client["city"],
        "amount": round(amount, 2),
        "avg_client_spend": client["avg_spend"],
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "country": country,
        "merchant": merchant,
        "category": category
    }

def send_raw(key, payload):
    producer.send(TOPIC_NAME, key=key, value=payload)

if __name__ == "__main__":
    print(f"Simulation bancaire haute fréquence démarrée (250 profils clients) -> '{TOPIC_NAME}'...\n")
    cycle = 0
    try:
        while True:
            cycle += 1
            client = random.choice(CLIENTS)

            # Injection occasionnelle de données corrompues (pour tester le nettoyage Spark)
            if cycle % 40 == 0:
                send_raw(client["client_id"], "{corrupted_json_payload")
                bad_tx = build_tx(client, -500.0, "Morocco", "Unknown", "ERROR")
                send_raw(client["client_id"], bad_tx)
                print(f"[DATA QUALITY TEST] Injection JSON invalide + montant négatif pour {client['client_id']}")
                continue

            # Scénario 1 : Rafale (HIGH_FREQUENCY - 7 transactions en 2 secondes)
            if cycle % 25 == 0:
                print(f"\n>>> [FRAUDE RAFALE] 7 opérations rapides pour {client['client_id']} ({client['client_name']}) <<<")
                for _ in range(7):
                    tx = build_tx(client, random.uniform(200, 900), "Morocco", random.choice(MERCHANTS_BY_CAT["ONLINE"]), "ONLINE")
                    send_raw(client["client_id"], tx)
                    time.sleep(0.15)
                continue

            # Scénario 2 : Voyage impossible (IMPOSSIBLE_TRAVEL - Maroc puis Étranger en 2s)
            if cycle % 30 == 0:
                foreign = random.choice(["France", "UAE", "Nigeria", "Brazil"])
                print(f"\n>>> [VOYAGE IMPOSSIBLE] {client['client_id']} : Morocco -> {foreign} en 2s <<<")
                tx1 = build_tx(client, random.uniform(150, 600), "Morocco", "ATM Attijariwafa", "ATM")
                send_raw(client["client_id"], tx1)
                time.sleep(1)
                tx2 = build_tx(client, random.uniform(3000, 12000), foreign, "Luxury Watch Store", "LUXURY")
                send_raw(client["client_id"], tx2)
                continue

            scenario = random.choices(
                ["NORMAL", "HIGH_AMOUNT", "BEHAVIORAL_SPIKE", "UNUSUAL_COUNTRY"],
                weights=[0.80, 0.07, 0.07, 0.06]
            )[0]

            if scenario == "HIGH_AMOUNT":
                tx = build_tx(client, random.uniform(22000, 95000), "Morocco", random.choice(MERCHANTS_BY_CAT["LUXURY"]), "LUXURY")
            elif scenario == "BEHAVIORAL_SPIKE":
                # 6x à 10x la dépense habituelle du client (même si < 20 000 DH)
                tx = build_tx(client, client["avg_spend"] * random.uniform(6, 10), "Morocco", "Apple Store", "LUXURY")
            elif scenario == "UNUSUAL_COUNTRY":
                tx = build_tx(client, random.uniform(800, 8000), random.choice(FOREIGN_COUNTRIES), "Online Shop", "ONLINE")
            else:
                cat = random.choice(["RETAIL", "ONLINE", "SERVICES", "ATM"])
                normal_amt = max(20.0, random.gauss(client["avg_spend"], client["avg_spend"] * 0.3))
                tx = build_tx(client, normal_amt, "Morocco", random.choice(MERCHANTS_BY_CAT[cat]), cat)

            send_raw(client["client_id"], tx)
            if cycle % 10 == 0:
                print(f"[Flux actif] {cycle} cycles envoyés | Dernier: {tx['transaction_id']} ({tx['client_id']} - {tx['amount']} DH - {tx['country']})")
            time.sleep(0.25)  # ~4 transactions/seconde

    except KeyboardInterrupt:
        print("\nArrêt de la simulation.")
    finally:
        producer.flush()
        producer.close()

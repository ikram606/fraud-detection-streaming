import time
import pandas as pd
import psycopg2
import plotly.express as px
import streamlit as st

st.set_page_config(
    page_title="SOC Bancaire - Detection de Fraude Temps Reel",
    layout="wide"
)

PG_CONFIG = {
    "host": "127.0.0.1",
    "port": 5433,
    "dbname": "fraud_db",
    "user": "admin",
    "password": "password"
}

@st.cache_data(ttl=2)
def load_data():
    conn = psycopg2.connect(**PG_CONFIG)
    alerts_df = pd.read_sql("SELECT * FROM fraud_alerts ORDER BY detected_at DESC;", conn)
    metrics_df = pd.read_sql("SELECT COALESCE(SUM(processed_tx_count), 0) AS total_tx, COALESCE(SUM(total_volume_dh), 0) AS total_vol FROM stream_metrics;", conn)
    conn.close()
    return alerts_df, metrics_df

st.title("SOC Bancaire - Detection de Transactions Suspectes en Temps Reel")
st.caption("Architecture Streaming : Python Producer -> Apache Kafka (3 Partitions) -> Spark Structured Streaming -> PostgreSQL")

with st.sidebar:
    st.header("Controles Temps Reel")
    auto_refresh = st.toggle("Rafraichissement auto (3s)", value=True)
    st.divider()
    alerts_df, metrics_df = load_data()

    anomaly_options = ["Tous"] + sorted(alerts_df["anomaly_type"].unique().tolist()) if not alerts_df.empty else ["Tous"]
    selected_anomaly = st.selectbox("Type d'anomalie", anomaly_options)

    risk_options = ["Tous", "CRITICAL", "HIGH"]
    selected_risk = st.selectbox("Niveau de risque", risk_options)

# KPIs principaux
total_tx = int(metrics_df["total_tx"].iloc[0])
total_vol = float(metrics_df["total_vol"].iloc[0])
total_alerts = len(alerts_df)
suspect_amount = float(alerts_df["amount"].sum()) if not alerts_df.empty else 0.0
fraud_rate = (total_alerts / total_tx * 100) if total_tx > 0 else 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Transactions Traitees", f"{total_tx:,}")
c2.metric("Volume Total Flux", f"{total_vol:,.0f} DH")
c3.metric("Alertes Detectees", f"{total_alerts:,}")
c4.metric("Taux d'Alerte", f"{fraud_rate:.2f} %")
c5.metric("Montant Suspect", f"{suspect_amount:,.0f} DH")

st.divider()

if alerts_df.empty:
    st.info("En attente d'alertes dans PostgreSQL. Lancez python producer\\producer.py pour alimenter le flux.")
else:
    filtered_df = alerts_df.copy()
    if selected_anomaly != "Tous":
        filtered_df = filtered_df[filtered_df["anomaly_type"] == selected_anomaly]
    if selected_risk != "Tous":
        filtered_df = filtered_df[filtered_df["risk_level"] == selected_risk]

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Repartition par Type d'Anomalie")
        by_type = filtered_df.groupby("anomaly_type", as_index=False).agg(
            Nombre=("alert_id", "count"),
            Montant_Total=("amount", "sum")
        )
        fig_type = px.bar(
            by_type,
            x="anomaly_type",
            y="Nombre",
            color="anomaly_type",
            text="Nombre",
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        fig_type.update_layout(showlegend=False, xaxis_title="", yaxis_title="Nombre d'alertes")
        st.plotly_chart(fig_type, use_container_width=True)

    with col_right:
        st.subheader("Repartition par Niveau de Risque et Anomalie")
        by_risk = filtered_df.groupby(["risk_level", "anomaly_type"], as_index=False)["alert_id"].count()
        fig_risk = px.sunburst(
            by_risk,
            path=["risk_level", "anomaly_type"],
            values="alert_id",
            color="risk_level",
            color_discrete_map={"CRITICAL": "#EF4444", "HIGH": "#F59E0B"}
        )
        st.plotly_chart(fig_risk, use_container_width=True)

    col_bottom1, col_bottom2 = st.columns([1, 2])

    with col_bottom1:
        st.subheader("Top 10 Clients a Risque")
        top_clients = (
            filtered_df.groupby(["client_id", "client_name"], as_index=False)
            .agg(Alertes=("alert_id", "count"), Score_Moyen=("risk_score", "mean"), Cumul_DH=("amount", "sum"))
            .sort_values(by=["Alertes", "Score_Moyen"], ascending=False)
            .head(10)
        )
        top_clients["Score_Moyen"] = top_clients["Score_Moyen"].round(1)
        st.dataframe(top_clients, use_container_width=True, hide_index=True)

    with col_bottom2:
        st.subheader("Journal des Dernieres Alertes")
        display_cols = [
            "event_timestamp", "transaction_id", "client_id", "client_name",
            "amount", "avg_client_spend", "country", "anomaly_type", "risk_score", "risk_level"
        ]
        st.dataframe(filtered_df[display_cols].head(50), use_container_width=True, hide_index=True)

if auto_refresh:
    time.sleep(3)
    st.rerun()

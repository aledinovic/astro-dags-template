# dags/openfda_tylenol_monthly.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import timedelta
import pendulum
import requests
import pandas as pd
from google.cloud import bigquery  # apenas se precisar explicitamente
# obs: df.to_gbq já usa pandas-gbq que depende de google-cloud-bigquery

# ====== CONFIG ======
GCP_PROJECT  = "mba-ciencia-dados-enap"      # e.g., "my-gcp-project"
BQ_DATASET   = "crypto"                      # e.g., "crypto"
BQ_TABLE     = "openfda_tylenol"             # tabela destino
BQ_LOCATION  = "US"                          # dataset location
GCP_CONN_ID  = "google_cloud_default"        # conexão Airflow -> GCP
# ====================

OPENFDA_BASE = "https://api.fda.gov/drug/event.json"
ACTIVE_PRINCIPLE = "acetaminophen"

DEFAULT_ARGS = {
    "owner": "Data Eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="openfda_tylenol_event_count_monthly",
    description="Coleta mensal (mês anterior) de contagens por receivedate para acetaminophen na OpenFDA, desde 2020, e salva no BigQuery.",
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    schedule="@monthly",   # todo dia 1º, cobrindo o mês anterior
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["openfda", "drug.event", "acetaminophen", "monthly"],
)
def openfda_tylenol_event_count_monthly():
    @task
    def fetch_and_save():
        ctx = get_current_context()
        start = ctx["data_interval_start"]
        end = ctx["data_interval_end"]

        start_str = start.format("YYYYMMDD")
        end_inclusive_str = (end - timedelta(days=1)).format("YYYYMMDD")

        search_expr = (
            f'patient.drug.medicinalproduct:"{ACTIVE_PRINCIPLE}" '
            f'AND receivedate:[{start_str} TO {end_inclusive_str}]'
        )

        params = {"search": search_expr, "count": "receivedate"}
        resp = requests.get(OPENFDA_BASE, params=params, timeout=30)

        if resp.status_code == 404:
            results = []
        else:
            resp.raise_for_status()
            results = resp.json().get("results", [])

        if results:
            df = pd.DataFrame(results).rename(columns={"term": "date"})
            df = df[["date", "count"]].copy()
            df = df.sort_values("date").reset_index(drop=True)
        else:
            df = pd.DataFrame(columns=["date", "count"])

        # --- Envio para o BigQuery ---
        table_id = f"{BQ_DATASET}.{BQ_TABLE}"
        df.to_gbq(
            destination_table=table_id,
            project_id=GCP_PROJECT,
            if_exists="append",   # acrescenta dados mês a mês
            location=BQ_LOCATION,
        )

        return df  # também fica disponível em XCom

    fetch_and_save()


dag = openfda_tylenol_event_count_monthly()

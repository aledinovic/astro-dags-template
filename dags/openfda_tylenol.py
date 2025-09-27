# dags/openfda_tylenol_manual.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import timedelta
import pendulum
import pandas as pd
import requests

# ====== CONFIG ======
GCP_PROJECT  = "mba-ciencia-dados-enap"
BQ_DATASET   = "crypto"
BQ_TABLE     = "openfda_tylenol"
BQ_LOCATION  = "US"
GCP_CONN_ID  = "google_cloud_default"
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
    dag_id="openfda_tylenol_event_count_manual",
    description="Sob demanda: count=receivedate para acetaminophen no mês informado (conf) ou mês anterior.",
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["openfda", "manual", "acetaminophen"],
)
def openfda_tylenol_event_count_manual():
    @task
    def fetch_and_save():
        ctx = get_current_context()
        run_conf = (ctx.get("dag_run") or {}).get("conf") or {}
        logical_date = ctx["logical_date"]

        # --- df sempre definido ---
        df = pd.DataFrame(columns=["date", "count"])

        # --- mês alvo ---
        if "year" in run_conf and "month" in run_conf:
            year = int(run_conf["year"])
            month = int(run_conf["month"])
            start = pendulum.datetime(year, month, 1, tz="UTC")
        else:
            prev = logical_date.start_of("month") - timedelta(days=1)
            start = prev.start_of("month")

        end = start.add(months=1)
        start_str = start.format("YYYYMMDD")
        end_str = (end - timedelta(days=1)).format("YYYYMMDD")

        # --- chamada OpenFDA ---
        params = {
            "search": f'patient.drug.medicinalproduct:"{ACTIVE_PRINCIPLE}" AND receivedate:[{start_str} TO {end_str}]',
            "count": "receivedate",
        }

        try:
            resp = requests.get(OPENFDA_BASE, params=params, timeout=30)
            if resp.status_code == 404:
                results = []
            else:
                resp.raise_for_status()
                payload = resp.json()
                results = payload.get("results", []) or []
        except Exception as e:
            # Log amigável e retorna df vazio
            print(f"[OpenFDA] Falha na requisição ou parse: {e}")
            return df

        if results:
            raw = pd.DataFrame(results)
            # para count=receivedate vem 'time' e 'count'
            if {"time", "count"}.issubset(raw.columns):
                df = (
                    raw[["time", "count"]]
                    .rename(columns={"time": "date"})
                    .sort_values("date")
                    .reset_index(drop=True)
                )
            else:
                # Estrutura inesperada: mantém vazio e loga
                print(f"[OpenFDA] Estrutura inesperada nas colunas: {list(raw.columns)}")

        # --- grava no BigQuery se houver linhas ---
        if not df.empty:
            bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION)
            credentials = bq_hook.get_credentials()
            df.to_gbq(
                destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
                project_id=GCP_PROJECT,
                if_exists="append",
                location=BQ_LOCATION,
                credentials=credentials,
            )

        return df

    fetch_and_save()

dag = openfda_tylenol_event_count_manual()

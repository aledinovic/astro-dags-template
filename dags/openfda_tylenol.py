# dags/openfda_tylenol_manual.py
from __future__ import annotations

from datetime import timedelta
import pendulum
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context  # <- import correto
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

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
    description=(
        "Executa sob demanda: contagens diárias (receivedate) para acetaminophen "
        "no mês informado em dag_run.conf (year, month) ou, se ausente, no mês anterior ao gatilho. "
        "Salva no BigQuery e retorna DataFrame em XCom."
    ),
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    schedule=True,         
    catchup="@montlhy",
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["openfda", "manual", "acetaminophen"],
)
def openfda_tylenol_event_count_manual():

    @task
    def fetch_and_save():
        # Contexto do run / conf
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")

        if dag_run and getattr(dag_run, "conf", None) is not None:
            run_conf = dag_run.conf
        elif dag_run and getattr(dag_run, "run_conf", None) is not None:  # fallback
            run_conf = dag_run.run_conf
        else:
            run_conf = {}

        logical_date = ctx["logical_date"]

        # DataFrame sempre inicializado (evita UnboundLocalError)
        df = pd.DataFrame(columns=["date", "count"])

        # Define mês alvo: conf.year/conf.month -> senão, mês anterior
        if "year" in run_conf and "month" in run_conf:
            year = int(run_conf["year"])
            month = int(run_conf["month"])
            start = pendulum.datetime(year, month, 1, tz="UTC")
        else:
            prev = logical_date.start_of("month") - timedelta(days=1)  # último dia do mês anterior
            start = prev.start_of("month")

        end = start.add(months=1)  # exclusivo
        start_str = start.format("YYYYMMDD")
        end_inclusive_str = (end - timedelta(days=1)).format("YYYYMMDD")

        # Consulta OpenFDA (apenas acetaminophen; count=receivedate -> retorna 'time')
        params = {
            "search": f'patient.drug.medicinalproduct:"{ACTIVE_PRINCIPLE}" '
                      f'AND receivedate:[{start_str} TO {end_inclusive_str}]',
            "count": "receivedate",
        }

        try:
            resp = requests.get(OPENFDA_BASE, params=params, timeout=30)
            print(f"[OpenFDA] URL: {resp.url}")  # útil para depuração
            if resp.status_code == 404:
                results = []
            else:
                resp.raise_for_status()
                results = (resp.json() or {}).get("results", []) or []
        except Exception as e:
            print(f"[OpenFDA] Falha na requisição/parse: {e}")
            return df  # vazio

        if results:
            raw = pd.DataFrame(results)
            # Para count=receivedate esperamos 'time' e 'count'
            if {"time", "count"}.issubset(raw.columns):
                df = (
                    raw[["time", "count"]]
                    .rename(columns={"time": "date"})
                    .sort_values("date")
                    .reset_index(drop=True)
                )
                # Metadados úteis
                df["interval_start"] = start_str
                df["interval_end"] = end_inclusive_str
            else:
                print(f"[OpenFDA] Estrutura inesperada nas colunas: {list(raw.columns)}")

        # Envia ao BigQuery apenas se houver linhas
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

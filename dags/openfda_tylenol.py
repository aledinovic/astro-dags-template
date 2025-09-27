# dags/openfda_tylenol_manual.py
from __future__ import annotations

from datetime import timedelta
import pendulum
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# ====== CONFIG ======
GCP_PROJECT  = "mba-ciencia-dados-enap"   # seu projeto GCP
BQ_DATASET   = "crypto"                   # dataset alvo
BQ_TABLE     = "openfda_tylenol"          # tabela alvo
BQ_LOCATION  = "US"                       # localização do dataset (US/EU)
GCP_CONN_ID  = "google_cloud_default"     # conexão do Airflow com SA
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
        "Sob demanda: contagens diárias (receivedate) para acetaminophen no mês "
        "passado em dag_run.conf (year, month) ou, se ausente, no mês anterior ao gatilho. "
        "Envia ao BigQuery e retorna DataFrame em XCom."
    ),
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    schedule="@monthly",          # roda só quando você aciona o gatilho
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["openfda", "manual", "acetaminophen"],
)
def openfda_tylenol_event_count_manual():

    @task
    def fetch_and_save():
        # --- Contexto ---
        ctx = get_current_context()
        # logical_date sempre disponível; dag_run pode ser None em alguns cenários
        logical_date = ctx.get("logical_date")
        dag_run = ctx.get("dag_run")  # isto é um objeto DagRun (não use .get nele)

        # --- Lê conf do gatilho com acesso por atributo ---
        # Se foi acionado com: {"year": 2023, "month": 1}
        if dag_run is not None and getattr(dag_run, "conf", None):
            run_conf = dag_run.conf  # dicionário
        else:
            run_conf = {}

        # --- Define o mês alvo ---
        # DataFrame inicial para evitar UnboundLocalError
        df = pd.DataFrame(columns=["date", "count"])

        if "year" in run_conf and "month" in run_conf:
            year = int(run_conf["year"])
            month = int(run_conf["month"])
            start = pendulum.datetime(year, month, 1, tz="UTC")
        else:
            # mês anterior ao gatilho
            ld = logical_date.in_timezone("UTC") if hasattr(logical_date, "in_timezone") else pendulum.now("UTC")
            prev = ld.start_of("month").subtract(days=1)
            start = prev.start_of("month")

        end = start.add(months=1)  # exclusivo
        start_str = start.format("YYYYMMDD")
        end_inclusive_str = (end - timedelta(days=1)).format("YYYYMMDD")

        # --- Chamada OpenFDA (count=receivedate retorna 'time'/'count') ---
        params = {
            "search": f'patient.drug.medicinalproduct:"{ACTIVE_PRINCIPLE}" '
                      f'AND receivedate:[{start_str} TO {end_inclusive_str}]',
            "count": "receivedate",
        }

        try:
            resp = requests.get(OPENFDA_BASE, params=params, timeout=30)
            print(f"[OpenFDA] URL: {resp.url}")
            if resp.status_code == 404:
                results = []
            else:
                resp.raise_for_status()
                results = (resp.json() or {}).get("results", []) or []
        except Exception as e:
            print(f"[OpenFDA] Falha na requisição/parse: {e}")
            return df  # retorna vazio sem falhar

        if results:
            raw = pd.DataFrame(results)
            # Para count=receivedate esperamos colunas 'time' e 'count'
            if {"time", "count"}.issubset(raw.columns):
                df = (
                    raw[["time", "count"]]
                    .rename(columns={"time": "date"})
                    .sort_values("date")
                    .reset_index(drop=True)
                )
                # metadados úteis
                df["interval_start"] = start_str
                df["interval_end"] = end_inclusive_str
            else:
                print(f"[OpenFDA] Estrutura inesperada nas colunas: {list(raw.columns)}")

        # --- Envia ao BigQuery apenas se houver linhas ---
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

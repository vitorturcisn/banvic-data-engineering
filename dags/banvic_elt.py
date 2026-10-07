from __future__ import annotations

import csv
import os
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import dag, task


BASE_DIR = Path("/opt/airflow")
MELTANO_DIR = BASE_DIR / "meltano"
DATA_DIR = BASE_DIR / "data" / "raw"

POSTGRES_CONN_ID = "banvic_postgres"

SOURCE_FILES = {
    "clientes": DATA_DIR / "clientes.csv",
    "contas": DATA_DIR / "contas.csv",
    "transacoes": DATA_DIR / "transacoes.csv",
    "propostas_credito": DATA_DIR / "propostas_credito.csv",
    "colaboradores": DATA_DIR / "colaboradores.csv",
    "agencias": DATA_DIR / "agencias.csv",
    "colaborador_agencia": DATA_DIR / "colaborador_agencia.csv",
}

REQUIRED_DB_ENV_VARS = (
    "TARGET_POSTGRES_HOST",
    "TARGET_POSTGRES_PORT",
    "TARGET_POSTGRES_DATABASE",
    "TARGET_POSTGRES_USER",
    "TARGET_POSTGRES_PASSWORD",
)

DEFAULT_ARGS = {
    "owner": "banvic",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def count_source_rows(file_path: Path) -> int:
    """Count data rows in a CSV source file."""
    with file_path.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as csv_file:
        reader = csv.reader(csv_file)

        # Header
        next(reader, None)

        return sum(1 for _ in reader)


def validate_destination_count(
    table_name: str,
    expected_count: int,
) -> int:
    """Validate the number of rows loaded into PostgreSQL."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    connection = hook.get_conn()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f'SELECT COUNT(*) FROM raw."{table_name}"'
            )

            actual_count = cursor.fetchone()[0]
    finally:
        connection.close()

    print(
        f"{table_name}: "
        f"source={expected_count}, "
        f"destination={actual_count}"
    )

    if actual_count != expected_count:
        raise ValueError(
            f"Contagem divergente para {table_name}: "
            f"esperado={expected_count}, "
            f"encontrado={actual_count}"
        )

    return actual_count


@dag(
    dag_id="banvic_elt",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="America/Sao_Paulo",
    ),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=2,
    tags=[
        "banvic",
        "elt",
        "meltano",
        "postgres",
    ],
    description=(
        "Ingestão independente dos CSVs do BanVic "
        "para PostgreSQL usando Meltano."
    ),
)
def banvic_elt():

    def create_ingestion_task(
        table_name: str,
        source_file: Path,
    ):
        @task(
            task_id=f"ingest_{table_name}",
            execution_timeout=timedelta(minutes=15),
        )
        def ingest() -> dict[str, int]:

            missing_vars = [
                name
                for name in REQUIRED_DB_ENV_VARS
                if not os.environ.get(name)
            ]

            if missing_vars:
                raise RuntimeError(
                    "Variáveis de conexão ausentes: "
                    + ", ".join(missing_vars)
                )

            source_count = count_source_rows(source_file)

            print(
                f"Iniciando ingestão de {table_name}. "
                f"Registros na origem: {source_count}"
            )

            command = [
                "/opt/meltano-venv/bin/meltano",
                "el",
                "tap-csv",
                "target-postgres",
                "--select",
                table_name,
                "--full-refresh",
            ]

            print(
                "Executando: "
                f"meltano el tap-csv target-postgres "
                f"--select {table_name} --full-refresh"
            )

            subprocess.run(
                command,
                cwd=MELTANO_DIR,
                env=os.environ.copy(),
                check=True,
            )

            destination_count = validate_destination_count(
                table_name=table_name,
                expected_count=source_count,
            )

            print(
                f"Ingestão de {table_name} concluída. "
                f"{destination_count} registros validados."
            )

            return {
                "table": table_name,
                "source_count": source_count,
                "destination_count": destination_count,
            }

        return ingest()

    ingestion_results = []

    for table_name, source_file in SOURCE_FILES.items():

        wait_for_file = FileSensor(
            task_id=f"wait_for_{table_name}",
            filepath=source_file.name,
            fs_conn_id="fs_default",
            poke_interval=30,
            timeout=600,
            mode="reschedule",
        )

        ingest_table = create_ingestion_task(
            table_name=table_name,
            source_file=source_file,
        )

        wait_for_file >> ingest_table

        ingestion_results.append(ingest_table)

    @task(task_id="summarize_ingestion")
    def summarize_ingestion(
        results: list[dict[str, int]],
    ) -> None:
        print("========================================")
        print("RESUMO DA INGESTÃO BANVIC")
        print("========================================")

        for result in results:
            print(
                f"{result['table']}: "
                f"{result['destination_count']} registros"
            )

        print("Todas as ingestões foram validadas com sucesso.")

    summarize_ingestion(ingestion_results)


banvic_elt()
from __future__ import annotations

import csv
import os
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task


BASE_DIR = Path("/opt/airflow")
MELTANO_DIR = BASE_DIR / "meltano"
DATA_DIR = BASE_DIR / "data" / "raw"

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


@dag(
    dag_id="banvic_elt",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["banvic", "elt", "meltano", "postgres"],
    description="Ingestão dos CSVs do ERP para PostgreSQL usando Meltano.",
)
def banvic_elt():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_source_files() -> dict[str, int]:
        """Validate source files and return their row counts."""
        row_counts: dict[str, int] = {}

        for table_name, file_path in SOURCE_FILES.items():
            if not file_path.exists():
                raise FileNotFoundError(
                    f"Arquivo de origem não encontrado: {file_path}"
                )

            with file_path.open(
                "r",
                encoding="utf-8",
                newline="",
            ) as csv_file:
                reader = csv.DictReader(csv_file)
                count = sum(1 for _ in reader)

            row_counts[table_name] = count
            print(f"{table_name}: {count} registros")

        print(f"Arquivos validados: {len(row_counts)}")
        return row_counts

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=15),
    )
    def run_meltano(_source_counts: dict[str, int]) -> bool:
        """Run the Meltano tap-csv -> target-postgres pipeline."""
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

        command = [
            "/opt/meltano-venv/bin/meltano",
            "run",
            "tap-csv",
            "target-postgres",
        ]

        print("Iniciando Meltano...")
        print(f"Diretório do projeto: {MELTANO_DIR}")

        subprocess.run(
            command,
            cwd=MELTANO_DIR,
            env=os.environ.copy(),
            check=True,
        )

        print("Meltano concluído com sucesso.")
        return True

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def validate_destination(
        expected_counts: dict[str, int],
        _meltano_success: bool,
    ) -> dict[str, int]:
        """Compare destination row counts with the source counts."""
        import psycopg
        from psycopg import sql

        with psycopg.connect(
            host=os.environ["TARGET_POSTGRES_HOST"],
            port=os.environ["TARGET_POSTGRES_PORT"],
            dbname=os.environ["TARGET_POSTGRES_DATABASE"],
            user=os.environ["TARGET_POSTGRES_USER"],
            password=os.environ["TARGET_POSTGRES_PASSWORD"],
        ) as connection:

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.schemata
                        WHERE schema_name = 'raw'
                    )
                    """
                )

                schema_exists = cursor.fetchone()[0]

                if not schema_exists:
                    raise RuntimeError(
                        "Schema raw não existe no PostgreSQL."
                    )

                actual_counts: dict[str, int] = {}

                for table_name, expected_count in expected_counts.items():
                    cursor.execute(
                        sql.SQL("SELECT COUNT(*) FROM raw.{}").format(
                            sql.Identifier(table_name)
                        )
                    )

                    actual_count = cursor.fetchone()[0]
                    actual_counts[table_name] = actual_count

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

        print("Validação do destino concluída com sucesso.")
        return actual_counts

    source_counts = validate_source_files()
    meltano_success = run_meltano(source_counts)
    validate_destination(source_counts, meltano_success)


banvic_elt()

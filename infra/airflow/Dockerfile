FROM apache/airflow:3.2.2

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/meltano-venv \
    && /opt/meltano-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/meltano-venv/bin/pip install --no-cache-dir meltano==4.2.2

RUN mkdir -p \
    /opt/airflow/meltano \
    /opt/airflow/data/raw \
    /opt/airflow/dags

COPY meltano/meltano.yml /opt/airflow/meltano/meltano.yml
COPY meltano/config /opt/airflow/meltano/config
COPY dags /opt/airflow/dags
COPY infra/airflow/requirements.txt /tmp/airflow-requirements.txt

RUN chown -R airflow:root \
    /opt/meltano-venv \
    /opt/airflow/meltano \
    /opt/airflow/data \
    /opt/airflow/dags \
    /tmp/airflow-requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt

WORKDIR /opt/airflow/meltano

RUN /opt/meltano-venv/bin/meltano install

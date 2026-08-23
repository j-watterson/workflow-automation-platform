FROM apache/airflow:3.3.0-python3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt" \
    --requirement /requirements.txt

COPY pyproject.toml /opt/airflow/project/pyproject.toml
COPY src /opt/airflow/project/src
RUN pip install --no-cache-dir --no-deps /opt/airflow/project


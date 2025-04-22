FROM apache/airflow:2.8.1-python3.9

# Variables pour les contraintes Airflow
ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.9
ENV AIRFLOW_VERSION=${AIRFLOW_VERSION}
ENV PYTHON_VERSION=${PYTHON_VERSION}

# Copie des dépendances
COPY requirements.txt .

# Installation avec les contraintes officielles Airflow pour éviter les conflits
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt \
    -c https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

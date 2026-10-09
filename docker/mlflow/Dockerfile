FROM ghcr.io/mlflow/mlflow:v3.16.1

RUN pip install --no-cache-dir psycopg2-binary==2.9.10
COPY start.py /mlflow/start.py
ENTRYPOINT ["python", "/mlflow/start.py"]

"""Construct the database URL without shell interpolation or password quoting issues."""

import os

from sqlalchemy.engine import URL


def main() -> None:
    database_uri = URL.create(
        "postgresql+psycopg2",
        username=os.environ["MLFLOW_DB_USER"],
        password=os.environ["MLFLOW_DB_PASSWORD"],
        host="postgres",
        database=os.environ["MLFLOW_DB_NAME"],
    ).render_as_string(hide_password=False)
    os.execvp(
        "mlflow",
        [
            "mlflow",
            "server",
            "--host",
            "0.0.0.0",
            "--port",
            "5000",
            "--backend-store-uri",
            database_uri,
            "--serve-artifacts",
            "--artifacts-destination",
            "/mlflow/artifacts",
            "--allowed-hosts",
            "localhost:*,127.0.0.1:*,mlflow:*,mlflow",
        ],
    )


if __name__ == "__main__":
    main()

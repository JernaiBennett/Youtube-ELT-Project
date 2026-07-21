import os
import pytest
import psycopg2
from unittest import mock
from airflow.models import Variable, Connection, DagBag

# Injects a fake AIRFLOW_VAR_API_KEY env var so Variable.get("API_KEY")
# resolves to a known value without touching real Airflow Variables.
@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")

# Injects a fake AIRFLOW_VAR_CHANNEL_HANDLE env var so
# Variable.get("CHANNEL_HANDLE") resolves to a known value.
@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="Mr Beast"):
        yield Variable.get("CHANNEL_HANDLE")

# Builds a mock Postgres Connection, encodes it as an Airflow connection
# URI env var, then resolves it back via Airflow's secrets backend so
# tests can assert the round-trip works without a real database.
@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name",          #schema is the db name
    )

    conn_uri = conn.get_uri()
    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")

# Loads all DAGs from the Airflow DagBag so tests can check for import
# errors and validate DAG/task structure.
@pytest.fixture()
def dagbag():
    yield DagBag()

# Reads Airflow-style variables directly from the environment
# (AIRFLOW_VAR_<NAME>) without going through Airflow's Variable model,
# used by tests that need the raw configured values.
@pytest.fixture()
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)
    return get_airflow_variable

# Opens a real psycopg2 connection to the warehouse using credentials from
# the environment, for integration tests that need to hit the actual database.
@pytest.fixture
def real_postgres_connection():
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        yield conn

    except psycopg2.Error as e:
        pytest.fail(f"Failed to connect to the database: {e}")

    finally:
        if conn:
            conn.close()


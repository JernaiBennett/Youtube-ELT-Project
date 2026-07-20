from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    """Open a connection + cursor to the ELT Postgres DB via the Airflow connection."""
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    # RealDictCursor so rows come back as dicts keyed by column name instead of tuples
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

# close cursor and connection to release resources
def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

# define schema
def create_schema(schema):

    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)

    conn.commit()

    close_conn_cursor(conn, cur)


def create_table(schema):
    """Create the yt_api table in the given schema.

    The staging schema stores raw per-video data, while other schemas
    (e.g. the warehouse) additionally track a Video_Type column.
    """
    conn, cur = get_conn_cursor()

    if schema == 'staging':
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );
            """
    else:
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Type" VARCHAR(10) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );
            """

    cur.execute(table_sql)

    conn.commit()

    close_conn_cursor(conn, cur)


def get_video_ids(cur, schema):
    """Fetch all video IDs already present in the given schema's yt_api table."""
    cur.execute(f"""SELECT "Video_ID" FROM {schema}. {table};""")
    ids = cur.fetchall()

    video_ids = [row['Video_ID'] for row in ids]

    return video_ids
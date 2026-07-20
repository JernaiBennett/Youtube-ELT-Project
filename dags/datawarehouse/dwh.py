# Airflow tasks that build the two-layer (staging -> core) data warehouse
# for YouTube API data: staging holds the raw pull, core holds the
# transformed/deduplicated version used for reporting.
from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():
    """Load raw YouTube API data into the staging schema, upserting rows
    and deleting any staging rows whose video ID no longer appears in the
    freshly loaded data."""

    schema = 'staging'

    conn,cur = None, None

    try:
        conn, cur = get_conn_cursor()

        YT_data = load_data()

        create_schema(schema)
        create_table(schema)

        # IDs already present in staging before this run, used to decide
        # insert vs. update and to detect rows that should be removed.
        table_ids = get_video_ids(cur, schema)

        for row in YT_data:
            #if first insertion
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)

            #add additional rows
            else:
                update_rows(cur,conn, schema, row)

            ids_in_json = {row['video_id'] for row in YT_data}

            # Videos that exist in staging but were not returned by this
            # API pull are considered stale and get removed.
            ids_to_delete = set(table_ids) - ids_in_json

            if ids_to_delete:
                delete_rows(cur, conn, schema, ids_to_delete)

            logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)

@task
def core_table():
    """Read the current staging data and sync it into the core schema,
    transforming each row and inserting/deleting to keep core in sync
    with staging."""

    schema = 'core'

    conn,cur = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        # IDs already present in core before this run.
        table_ids = get_video_ids(cur, schema)

        # Tracks which video IDs are still present in staging so we can
        # figure out what to delete from core afterwards.
        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        for row in rows:

            current_video_ids.add(row["Video_ID"])

            #if table is empty
            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)

            else:
                transformed_row = transform_data(row)

                if transformed_row["Video_ID"] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)

                else:
                    insert_rows(cur, conn, schema, transformed_row)

        # Core rows for videos no longer present in staging are removed.
        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)
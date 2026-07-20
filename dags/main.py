from airflow.decorators import dag
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

# Local timezone used to anchor the DAG's start_date and schedule
local_tz = pendulum.timezone("America/New_York")

# Default Airflow args applied to every task in the DAG
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email": "data@engineers.com",
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,               # only one run of this DAG at a time
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    #"end_date": datetime(2030, 12, 31, tzinfo=local_tz),
}

@dag(
    dag_id = 'produce_json',
    default_args=default_args,
    description= 'DAG to produce JSON file with raw data',
    schedule='0 14 * * *',   # runs daily at 14:00 (2 PM) local_tz
    catchup = False           # don't backfill runs for past schedule intervals
)
def etl_dag():
    # ETL pipeline: resolve the channel's uploads playlist, list its video IDs,
    # pull stats/metadata per video, then persist the result as JSON.
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    # Task dependencies: each step feeds the next
    playlist_id >> video_ids >> extract_data >> save_to_json_task

# Register the DAG with Airflow
etl_dag()
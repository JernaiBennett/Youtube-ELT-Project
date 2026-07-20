import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data():
    # Extraction task writes today's dump to this path, so loading assumes same-day execution.
    file_path = f".data/YT_data_{date.today()}.json"

    try:
        logger.info("Processing file: YT_data_{date.today()}")

        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)

        return data

    except FileNotFoundError:
        # Re-raise after logging so Airflow still marks the task as failed.
        logger.error(f"File not found:{file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise
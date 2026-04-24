import time
import pyodbc
import logging
from configs.db_config import get_db_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_connection():
    config = get_db_config()

    connection_string = (
        f"DRIVER={{{config.driver}}};"
        f"SERVER={config.server};"
        f"DATABASE={config.database};"
        f"UID={config.username};"
        f"PWD={config.password};"
    )

    max_retries = 3
    delay = 5

    for attempt in range(1, max_retries + 1):
        try:
            conn = pyodbc.connect(connection_string)
            logger.info("Database connection established successfully.")
            return conn

        except Exception as e:
            logger.warning(
                f"DB connection failed (attempt {attempt}/{max_retries}): {e}"
            )

            if attempt == max_retries:
                logger.error("All connection attempts failed.")
                raise

            time.sleep(delay)


if __name__ == "__main__":
    conn = get_connection()
    print("Connected successfully!")

    cursor = conn.cursor()
    rows = cursor.execute("SELECT TOP 5 * FROM DATA_ASSET").fetchall()

    for row in rows:
        print(row)
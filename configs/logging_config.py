"""
Logging configuration for the DQ Framework.
Sets up a rotating file handler writing to logs/dq_run.log.
Log format: [timestamp] [level] [module] message
"""

import os
import logging
from logging.handlers import TimedRotatingFileHandler


# ──────────────────────────────────────────────
# Log directory and file
# ──────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
LOG_FILE = os.path.join(LOG_DIR, "dq_run.log")

# Ensure logs directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# ──────────────────────────────────────────────
# Log format
# ──────────────────────────────────────────────
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure the root logger with a rotating file handler and console handler.

    Args:
        level: Logging level (default: INFO).

    Returns:
        None
    """
    root_logger = logging.getLogger()

    # Avoid adding duplicate handlers on repeated calls
    if root_logger.handlers:
        return

    root_logger.setLevel(level)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # ── File handler: daily rotation, keep 30 days ──
    file_handler = TimedRotatingFileHandler(
        filename=LOG_FILE,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    # ── Console handler ──
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

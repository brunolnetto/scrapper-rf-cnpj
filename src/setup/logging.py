import logging
import sys
from datetime import datetime
from dotenv import load_dotenv
from os import getenv, makedirs, path
from pythonjsonlogger import jsonlogger

from ..utils.logging import clear_latest_items

# Constants
LOG_FILES_HORIZON = 5
FIELDS = [
    "name",
    "process",
    "processName",
    "threadName",
    "thread",
    "taskName",
    "asctime",
    "created",
    "relativeCreated",
    "msecs",
    "pathname",
    "module",
    "filename",
    "funcName",
    "levelno",
    "levelname",
    "message",
]

# Load environment variables
load_dotenv()
ENVIRONMENT = getenv("ENVIRONMENT", "development")

# Get the root logger and configure it properly to prevent duplication
root_logger = logging.getLogger()

# Global flag to prevent multiple setup
_logging_configured = False

def configure_logging():
    """Configure logging once globally."""
    global _logging_configured, root_logger
    
    if _logging_configured:
        return
    
    # Clear all existing handlers on the root logger to prevent duplication
    root_logger.handlers.clear()
    
    # Set root logger level
    root_logger.setLevel(logging.DEBUG)
    
    # Formatters
    json_format = " ".join(map(lambda field_name: f"%({field_name})s", FIELDS))
    json_formatter = jsonlogger.JsonFormatter(json_format)
    console_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
    
    # Console handler for development
    if ENVIRONMENT == "development":
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)
    
    # File handlers
    date_str = datetime.now().strftime("%Y-%m-%d")
    time_str = datetime.now().strftime("%H_%M")
    log_root_path = f"logs/{date_str}"

    if path.exists(log_root_path):
        clear_latest_items(log_root_path, LOG_FILES_HORIZON)

    base_path = f"{log_root_path}/{time_str}"
    error_file = f"{base_path}/error_log.log"
    info_file = f"{base_path}/info_log.log"

    makedirs(path.dirname(error_file), exist_ok=True)
    makedirs(path.dirname(info_file), exist_ok=True)

    # Error file handler
    error_handler = logging.FileHandler(error_file, mode="a")
    error_handler.setFormatter(json_formatter)
    error_handler.setLevel(logging.ERROR)
    root_logger.addHandler(error_handler)
    
    # Info file handler (for INFO and WARNING)
    info_handler = logging.FileHandler(info_file, mode="a")
    info_handler.setFormatter(json_formatter)
    info_handler.setLevel(logging.INFO)
    root_logger.addHandler(info_handler)
    
    _logging_configured = True

# Configure logging on import
configure_logging()

# Create a logger for this module
logger = logging.getLogger(__name__)
logger.info("Logging started.")

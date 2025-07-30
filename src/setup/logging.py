import logging
import sys
from datetime import datetime
from dotenv import load_dotenv
from os import getenv, makedirs, path
from pythonjsonlogger import jsonlogger
from utils.logging import clear_latest_items

# Constants
LOG_FILES_HORIZON = 5
FIELDS = [
    "name", "process", "processName", "threadName", "thread", "taskName",
    "asctime", "created", "relativeCreated", "msecs", "pathname", "module",
    "filename", "funcName", "levelno", "levelname", "message"
]

# Load environment variables
load_dotenv()
ENVIRONMENT = getenv('ENVIRONMENT', 'development')

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set to the lowest level to capture all messages

# Formatter
logging_format = " ".join(map(lambda field_name: f"%({field_name})s", FIELDS))
formatter = jsonlogger.JsonFormatter(logging_format)

def setup_stream_handlers():
    """Setup stream handlers for development environment."""
    stdout_handler = logging.StreamHandler(sys.stdout)
    stderr_handler = logging.StreamHandler(sys.stderr)
    
    stdout_handler.setLevel(logging.INFO)
    stderr_handler.setLevel(logging.WARN)
    
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)

def setup_file_handlers():
    """Setup file handlers for logging."""
    date_str = datetime.now().strftime("%Y-%m-%d")
    time_str = datetime.now().strftime("%H_%M")
    log_root_path = f'logs/{date_str}'
    
    if path.exists(log_root_path):
        clear_latest_items(log_root_path, LOG_FILES_HORIZON)
    
    base_path = f"{log_root_path}/{time_str}"
    error_file = f"{base_path}/error_log.log"
    info_file = f"{base_path}/info_log.log"
    
    makedirs(path.dirname(error_file), exist_ok=True)
    makedirs(path.dirname(info_file), exist_ok=True)
    
    log_infos = [
        (error_file, logging.ERROR),
        (info_file, logging.INFO),
        (info_file, logging.WARN),
    ]
    
    for log_file, log_level in log_infos:
        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)

if ENVIRONMENT == 'development':
    setup_stream_handlers()

setup_file_handlers()

logger.info("Logging started.")

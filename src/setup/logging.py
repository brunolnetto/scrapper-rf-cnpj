import logging
import sys
import threading
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


class LoggingConfigurator:
    """
    Encapsulated logging configuration with environment-specific setups,
    thread-safe configuration, and flexible handler management.
    """

    def __init__(self, environment: str = None):
        self.environment = environment or getenv("ENVIRONMENT", "development")
        self.root_logger = logging.getLogger()
        self._configured = False
        self._lock = threading.Lock()

    def configure(self):
        """Configure logging once globally (thread-safe)."""
        with self._lock:
            if self._configured:
                return

            # Clear existing handlers
            self.root_logger.handlers.clear()
            self.root_logger.setLevel(logging.DEBUG)

            # Create formatters
            json_formatter = self._create_json_formatter()
            console_formatter = self._create_console_formatter()

            # Add handlers based on environment
            if self.environment == "development":
                console_handler = self._create_console_handler(console_formatter)
                self.root_logger.addHandler(console_handler)

            # Always add file handlers
            error_handler, info_handler = self._create_file_handlers(json_formatter)
            self.root_logger.addHandler(error_handler)
            self.root_logger.addHandler(info_handler)

            self._configured = True

    def _create_json_formatter(self) -> jsonlogger.JsonFormatter:
        """Create JSON formatter for structured logging."""
        json_format = " ".join(map(lambda field_name: f"%({field_name})s", FIELDS))
        return jsonlogger.JsonFormatter(json_format)

    def _create_console_formatter(self) -> logging.Formatter:
        """Create console formatter for development output."""
        return logging.Formatter('%(levelname)s:%(name)s:%(message)s')

    def _create_console_handler(self, formatter: logging.Formatter) -> logging.Handler:
        """Create console handler for development environment."""
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        return handler

    def _create_file_handlers(self, formatter: jsonlogger.JsonFormatter) -> tuple:
        """Create error and info file handlers with directory management."""
        # Create log directory structure
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

        # Error handler
        error_handler = logging.FileHandler(error_file, mode="a")
        error_handler.setFormatter(formatter)
        error_handler.setLevel(logging.ERROR)

        # Info handler
        info_handler = logging.FileHandler(info_file, mode="a")
        info_handler.setFormatter(formatter)
        info_handler.setLevel(logging.INFO)

        return error_handler, info_handler

    def get_logger(self, name: str) -> logging.Logger:
        """Get a configured logger for the given name."""
        self.configure()
        return logging.getLogger(name)

    def reconfigure(self, environment: str = None):
        """Reconfigure logging (useful for testing or runtime changes)."""
        if environment:
            self.environment = environment
        self._configured = False
        self.configure()


# Global instance for backward compatibility
_configurator = LoggingConfigurator()

# Backward compatibility functions
def configure_logging():
    """Legacy function for backward compatibility."""
    _configurator.configure()

def get_logger(name: str) -> logging.Logger:
    """Get a configured logger (new preferred method)."""
    return _configurator.get_logger(name)

# Configure on import
configure_logging()

# Create logger for this module
logger = get_logger(__name__)
logger.info("Logging started.")

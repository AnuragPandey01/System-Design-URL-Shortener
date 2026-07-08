import logging
import sys


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configures centralized logging for the URL Shortener application following standard practices.
    Sets stream formatting, log levels, and standard output handlers.
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

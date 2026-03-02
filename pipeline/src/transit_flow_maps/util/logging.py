"""Logging helpers for pipeline commands."""

import logging

_LOGGING_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure_logging(*, debug: bool = False) -> None:
    """Configure process-wide logging once."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=_LOGGING_FORMAT)


def get_logger(name: str) -> logging.Logger:
    """Return a logger instance for a module."""
    return logging.getLogger(name)

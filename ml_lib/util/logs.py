import logging


def configure_logging() -> None:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

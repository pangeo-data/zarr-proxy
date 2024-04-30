import logging
import sys


def get_logger() -> logging.Logger:
    logger = logging.getLogger('zarr-proxy')

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(levelname)s:     %(asctime)s  - %(name)s - %(message)s'),
    )
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger

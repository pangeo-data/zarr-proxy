import logging
import sys

from fastapi import FastAPI
from .api import ping


def get_logger() -> logging.Logger:
    logger = logging.getLogger("zarr-proxy")

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(levelname)s:     %(asctime)s  - %(name)s - %(message)s"),
    )
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger

logger = get_logger()


def create_application() -> FastAPI:
    application = FastAPI()
    application.include_router(ping.router, tags=["ping"])
  
    return application


app = create_application()


@app.on_event("startup")
async def startup_event():

    logger.info("Application startup...")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown...")
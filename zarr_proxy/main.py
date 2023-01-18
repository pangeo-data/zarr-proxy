from fastapi import FastAPI

from .api import dataset, ping
from .logging import get_logger


def create_application() -> FastAPI:
    application = FastAPI()
    application.include_router(ping.router, tags=["ping"])
    application.include_router(dataset.router, tags=["dataset"], prefix="/dataset")

    return application


app = create_application()


@app.on_event("startup")
async def startup_event():

    logger = get_logger()
    logger.info("Application startup...")


@app.on_event("shutdown")
async def shutdown_event():
    logger = get_logger()
    logger.info("Application shutdown...")

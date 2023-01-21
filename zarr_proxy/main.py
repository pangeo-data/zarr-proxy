from fastapi import FastAPI
from mangum import Mangum

from . import store
from .log import get_logger


def create_application() -> FastAPI:
    application = FastAPI()
    application.include_router(store.router, tags=["main"])

    return application


app = create_application()
handler = Mangum(app)


@app.on_event("startup")
async def startup_event():

    logger = get_logger()
    logger.info("Application startup...")


@app.on_event("shutdown")
async def shutdown_event():
    logger = get_logger()
    logger.info("Application shutdown...")

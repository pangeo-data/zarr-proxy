from fastapi import FastAPI
from mangum import Mangum

from .log import get_logger
from .store import router as store_router


def create_application() -> FastAPI:
    application = FastAPI()
    application.include_router(store_router, tags=["main"])

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


handler = Mangum(app, lifespan="off")

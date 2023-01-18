from fastapi import APIRouter, Depends

router = APIRouter()


@router.get("/ping")
async def ping() -> dict:
    return {
        "ping": "pong",
    }
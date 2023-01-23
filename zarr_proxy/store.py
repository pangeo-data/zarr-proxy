import json
import typing

import zarr
from fastapi import APIRouter, Header
from starlette.responses import Response

from .log import get_logger
from .logic import chunk_id_to_slice, chunks_from_string

router = APIRouter()
logger = get_logger()


def open_store(*, host: str, path: str) -> zarr.storage.FSStore:
    base_url = f"https://{host}/{path}"
    logger.info(f"Opening store: {base_url}")
    return zarr.storage.FSStore(base_url)


@router.get("/ping")
def ping() -> dict:
    return {"ping": "pong"}


@router.get("/{host}/{path:path}/.zmetadata")
def get_zmetadata(host: str, path: str) -> dict:
    store = open_store(host=host, path=path)
    return json.loads(store[".zmetadata"].decode())


@router.get("/{host}/{path:path}/.zattrs")
def get_zattrs(host: str, path: str) -> dict:
    store = open_store(host=host, path=path)
    return json.loads(store[".zattrs"].decode())


@router.get("/{host}/{path:path}/.zgroup")
def get_zgroup(host: str, path: str) -> dict:
    store = open_store(host=host, path=path)
    return json.loads(store[".zgroup"].decode())


@router.get("/{host}/{path:path}/.zarray")
def get_zarray(host: str, path: str, chunks: typing.Union[str, None] = Header(default=None)) -> dict:

    chunks = chunks_from_string(chunks)
    store = open_store(host=host, path=path)

    # Rewrite chunks
    meta = json.loads(store[".zarray"].decode())
    meta["chunks"] = chunks
    meta["compressor"] = None
    meta["filters"] = []
    return meta


@router.get("/{host}/{path:path}/{chunk_key}")
def get_chunk(
    host: str, path: str, chunk_key: str, chunks: typing.Union[str, None] = Header(default=None)
) -> bytes:

    logger.info(f"Getting chunk: {chunk_key}")
    logger.info(f"Chunks: {chunks}")
    logger.info(f'Host: {host}, Path: {path}')

    store = open_store(host=host, path=path)
    arr = zarr.open(store, mode="r")
    if chunks is None:
        logger.info("No chunks provided, returning full array")
        data = arr[:]
    else:
        logger.info("Returning chunk")
        chunks = chunks_from_string(chunks)
        data = arr[chunk_id_to_slice(chunk_key, chunks=chunks, shape=arr.shape)]

    return Response(data.tobytes(), media_type='application/octet-stream')

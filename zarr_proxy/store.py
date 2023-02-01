import json
import typing

import zarr
from fastapi import APIRouter, Header, HTTPException
from starlette.responses import Response

from .log import get_logger
from .logic import chunk_id_to_slice, parse_chunks_header

router = APIRouter()
logger = get_logger()


def open_store(*, host: str, path: str) -> zarr.storage.FSStore:
    base_url = f"https://{host}/{path}"
    logger.info(f"Opening store: {base_url}")
    return zarr.storage.FSStore(base_url)


@router.get("/health")
def ping() -> dict:
    return {"ping": "pong"}


@router.get("/{host}/{path:path}/.zmetadata")
def get_zmetadata(host: str, path: str, chunks: typing.Union[list[str], None] = Header(default=None)) -> dict:
    chunks = parse_chunks_header(chunks[0]) if chunks is not None else {}
    store = open_store(host=host, path=path)
    zmetadata = json.loads(store[".zmetadata"].decode())
    if chunks is None:
        # return zmetadata as is
        return zmetadata

    # Rewrite chunks in zmetadata
    for variable, chunks in chunks.items():
        try:
            zmetadata['metadata'][f'{variable}/.zarray']['chunks'] = chunks
        except KeyError:
            raise HTTPException(
                status_code=404,
                detail=f"Variable {variable} specified in chunks header: {chunks} is not found in the store",
            )

    return zmetadata


@router.get("/{host}/{path:path}/.zattrs")
def get_zattrs(host: str, path: str) -> dict:
    store = open_store(host=host, path=path)
    return json.loads(store[".zattrs"].decode())


@router.get("/{host}/{path:path}/.zgroup")
def get_zgroup(host: str, path: str) -> dict:
    store = open_store(host=host, path=path)
    return json.loads(store[".zgroup"].decode())


@router.get("/{host}/{path:path}/.zarray")
def get_zarray(host: str, path: str, chunks: typing.Union[list[str], None] = Header(default=None)) -> dict:

    store = open_store(host=host, path=path)
    # Rewrite chunks
    meta = json.loads(store[".zarray"].decode())
    chunks = parse_chunks_header(chunks[0]) if chunks is not None else {}
    variable = path.split("/")[-1]
    variable_chunks = chunks.get(variable, meta["chunks"])
    meta["chunks"] = variable_chunks
    meta["compressor"] = None
    meta["filters"] = []
    return meta


@router.get("/{host}/{path:path}/{chunk_key}")
def get_chunk(
    host: str, path: str, chunk_key: str, chunks: typing.Union[list[str], None] = Header(default=None)
) -> bytes:

    logger.info(f"Getting chunk: {chunk_key}")
    logger.info(f"Chunks: {chunks}")
    logger.info(f'Host: {host}, Path: {path}')

    chunks = parse_chunks_header(chunks[0]) if chunks is not None else {}
    variable = path.split("/")[-1]
    variable_chunks = chunks.get(variable, None)

    store = open_store(host=host, path=path)
    arr = zarr.open(store, mode="r")
    if variable_chunks is None:
        logger.info(f"No chunks provided, using the default chunks: {arr.chunks}")
        variable_chunks = arr.chunks

    else:
        logger.info(f"Using chunks provided: {variable_chunks}")

    try:
        logger.info(
            f"Getting chunk: {chunk_key} with chunks: {variable_chunks} from array with shape: {arr.shape}"
        )
        data_slice = chunk_id_to_slice(chunk_key, chunks=variable_chunks, shape=arr.shape)
    except IndexError as exc:
        # The chunk key is not valid or the chunks are not valid
        logger.error(exc)
        raise HTTPException(status_code=400, detail=str(exc))

    try:
        data = arr[data_slice]
        return Response(data.tobytes(), media_type='application/octet-stream')

    except ValueError as exc:
        message = f"Error getting chunk: {chunk_key} with chunks: {variable_chunks} from array with shape: {arr.shape}. Slice used: {data_slice}"
        logger.error(message)
        logger.error(exc)
        raise HTTPException(status_code=400, detail=message)

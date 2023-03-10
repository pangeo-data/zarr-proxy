import json
import typing

import zarr
from fastapi import APIRouter, Depends, Header, HTTPException
from starlette.responses import Response

from .config import Settings, format_bytes, get_settings
from .log import get_logger
from .logic import chunk_id_to_slice, parse_chunks_header

router = APIRouter()
logger = get_logger()


def open_store(*, host: str, path: str) -> zarr.storage.FSStore:
    base_url = f"https://{host}/{path}"
    logger.info(f"Opening store: {base_url}")
    return zarr.storage.FSStore(base_url)


@router.get("/health")
def ping(settings: Settings = Depends(get_settings)) -> dict:
    return {
        "ping": "pong",
        "zarr_proxy_payload_size_limit": format_bytes(settings.zarr_proxy_payload_size_limit),
    }


@router.get("/{host}/{path:path}/.zmetadata")
def get_zmetadata(host: str, path: str, chunks: typing.Union[list[str], None] = Header(default=None)) -> dict:
    chunks = parse_chunks_header(chunks[0]) if chunks is not None else {}
    store = open_store(host=host, path=path)
    zmetadata = json.loads(store[".zmetadata"].decode())

    # Rewrite chunks and compressor in zmetadata
    # TODO: we should probably add more validation here to make sure the specified variables
    # in the chunks header are actually in the zmetadata
    zmetadata_variables = set()
    for item in zmetadata["metadata"]:
        if item.endswith(".zarray"):
            variable = item.split("/")[0]
            zmetadata_variables.add(variable)
            variable_chunks = chunks.get(variable, zmetadata["metadata"][item]["chunks"])
            zmetadata["metadata"][item]["chunks"] = variable_chunks
            zmetadata["metadata"][item]['compressor'] = None

    # Check that all variables in the chunks header are in the zmetadata
    if not zmetadata_variables.issuperset(chunks.keys()):
        message = f"Invalid chunks header. Variables {sorted(chunks.keys() - zmetadata_variables)} not found in zmetadata: {sorted(zmetadata_variables)}"
        raise HTTPException(status_code=400, detail=message)

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
    host: str,
    path: str,
    chunk_key: str,
    chunks: typing.Union[list[str], None] = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> bytes:
    logger.info(f"Getting chunk: {chunk_key}")
    logger.info(f"Chunks: {chunks}")
    logger.info(f'Host: {host}, Path: {path}')
    logger.info(f"Settings: {settings}")

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
        size = data.nbytes
        # check that the size of the data does not exceed the maximum payload size
        if settings.zarr_proxy_payload_size_limit and (size > settings.zarr_proxy_payload_size_limit):
            message = f"Chunk with {format_bytes(size)} and shape {variable_chunks} exceeds server's payload size limit of {format_bytes(settings.zarr_proxy_payload_size_limit)}"
            logger.error(message)
            raise HTTPException(status_code=400, detail=message)

        return Response(data.tobytes(), media_type='application/octet-stream')

    except ValueError as exc:
        message = f"Error getting chunk: {chunk_key} with chunks: {variable_chunks} from array with shape: {arr.shape}. Slice used: {data_slice}"
        logger.error(message)
        logger.error(exc)
        raise HTTPException(status_code=400, detail=message)

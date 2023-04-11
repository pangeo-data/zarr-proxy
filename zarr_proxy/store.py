import traceback
import typing

import zarr
import zarr.errors
from fastapi import APIRouter, Depends, Header
from starlette.responses import Response

from .config import Settings, format_bytes, get_settings
from .exceptions import ZarrProxyHTTPException
from .helpers import format_exception, load_metadata_file, open_store
from .log import get_logger
from .logic import chunk_id_to_slice, parse_chunks_header

router = APIRouter()
logger = get_logger()


@router.get("/health")
def ping(settings: Settings = Depends(get_settings)) -> dict:
    return {
        "ping": "pong",
        "zarr_proxy_payload_size_limit": format_bytes(settings.zarr_proxy_payload_size_limit),
    }


@router.get("/{host}/{path:path}/.zmetadata")
def get_zmetadata(host: str, path: str, chunks: typing.Union[list[str], None] = Header(default=None)) -> dict:
    chunks = parse_chunks_header(chunks[0]) if chunks is not None else {}
    store = open_store(host=host, path=path, logger=logger)
    zmetadata = load_metadata_file(store=store, key=".zmetadata", logger=logger)

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
        details = {"message": message}
        raise ZarrProxyHTTPException(status_code=400, **details)

    return zmetadata


@router.get("/{host}/{path:path}/.zattrs")
def get_zattrs(host: str, path: str) -> dict:
    store = open_store(host=host, path=path, logger=logger)
    return load_metadata_file(store=store, key=".zattrs", logger=logger)


@router.get("/{host}/{path:path}/.zgroup")
def get_zgroup(host: str, path: str) -> dict:
    store = open_store(host=host, path=path, logger=logger)
    return load_metadata_file(store=store, key=".zgroup", logger=logger)


@router.get("/{host}/{path:path}/.zarray")
def get_zarray(host: str, path: str, chunks: typing.Union[list[str], None] = Header(default=None)) -> dict:
    store = open_store(host=host, path=path, logger=logger)
    # Rewrite chunks
    meta = load_metadata_file(store=store, key=".zarray", logger=logger)
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
    logger.info("Getting chunk: %s", chunk_key)
    logger.info("Chunks: %s", chunks)
    logger.info('Host: %s, Path: %s', host, path)
    logger.info("Settings: %s", settings)

    chunks = parse_chunks_header(chunks[0]) if chunks is not None else {}
    variable = path.split("/")[-1]
    variable_chunks = chunks.get(variable, None)

    store = open_store(host=host, path=path, logger=logger)
    try:
        arr = zarr.open(store, mode="r")
    except zarr.errors.PathNotFoundError as exc:
        logger.error(exc)
        details = {'message': 'Path not found', 'stack_trace': format_exception(traceback.format_exc())}
        raise ZarrProxyHTTPException(status_code=404, **details) from exc
    if variable_chunks is None:
        logger.info("No chunks provided, using the default chunks: %s", arr.chunks)
        variable_chunks = arr.chunks

    else:
        logger.info("Using chunks provided: %s, variable_chunks")

    try:
        logger.info(
            "Getting chunk: %s with chunks: %s from array with shape: %s",
            chunk_key,
            variable_chunks,
            arr.shape,
        )
        data_slice = chunk_id_to_slice(chunk_key, chunks=variable_chunks, shape=arr.shape)
    except IndexError as exc:
        # The chunk key is not valid or the chunks are not valid
        logger.error(exc)
        details = {
            'message': 'Invalid chunk key or chunks',
            'stack_trace': format_exception(traceback.format_exc()),
        }
        raise ZarrProxyHTTPException(status_code=400, **details)

    try:
        data = arr[data_slice]
        size = data.nbytes
        # check that the size of the data does not exceed the maximum payload size
        if settings.zarr_proxy_payload_size_limit and (size > settings.zarr_proxy_payload_size_limit):
            message = f"Chunk with {format_bytes(size)} and shape {variable_chunks} exceeds server's payload size limit of {format_bytes(settings.zarr_proxy_payload_size_limit)}"
            logger.error(message)
            details = {'message': message}
            raise ZarrProxyHTTPException(status_code=400, **details)

        return Response(data.tobytes(), media_type='application/octet-stream')

    except ValueError as exc:
        message = "Error getting chunk: %s with chunks: %s from array with shape: %s. Slice used: %s"
        details = {'message': message, 'stack_trace': format_exception(traceback.format_exc())}
        logger.error(message, chunk_key, variable_chunks, arr.shape, data_slice)
        raise ZarrProxyHTTPException(status_code=400, **details) from exc

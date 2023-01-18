import json

from fastapi import APIRouter
import zarr
from starlette.responses import Response

from ..logic import chunk_id_to_slice

router = APIRouter()


@router.get("/{host}/{path:path}/.zarray")
def get_dataset(host: str, path: str, chunks: str) -> dict:

    fname = ".zarray"
    chunks = tuple(int(c) for c in chunks.split(','))

    # return {"host": host, "path": path, "chunks": chunks}
    base_url = f"https://{host}/{path}"
    store = zarr.storage.FSStore(base_url)

    # Rewrite chunks
    meta = json.loads(store[".zarray"].decode())
    meta["chunks"] = chunks
    meta["compressor"] = {}
    meta["filters"] = []
    return meta


@router.get("/{host}/{path:path}/{chunk_key}")
def get_chunk(host: str, path: str, chunk_key: str, chunks: str) -> bytes:

    base_url = f"https://{host}/{path}"
    store = zarr.storage.FSStore(base_url)

    chunks = tuple(int(c) for c in chunks.split(','))

    arr = zarr.open(store, mode="r")
    data = arr[chunk_id_to_slice(chunk_key, chunks=chunks, shape=arr.shape)]
    response = Response(data.tobytes(), media_type='application/octet-stream')
    return response

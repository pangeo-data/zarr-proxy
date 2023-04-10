import json
import logging

import aiohttp.client_exceptions
import zarr
from fastapi import HTTPException


def load_metadata_file(*, store: zarr.storage.FSStore, key: str, logger: logging.Logger) -> dict:
    """Load the metadata file from the store.

    Parameters
    ----------
    store : zarr.storage.FSStore
        The store to load the metadata file from.
    key : str
        The key of the metadata file in the store.
    logger : logging.Logger
        The logger to use.

    Returns
    -------
    dict
        The metadata file as a dictionary.
    """
    try:
        return json.loads(store[key].decode())
    except KeyError as exc:
        logger.error('Key %s not found in store: %s', key, store)
        logger.error(exc)
        raise HTTPException(status_code=404, detail=f"{key} not found in store: {store.path}") from exc
    except aiohttp.client_exceptions.ClientResponseError as exc:
        logger.error('ClientResponseError: %s', exc)
        logger.error(dir(exc))
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc

    except Exception as exc:
        logger.error("An error occurred while loading metadata file: %s", exc)
        raise HTTPException(status_code=500, detail="An error occurred while loading metadata file.") from exc


def open_store(*, host: str, path: str, logger: logging.Logger) -> zarr.storage.FSStore:
    base_url = f"https://{host}/{path}"
    logger.info(f"Opening store: {base_url}")
    return zarr.storage.FSStore(base_url)

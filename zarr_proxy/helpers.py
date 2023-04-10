import json
import logging
import traceback

import aiohttp.client_exceptions
import zarr
from fastapi import HTTPException


def format_exception(exc: str) -> str:
    """Format an exception as a dictionary.

    Parameters
    ----------
    exc : Exception
        The exception to format.

    """
    return exc.splitlines()[-1]


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
    details = {}
    try:
        return json.loads(store[key].decode())
    except KeyError as exc:
        details = {
            'stack_trace': format_exception(traceback.format_exc()),
            'message': f'{key} not found in store: {store.path}',
        }
        raise HTTPException(status_code=404, detail=details) from exc
    except aiohttp.client_exceptions.ClientResponseError as exc:
        details = {'stack_trace': format_exception(traceback.format_exc()), 'message': exc.message}
        if exc.status == 403:
            details[
                'message'
            ] = f'Access denied to {store.path}. Make sure the dataset store supports public read access and has not been moved or deleted.'
        raise HTTPException(status_code=exc.status, detail=details) from exc

    except Exception as exc:
        logger.error("An error occurred while loading metadata file: %s", exc)
        details = {
            'stack_trace': format_exception(traceback.format_exc()),
            'message': 'An error occurred while loading metadata file.',
        }
        raise HTTPException(status_code=500, detail=details) from exc


def open_store(*, host: str, path: str, logger: logging.Logger) -> zarr.storage.FSStore:
    base_url = f"https://{host}/{path}"
    logger.info(f"Opening store: {base_url}")
    return zarr.storage.FSStore(base_url)

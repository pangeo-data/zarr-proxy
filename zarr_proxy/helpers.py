import json
import logging
import traceback

import aiohttp.client_exceptions
import zarr

from .exceptions import ZarrProxyHTTPException


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

    This function loads the metadata file from a given store and returns it as a dictionary.

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
        # Load the metadata file from the store and decode it from bytes to string, then parse it as JSON
        return json.loads(store[key].decode())

    except KeyError as exc:
        # If the specified key is not found in the store, raise an HTTPException with a 404 status code

        raise ZarrProxyHTTPException(
            status_code=404,
            stack_trace=format_exception(traceback.format_exc()),
            message=f'{key} not found in store: {store.path}',
        ) from exc

    except aiohttp.client_exceptions.ClientResponseError as exc:
        # If there is a client error loading the metadata (e.g. 403 Forbidden), raise an HTTPException with the same status code and detailed message
        details = {'stack_trace': format_exception(traceback.format_exc()), 'message': exc.message}
        if exc.status == 403:
            # S3 stores return a 403 Forbidden error if the client does not have permission to list the bucket contents
            # This makes it difficult to differentiate between a 404 error (object does not exist) and a 403 error
            # (object exists but client does not have permission to list contents of the bucket)
            # https://stackoverflow.com/questions/19037664/how-do-i-have-an-s3-bucket-return-404-instead-of-403-for-a-key-that-does-not-e
            details[
                'message'
            ] = f'Access denied to {store.path}. Make sure the dataset store supports public read access and has not been moved or deleted.'
        raise ZarrProxyHTTPException(status_code=exc.status, **details) from exc

    except Exception as exc:
        # If there is any other error while loading the metadata, log the error, raise an HTTPException with a 500 status code and a detailed message
        logger.error("An error occurred while loading metadata file: %s", exc)
        details = {
            'stack_trace': format_exception(traceback.format_exc()),
            'message': 'An error occurred while loading metadata file.',
        }
        raise ZarrProxyHTTPException(status_code=500, **details) from exc


def open_store(*, host: str, path: str, logger: logging.Logger) -> zarr.storage.FSStore:
    base_url = f"https://{host}/{path}"
    logger.info(f"Opening store: {base_url}")
    return zarr.storage.FSStore(base_url)

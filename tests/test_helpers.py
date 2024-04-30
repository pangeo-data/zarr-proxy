import logging
from unittest.mock import MagicMock, patch

import aiohttp
import pytest
import zarr

from zarr_proxy.exceptions import ZarrProxyHTTPException
from zarr_proxy.helpers import load_metadata_file, open_store


@pytest.fixture
def store_mock():
    store = MagicMock()
    store.path = '/test/store/path'
    return store


@pytest.fixture
def logger_mock():
    return MagicMock(spec=logging.Logger)


def test_load_metadata_file_success(store_mock, logger_mock):
    store_mock.__getitem__.return_value = b'{"test_key": "test_value"}'
    result = load_metadata_file(store=store_mock, key='metadata_key', logger=logger_mock)
    assert result == {'test_key': 'test_value'}


def test_load_metadata_file_key_error(store_mock, logger_mock):
    store_mock.__getitem__.side_effect = KeyError('metadata_key')
    with pytest.raises(ZarrProxyHTTPException) as exc_info:
        load_metadata_file(store=store_mock, key='metadata_key', logger=logger_mock)
    assert exc_info.value.status_code == 404
    message = exc_info.value.message
    assert message == 'metadata_key not found in store: /test/store/path'


def test_load_metadata_file_client_response_error(store_mock, logger_mock):
    request_info_mock = MagicMock()
    request_info_mock.real_url = 'http://example.com'
    history_mock = MagicMock()
    response_error = aiohttp.ClientResponseError(
        request_info=request_info_mock, history=history_mock, status=500
    )

    store_mock.__getitem__.side_effect = response_error
    with pytest.raises(ZarrProxyHTTPException) as exc_info:
        load_metadata_file(store=store_mock, key='metadata_key', logger=logger_mock)
    assert exc_info.value.status_code == 500


def test_load_metadata_file_general_error(store_mock, logger_mock):
    store_mock.__getitem__.side_effect = Exception('Unexpected error')
    with pytest.raises(ZarrProxyHTTPException) as exc_info:
        load_metadata_file(store=store_mock, key='metadata_key', logger=logger_mock)
    assert exc_info.value.status_code == 500
    assert exc_info.value.message == 'An error occurred while loading metadata file.'


def test_open_store(logger_mock):
    base_url = 'https://example.com/test_path'
    with MagicMock(spec=zarr.storage.FSStore) as fsstore_mock:
        with patch('zarr.storage.FSStore', return_value=fsstore_mock) as fsstore_constructor:
            result = open_store(host='example.com', path='test_path', logger=logger_mock)
            assert result == fsstore_mock
            fsstore_constructor.assert_called_once_with(base_url)

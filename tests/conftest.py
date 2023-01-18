import pytest
from fastapi.testclient import TestClient

from zarr_proxy.main import create_application


@pytest.fixture(scope="module")
def test_app():
    app = create_application()

    with TestClient(app) as test_client:
        yield test_client

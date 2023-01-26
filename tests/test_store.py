import pytest


@pytest.mark.parametrize(
    'store,chunks',
    [
        (
            'storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr/air',
            'lat=10,air=10,10',
        ),
        (
            'storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr/lat',
            'lat=5,air=10,10',
        ),
        (
            'storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr/lon',
            'lat=10,air=10,10',
        ),
    ],
)
def test_store_zarray(test_app, store, chunks):
    response = test_app.get(f"/{store}/.zarray", headers={'chunks': chunks})

    assert response.status_code == 200

    data = response.json()

    assert {
        'chunks',
        'compressor',
        'dtype',
        'filters',
        'fill_value',
        'order',
        'shape',
        'zarr_format',
    }.issubset(data.keys())


@pytest.mark.parametrize(
    'store',
    ['storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr'],
)
def test_store_zattrs(test_app, store):
    response = test_app.get(f"/{store}/air/.zattrs")
    assert response.status_code == 200

    assert {'units', 'long_name'}.issubset(response.json().keys())


@pytest.mark.parametrize(
    'store',
    ['storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr'],
)
def test_store_zmetadata(test_app, store):
    response = test_app.get(f"/{store}/.zmetadata")
    assert response.status_code == 200

    assert {'.zattrs', '.zgroup'}.issubset(response.json()['metadata'].keys())


@pytest.mark.parametrize(
    'store,chunk_key,chunks',
    [
        (
            'storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr/air',
            '0.0',
            'lat=10,air=10,10',
        ),
        (
            'storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr/lat',
            '0',
            'lat=10,air=10,10',
        ),
        (
            'storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr/lon',
            '1',
            'lat=10,air=10,10',
        ),
    ],
)
def test_store_array_chunk(test_app, store, chunk_key, chunks):
    response = test_app.get(f"/{store}/{chunk_key}", headers={'chunks': chunks})

    assert response.status_code == 200

    # get the bytes from the response
    data = response.content
    assert isinstance(data, bytes)

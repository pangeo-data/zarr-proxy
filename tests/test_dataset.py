import pytest


@pytest.mark.parametrize(
    'store',
    ['storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr'],
)
def test_dataset_zarray(test_app, store):
    response = test_app.get(f"/dataset/{store}/air/.zarray?chunks=10,10,10")
    assert response.status_code == 200

    assert {
        'chunks',
        'compressor',
        'dtype',
        'filters',
        'fill_value',
        'order',
        'shape',
        'zarr_format',
    }.issubset(response.json().keys())


@pytest.mark.parametrize(
    'store',
    ['storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr'],
)
def test_dataset_zattrs(test_app, store):
    response = test_app.get(f"/dataset/{store}/air/.zattrs")
    assert response.status_code == 200

    assert {'units', 'long_name'}.issubset(response.json().keys())


@pytest.mark.parametrize(
    'store',
    ['storage.googleapis.com/carbonplan-maps/ncview/demo/single_timestep/air_temperature.zarr'],
)
def test_dataset_zmetadata(test_app, store):
    response = test_app.get(f"/dataset/{store}/.zmetadata")
    assert response.status_code == 200

    assert {'.zattrs', '.zgroup'}.issubset(response.json()['metadata'].keys())

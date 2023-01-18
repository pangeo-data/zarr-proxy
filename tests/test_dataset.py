def test_dataset_get(test_app):
    response = test_app.get(
        "/dataset/s3.amazonaws.com/llc4320/LLC4320_2012-01-01T12:00:00.000000000Z.zarr/.zarray"
    )
    assert response.status_code == 200
    assert response.json() == {
        "host": "s3.amazonaws.com",
        "path": "llc4320/LLC4320_2012-01-01T12:00:00.000000000Z.zarr/.zarray",
        "fname": ".zarray",
    }

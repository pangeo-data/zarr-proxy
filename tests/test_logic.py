from zarr_proxy.logic import chunk_id_to_slice


def test_chunk_id_to_slice():

    shape = (2, 4)
    chunks = (1, 2)

    assert chunk_id_to_slice("0.0", shape=shape, chunks=chunks) == (slice(0, 1), slice(0, 2))
    assert chunk_id_to_slice("1.0", shape=shape, chunks=chunks) == (slice(1, 2), slice(0, 2))
    assert chunk_id_to_slice("1.1", shape=shape, chunks=chunks) == (slice(1, 2), slice(2, 4))

import pytest

from zarr_proxy.logic import chunk_id_to_slice, parse_chunks_header


def test_chunk_id_to_slice():

    shape = (2, 4)
    chunks = (1, 2)

    assert chunk_id_to_slice("0.0", shape=shape, chunks=chunks) == (slice(0, 1), slice(0, 2))
    assert chunk_id_to_slice("1.0", shape=shape, chunks=chunks) == (slice(1, 2), slice(0, 2))
    assert chunk_id_to_slice("1.1", shape=shape, chunks=chunks) == (slice(1, 2), slice(2, 4))


@pytest.mark.parametrize(
    "chunks, expected_output",
    [
        ("bed=10,10,prec=20,20,lat=5", {"bed": (10, 10), "prec": (20, 20), "lat": (5,)}),
        ("foo=1,1,bar=2,2", {"foo": (1, 1), "bar": (2, 2)}),
        ("a=1,b=2,c=3,d=4", {"a": (1,), "b": (2,), "c": (3,), "d": (4,)}),
        ("", {}),
        ("bed=10,10,prec=20,20,lat=five", {'bed': (10, 10), 'prec': (20, 20)}),
    ],
)
def test_parse_chunks_header(chunks, expected_output):
    output = parse_chunks_header(chunks)
    assert output == expected_output

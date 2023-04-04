import pytest

from zarr_proxy.logic import chunk_id_to_slice, parse_chunks_header, validate_chunks_info


@pytest.mark.parametrize(
    "shape, chunks, chunk_index, chunks_block_shape",
    [
        ((5, 5), (2, 2), (0, 0), (2, 2)),
        ((5, 5), (2, 2), (0, 1), (2, 2)),
        ((5, 5), (2, 2), (1, 0), (2, 2)),
        ((5, 5), (2, 2), (1, 1), (2, 2)),
    ],
)
def test_validate_chunks_info_valid(shape, chunks, chunk_index, chunks_block_shape):
    validate_chunks_info(
        shape=shape, chunks=chunks, chunk_index=chunk_index, chunks_block_shape=chunks_block_shape
    )


@pytest.mark.parametrize(
    "shape, chunks, chunk_index, chunks_block_shape",
    [
        ((5, 5), (2, 2), (0, 5), (2, 2)),
        ((5, 5), (2, 2), (5, 0), (2, 2)),
        ((5, 5), (2, 2), (5, 5), (2, 2)),
        ((5, 5), (3, 3), (2, 2), (2, 2)),
    ],
)
def test_validate_chunks_info_invalid(shape, chunks, chunk_index, chunks_block_shape):
    with pytest.raises(IndexError):
        validate_chunks_info(
            shape=shape, chunks=chunks, chunk_index=chunk_index, chunks_block_shape=chunks_block_shape
        )


def test_chunk_id_to_slice():
    shape = (2, 4)
    chunks = (1, 2)

    assert chunk_id_to_slice("0.0", shape=shape, chunks=chunks) == (slice(0, 1), slice(0, 2))
    assert chunk_id_to_slice("1.0", shape=shape, chunks=chunks) == (slice(1, 2), slice(0, 2))
    assert chunk_id_to_slice("1.1", shape=shape, chunks=chunks) == (slice(1, 2), slice(2, 4))


@pytest.mark.parametrize(
    "chunks, expected_output",
    [
        ("", {}),
        ("bed=10,10,prec=20,20,lat=5", {"bed": (10, 10), "prec": (20, 20), "lat": (5,)}),
        ("foo=1,1,bar=2,2", {"foo": (1, 1), "bar": (2, 2)}),
        ("Foo=1,1,BAR=2,2", {"Foo": (1, 1), "BAR": (2, 2)}),
        ("a=1,b=2,c=3,d=4", {"a": (1,), "b": (2,), "c": (3,), "d": (4,)}),
        ("", {}),
        ("bed=10,10,prec=20,20,lat=five", {'bed': (10, 10), 'prec': (20, 20)}),
        (
            'time=6443, analysed_sst=30,100,100, analysis_error=30,100,100, mask=30,100,100, sea-ice-fraction=30,100,100, sea surface temperate=30,30',
            {
                'time': (6443,),
                'analysed_sst': (30, 100, 100),
                'analysis_error': (30, 100, 100),
                'mask': (30, 100, 100),
                'sea-ice-fraction': (30, 100, 100),
                'sea surface temperate': (30, 30),
            },
        ),
        (
            "year=31, annual_maximum=1,128,128, days_exceeding_29=1,128,128, days_exceeding_30.5=1,128,128",
            {
                "year": (31,),
                "annual_maximum": (1, 128, 128),
                "days_exceeding_29": (1, 128, 128),
                "days_exceeding_30.5": (1, 128, 128),
            },
        ),
    ],
)
def test_parse_chunks_header(chunks, expected_output):
    output = parse_chunks_header(chunks)
    assert output == expected_output


def test_input_with_mixed_delimiters():
    chunks = "bed=10 10,prec=20;20,lat=5"
    expected = {"bed": (10, 10), "prec": (20, 20), "lat": (5,)}
    with pytest.raises(AssertionError):
        assert parse_chunks_header(chunks) == expected


def test_input_with_invalid_characters():
    chunks = "bed#=10,10,prec$=20,20,lat^=5"
    expected = {}
    assert parse_chunks_header(chunks) == expected

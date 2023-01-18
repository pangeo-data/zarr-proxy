"""Logic for the zarr proxy"""


def chunk_id_to_slice(
    chunk_key: str, *, chunks: tuple[int], shape: tuple[int], delimiter: str = "."
) -> tuple[slice]:
    """
    Given a Zarr chunk key, return the slice of an array to extract that data

    Parameters
    ----------
    chunk_key: the desired chunk, e.g. "1.3.2"
    chunks: the desired chunking
    shape: the total array shape
    delimiter: chunk separator character
    """
    chunk_index = tuple(int(c) for c in chunk_key.split("."))
    assert len(chunk_index) == len(chunks)
    assert len(shape) == len(chunks)
    # TODO: more error handling maybe?
    slices = tuple(
        (
            slice(min(c * ci, s), min(c * (ci + 1), s))
            for c, s, ci in zip(chunks, shape, chunk_index)
        )
    )
    return slices

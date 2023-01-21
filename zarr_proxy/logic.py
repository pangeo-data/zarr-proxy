"""Logic for the zarr proxy"""


def chunks_from_string(chunks: str) -> tuple[int]:
    """
    Given a string of comma-separated integers, return a tuple of ints

    Parameters
    ----------
    chunks: e.g. "1,2,3"
    """
    return tuple(int(c) for c in chunks.split(","))


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

    if len(chunk_index) != len(chunks):
        raise ValueError(f"The length of chunk_index: {chunk_index} and chunks: {chunks} must be the same.")

    if len(shape) != len(chunks):
        raise ValueError(f"The length of shape: {shape} and chunks: {chunks} must be the same.")

    # TODO: more error handling maybe?
    slices = tuple(
        (slice(min(c * ci, s), min(c * (ci + 1), s)) for c, s, ci in zip(chunks, shape, chunk_index))
    )
    return slices

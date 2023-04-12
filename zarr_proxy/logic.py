"""Logic for the zarr proxy"""

import functools
import math
import operator
import re
import typing


def parse_chunks_header(chunks: str) -> dict[str, tuple[int, ...]]:
    """Parse the chunks header into a dictionary of chunk keys and chunk sizes.

    This turns a string like "bed=10,10,prec=20,20,lat=5" into a dictionary like
    {"bed": (10, 10), "prec": (20, 20), "lat": (5,)}.

    Parameters
    ----------
    chunks: str
        e.g. "bed=10,10,prec=20,20,lat=5"

    Returns
    -------
    dict[str, tuple[int, ...]]
        e.g. {"bed": (10, 10), "prec": (20, 20), "lat": (5,)}


    """

    # This pattern matches one or more characters that are either lowercase or uppercase
    # letters (\w), whitespace (\s), underscore (_), or hyphen (-), followed by
    # the character = and then one or more digits (\d) or commas (,). The + following
    # each character class means to match one or more of these characters. The \b at the
    # end of the pattern is a word boundary and ensures that only complete words matching the pattern are returned.
    parsed_list = re.findall(r'([\w\s\._-]+=[\d,]+)\b', chunks)
    parsed_dict = {}
    for item in parsed_list:
        key, value = item.strip().split('=')
        parsed_dict[key] = tuple(map(int, value.strip(',').split(',')))
    return parsed_dict


def virtual_array_info(
    *, shape: tuple[int, ...], chunks: tuple[int, ...]
) -> dict[str, typing.Union[tuple[int, ...], int]]:
    """
    Return a dictionary of information about a virtual array. This is
    is useful for calculating the number of chunks needed when dividing an
    array into smaller blocks

    Parameters
    ----------
    shape: tuple[int]
        The shape of the array
    chunks: tuple[int]
        The desired chunking

    Returns
    -------
    dict[str, tuple[int] | int]

    """
    chunks_block_shape = tuple(math.ceil(s / c) for s, c in zip(shape, chunks)) if shape else 1
    nchunks = functools.reduce(operator.mul, chunks_block_shape, 1)

    return {"chunks_block_shape": chunks_block_shape, "num_chunks": nchunks}


def validate_chunks_info(
    *,
    shape=tuple[int, ...],
    chunks=tuple[int, ...],
    chunk_index=tuple[int, ...],
    chunks_block_shape=tuple[int, ...],
):
    """Validate the chunks info. Raise an error if the chunk_index is out of bounds

    Parameters
    ----------
    shape: tuple[int]
        The shape of the array
    chunks: tuple[int]
        The desired chunking
    chunk_index: tuple[int]
        The chunk index
    chunks_block_shape: tuple[int]
        The chunks block shape

    Returns
    -------
    None


    Raises
    ------
    IndexError
        If the chunk_index is out of bounds or lengths of chunk_index, chunks, or shape are inconsistent


    """
    # Check if lengths of chunk_index and chunks are the same
    if len(chunk_index) != len(chunks):
        raise IndexError(f"The length of chunk_index: {chunk_index} and chunks: {chunks} must be the same.")

    # Check if lengths of shape and chunks are the same
    if len(shape) != len(chunks):
        raise IndexError(f"The length of shape: {shape} and chunks: {chunks} must be the same.")

    # Check if the chunk_index is within the bounds of chunks_block_shape
    for index, dim_size in enumerate(chunks_block_shape):
        if chunk_index[index] < 0 or chunk_index[index] >= dim_size:
            raise IndexError(
                f"The chunk_index: {chunk_index} must be less than the chunks block shape: {chunks_block_shape}"
            )


def chunk_id_to_slice(
    chunk_key: str, *, chunks: tuple[int, ...], shape: tuple[int, ...], delimiter: str = "."
) -> tuple[slice]:
    """
    Given a Zarr chunk key, return the slice of an array to extract that data

    Parameters
    ----------
    chunk_key: str
        the desired chunk, e.g. "1.3.2"
    chunks: tuple
        the desired chunking
    shape: tuple
        the total array shape
    delimiter: str
        chunk separator character

    Returns
    -------
    tuple[slice]
        the slices to extract the chunk from the array
    """
    # Convert the chunk key string to a tuple of integers
    chunk_indices = tuple(int(chunk) for chunk in chunk_key.split(delimiter))

    # TODO: more error handling maybe?
    # Retrieve information about the virtual array
    array_info = virtual_array_info(shape=shape, chunks=chunks)
    validate_chunks_info(
        shape=shape,
        chunks=chunks,
        chunk_index=chunk_indices,
        chunks_block_shape=array_info["chunks_block_shape"],
    )
    # Generate slices for the given chunk indices
    slices = tuple(
        slice(min(chunk_size * chunk_index, dim_size), min(chunk_size * (chunk_index + 1), dim_size))
        for chunk_size, dim_size, chunk_index in zip(chunks, shape, chunk_indices)
    )
    return slices

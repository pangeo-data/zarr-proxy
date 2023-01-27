"""Logic for the zarr proxy"""

import re


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

    # ([a-zA-Z]+=[\d,]+) regular expression looks for one or more letters (both uppercase and lowercase)
    # followed by an equal sign and one or more digits and commas. The letters are grouped
    # together using square brackets and the "+" symbol specifies that there needs to be one
    # or more of them. The "A-Z" and "a-z" inside the square brackets specify that it should
    # match uppercase and lowercase letters. The equal sign and digits and commas are also matched
    # with the "+" symbol to match one or more of them. The entire match is wrapped in a capturing group,
    # indicated by the parentheses, which can be used to access the match in the code.

    parsed_list = re.findall(r'([a-zA-Z]+=[\d,]+)', chunks)
    parsed_dict = {}
    for item in parsed_list:
        key, value = item.split('=')
        parsed_dict[key] = tuple(map(int, value.strip(',').split(',')))
    return parsed_dict


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

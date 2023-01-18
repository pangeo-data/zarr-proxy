from fastapi import APIRouter

router = APIRouter()


@router.get("/{host}/{path:path}")
def get_dataset(host: str, path: str) -> dict:
    fname = path.split("/")[-1]
    if fname in ('.zattrs', '.zgroup'):
        # Pass through unchanged
        pass
    elif fname == '.zarray':
        # Rewrite chunks
        pass

    else:
        # This is a chunk
        pass

    return {
        "host": host,
        "path": path,
        "fname": fname,
    }

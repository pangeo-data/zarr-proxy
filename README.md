# zarr-proxy

_✨ This code is highly experimental! Let the buyer beware ⚠️ ;) ✨_

| CI          | [![GitHub Workflow Status][github-ci-badge]][github-ci-link] [![Code Coverage Status][codecov-badge]][codecov-link] [![pre-commit.ci status][pre-commit.ci-badge]][pre-commit.ci-link] |
| :---------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
| **Docs**    |                                                                     [![Documentation Status][rtd-badge]][rtd-link]                                                                     |
| **Package** |                                                          [![Conda][conda-badge]][conda-link] [![PyPI][pypi-badge]][pypi-link]                                                          |
| **License** |                                                                         [![License][license-badge]][repo-link]                                                                         |

A proxy for zarr stores that allows for chunking overrides. This is useful for clients that want to request data in a specific chunking scheme, but the data is stored in a different chunking scheme (e.g. a dataset stored in a chunking scheme that is optimized for fast reading, but the client wants to request data in a chunking scheme that is optimized for fast rendering). One advantage of using a proxy is that we don't need to persistently store the data in multiple chunking schemes. Instead, we can simply request the data in the desired chunking scheme on the fly.

## Usage

The proxy is a simple [FastAPI](https://fastapi.tiangolo.com/) application. It can be run locally using the following command:

```bash
uvicorn zarr_proxy.main:app --reload
```

Once the proxy is running, you can use it to access a zarr store by using the following URL pattern: `http://{PROXY_ADDRESS}/{ZARR_STORE_ADDRESS}`. For example, if the proxy is running on `localhost:8000` and you want to access the zarr store at `https://my.zarr.store`, you would use the following URL: `http://localhost:8000/my.zarr.store`.

The proxy supports the following HTTP headers:

- `chunks`: A comma-separated list of chunk overrides. Each chunk override is of the form `{variable}={shape}`, where `variable` is the name of the variable to override and `shape` is the shape of the chunks to use for that variable. For example, `chunks=temperature=256,256,30,pressure=256,256,30` would override the chunking of the `temperature` and `pressure` variables to be 256x256x30 and 256x256x30, respectively. If a variable is not specified in the `chunks` header, the chunking of that variable will not be overridden.

### Python client

Before constructing the `chunks` header, a Python client might inspect the dataset `.zmetadata` to determine the existing chunking of each variable. This can be done using the [requests](https://requests.readthedocs.io/en/master/) library:

```python
import requests

proxy_zarr_store = 'http://localhost:8000/my.zarr.store'
# get zmetadata
zmetadata = requests.get(f'{proxy_zarr_store}/.zmetadata').json()
print(zmetadata)
```

Once the `.zmetadata` has been retrieved, the client can construct the `chunks` header. For example, the following code will construct a `chunks` header that overrides the chunking of `temperature` and `pressure` variables(arrays) to be 256x256x30:

```python
chunks='temperature=256,256,30,pressure=256,256,30'
```

We can then use the `chunks` header to construct a `zarr` store and by passing the `chunks` header to the `client_kwargs` argument of the `zarr.storage.FSStore` constructor:

```python
import zarr
store = zarr.storage.FSStore(proxy_zarr_store, client_kwargs={'headers': {"chunks": chunks}})
```

This store can be then used via the [xarray](http://xarray.pydata.org/en/stable/) library:

```python
import xarray as xr
ds = xr.open_dataset(store, engine='zarr', chunks={})
```

### JavaScript client

A web-based client might prefetch and inspect dataset `.zmetadata` before constructing a [`Headers`](https://developer.mozilla.org/en-US/docs/Web/API/Headers/Headers) object with desired `chunks` header(s) to pass on to a Zarr client.

In this example, the `getHeaders()` constructor includes `chunks` headers for all variables whose existing chunking does not meet the use-case-specific chunk "cap" requirements:

```js
const getHeaders = (variables, zmetadata, axes) => {
  const headers = []

  variables.forEach((variable) => {
    const existingChunks = zmetadata.metadata[`${variable}/.zarray`].chunks
    const dims = zmetadata.metadata[`${variable}/.zattrs`]['_ARRAY_DIMENSIONS']
    const { X, Y } = axes[variable]

    // cap spatial dimensions at length 256, cap non-spatial dimensions at length 30
    const limits = dims.map((d) => ([X, Y].includes(d) ? 256 : 30))
    const override = getChunkShapeOverride(existingChunks, limits)

    if (override) {
      shape.push(['chunks', `${variable}=${override.join(',')}`])
    }
  })

  return new Headers(headers)
}
```

[github-ci-badge]: https://github.com/pangeo-data/zarr-proxy/actions/workflows/main.yaml/badge.svg
[github-ci-link]: https://github.com/pangeo-data/zarr-proxy/actions/workflows/main.yaml
[codecov-badge]: https://img.shields.io/codecov/c/github/pangeo-data/zarr-proxy.svg?logo=codecov
[codecov-link]: https://codecov.io/gh/pangeo-data/zarr-proxy
[rtd-badge]: https://img.shields.io/readthedocs/zarr-proxy/latest.svg
[rtd-link]: https://zarr-proxy.readthedocs.io/en/latest/?badge=latest
[pypi-badge]: https://img.shields.io/pypi/v/zarr-proxy?logo=pypi
[pypi-link]: https://pypi.org/project/zarr-proxy
[conda-badge]: https://img.shields.io/conda/vn/conda-forge/zarr-proxy?logo=anaconda
[conda-link]: https://anaconda.org/conda-forge/zarr-proxy
[license-badge]: https://img.shields.io/github/license/pangeo-data/zarr-proxy
[repo-link]: https://github.com/pangeo-data/zarr-proxy
[pre-commit.ci-badge]: https://results.pre-commit.ci/badge/github/pangeo-data/zarr-proxy/main.svg
[pre-commit.ci-link]: https://results.pre-commit.ci/latest/github/pangeo-data/zarr-proxy/main

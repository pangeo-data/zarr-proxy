# zarr-proxy

## Client Examples

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

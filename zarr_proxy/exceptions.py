import http
import typing

from fastapi.requests import Request
from fastapi.responses import JSONResponse


class ZarrProxyHTTPException(Exception):
    """Base class for all exceptions raised by this package."""

    def __init__(
        self,
        status_code: int,
        message: typing.Optional[str] = None,
        stack_trace: typing.Optional[str] = None,
    ):
        self.status_code = status_code
        if message is None:
            self.message = http.HTTPStatus(status_code).phrase
        else:
            self.message = message
        self.stack_trace = stack_trace

    def __repr__(self):
        return f'{self.__class__.__name__}(status_code={self.status_code!r}, message={self.message!r}, stack_trace={self.stack_trace!r})'


async def zarr_proxy_http_exception_handler(
    request: Request,
    exc: ZarrProxyHTTPException,
) -> JSONResponse:
    """Handle exceptions raised by this package."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            'message': exc.message,
            'stack_trace': exc.stack_trace,
        },
    )

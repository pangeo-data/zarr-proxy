import logging

from mangum import Mangum

from zarr_proxy.main import app

logging.getLogger('mangum.lifespan').setLevel(logging.ERROR)
logging.getLogger('mangum.http').setLevel(logging.ERROR)

handler = Mangum(app, lifespan='auto')

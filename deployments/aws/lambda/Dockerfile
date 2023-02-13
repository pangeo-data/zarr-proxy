ARG PYTHON_VERSION=3.9

FROM --platform=linux/amd64 public.ecr.aws/lambda/python:${PYTHON_VERSION}

WORKDIR /tmp

# Install system dependencies e.g. git
RUN yum install -y git

RUN python -m pip install pip -U
RUN python -m pip install "git+https://github.com/pangeo-data/zarr-proxy.git" "mangum>=0.17.0" -t /asset --no-binary pydantic

# Reduce package size and remove useless files
RUN cd /asset && find . -type f -name '*.pyc' | while read f; do n=$(echo $f | sed 's/__pycache__\///' | sed 's/.cpython-[2-3][0-9]//'); cp $f $n; done;
RUN cd /asset && find . -type d -a -name '__pycache__' -print0 | xargs -0 rm -rf
RUN cd /asset && find . -type f -a -name '*.py' -print0 | xargs -0 rm -f
RUN find /asset -type d -a -name 'tests' -print0 | xargs -0 rm -rf
RUN rm -rdf /asset/numpy/doc/ /asset/boto3* /asset/botocore* /asset/bin /asset/geos_license /asset/Misc

COPY lambda/handler.py /asset/handler.py

CMD ["echo", "hello world"]

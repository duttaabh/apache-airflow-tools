FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.11

# Install system dependencies for psycopg2
RUN yum install -y postgresql-devel gcc python3-devel

# Install all Lambda dependencies: psycopg2-binary, python-jose, and requests
RUN pip install --upgrade pip && \
    pip install psycopg2-binary==2.9.9 python-jose requests -t /python --no-cache-dir

# Create the layer structure
RUN mkdir -p /layer/python && cp -r /python/* /layer/python/

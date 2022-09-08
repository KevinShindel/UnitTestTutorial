FROM python:3.10-alpine
MAINTAINER "Kevin Shindel"
RUN mkdir test
WORKDIR test

COPY requirements.txt .
COPY setup.py .

RUN pip install --upgrade pip \
    && pip install -e . \
    && pip3 install -r requirements.txt --no-cache-dir
CMD "pytest"
ENV PYTHONDONTWRITEBYTECODE=true
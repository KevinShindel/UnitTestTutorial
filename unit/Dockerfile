FROM python:3.10-alpine
MAINTAINER "Kevin Shindel"
RUN mkdir "/opt/unit" -p
#RUN pip install coverage --no-cache-dir
WORKDIR /opt/unit
COPY entrypoint.sh ../
COPY requirements.txt ../
RUN pip install --upgrade pip \
    && pip3 install -r ../requirements.txt --no-cache-dir
RUN ["chmod", "+x", "../entrypoint.sh"]

ENV MODE=COVERAGE
ENTRYPOINT ["/opt/entrypoint.sh"]
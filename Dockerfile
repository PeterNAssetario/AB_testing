# syntax=docker/dockerfile:experimental
ARG PYTHON_VERSION=3.8
FROM python:${PYTHON_VERSION}-slim as base

RUN apt update -y && apt install -y git

RUN pip3 install pip==21.1.3

RUN mkdir -p -m 0600 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

# ----------------------------------------
FROM base as dev

RUN apt update -y && apt install -y tar gzip git awscli groff curl zip

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "./awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -r ./aws "./awscliv2.zip"

# ----------------------------------------
FROM base as prod

ADD requirements.txt ./
RUN --mount=type=ssh pip3 install -r requirements.txt

COPY scripts/entrypoint.sh /opt/app/entrypoint.sh
COPY main_testing_script.py /opt/app/main_testing_script.py
COPY bayesian_testing/ /opt/app/bayesian_testing/

WORKDIR /opt/app
ENV PYTHONPATH /opt/app
ENV PYTHONUNBUFFERED 1

CMD /opt/app/entrypoint.sh
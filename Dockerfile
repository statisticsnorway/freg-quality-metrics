FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.2.2 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

# prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

RUN apt-get update \
  && apt-get install -y python3-pip python3-dev build-essential curl \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/app

# install poetry - respects $POETRY_VERSION & $POETRY_HOME
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl -sSL https://install.python-poetry.org | python3 -

# copy project requirement files here to ensure they will be cached.
WORKDIR $PYSETUP_PATH
COPY poetry.lock pyproject.toml ./

# install runtime deps - uses $POETRY_VIRTUALENVS_IN_PROJECT internally
RUN poetry install --no-root --only main

RUN mkdir freg_quality_metrics
COPY freg_quality_metrics freg_quality_metrics
RUN poetry install --only main

RUN mkdir /app
WORKDIR /app

COPY ./bin/run.sh /app/bin/run.sh
COPY run.py /app

# Uncomment the section below when testing locally with Docker
# Create and copy a service account key for the account
# qa-metrics-wi@dev-freg-3896.iam.gserviceaccount.com and store as service-key-dev.json
# COPY ./service-key-dev.json /app/service-key.json
# ENV GCP_PROJECT=dev-freg-3896
# ENV GOOGLE_APPLICATION_CREDENTIALS=service-key.json

# Create a non-root user
RUN useradd -ms /bin/bash aurora

#USER 9000

EXPOSE 8080
ENTRYPOINT ["bash", "/app/bin/run.sh"]

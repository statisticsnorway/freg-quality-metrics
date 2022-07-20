FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.1.13 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

# prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

RUN apt-get update \
  && apt-get install -y python3-pip python3-dev curl \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# install poetry - respects $POETRY_VERSION & $POETRY_HOME
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python -

# copy project requirement files here to ensure they will be cached.
WORKDIR $PYSETUP_PATH
COPY poetry.lock pyproject.toml ./

# install runtime deps - uses $POETRY_VIRTUALENVS_IN_PROJECT internally
RUN poetry install --no-dev

COPY ./app .

USER 9000

CMD ["gunicorn", "main:app", "-b", "0.0.0.0:8080", "-w", "1","-k", "uvicorn.workers.UvicornWorker", "-t", "0", "--log-config", "logging.config", "--log-level", "info"]

EXPOSE 8080
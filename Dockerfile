FROM python:3.11-alpine

RUN adduser -u 10000 -D nextflow

RUN pip install poetry==2.2.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN touch README.md

RUN poetry install --without dev --no-root && rm -rf $POETRY_CACHE_DIR

COPY src ./src
#COPY .env ./

# Make port 8080 available to the world outside this container
EXPOSE 8000



RUN chown -R nextflow:nextflow /app
USER 10000:10000

ENTRYPOINT ["poetry", "run", "python", "-m", "src.main"]
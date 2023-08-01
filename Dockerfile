FROM europe-west1-docker.pkg.dev/mgr-analyticsb2b-prod-w1dv/containers/poetry-git:1.2-3.8 as build
WORKDIR /tmp
COPY poetry.lock pyproject.toml /tmp/

RUN mkdir -p /venv && \
    python3 -m venv /venv --without-pip && \
    . /venv/bin/activate && \
    poetry check --no-ansi && \
    git config --global url."https://gitlab-ci-token:$(cat /kaniko/CI_JOB_TOKEN)@gitlab.com".insteadOf https://gitlab.com && \
    poetry config --no-ansi -n http-basic.analyticsb2b__apps__common gitlab-ci-token "$(cat /kaniko/CI_JOB_TOKEN)" && \
    poetry config --no-ansi -n http-basic.analyticsb2b__apps__apps_base gitlab-ci-token "$(cat /kaniko/CI_JOB_TOKEN)" && \
    poetry install --no-root --only main --no-ansi -n && \
    rm ~/.config/pypoetry/auth.toml && \
    pip3 check

FROM europe-west1-docker.pkg.dev/mgr-platform-prod-khsu/image-hub/dockerhub/python:3.8-slim-bullseye
LABEL maintainer="Analytics Suite"
ENV PYTHONPATH="${PYTHONPATH}:/src" \
    PYTHONUNBUFFERED=1
RUN mkdir -p /airflow/xcom
RUN chown nobody:nogroup -R /airflow/xcom
WORKDIR /src/dwd
ENV PATH=/venv/bin:$PATH
COPY --from=build /venv /venv
COPY --chown=nobody:nogroup dwd /src/dwd
USER nobody

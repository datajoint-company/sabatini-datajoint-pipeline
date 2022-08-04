ARG PY_VER
ARG WORKER_BASE_HASH
FROM datajoint/djbase:py${PY_VER}-debian-${WORKER_BASE_HASH}

USER root
RUN apt update && \
    apt-get install -y ssh git

USER anaconda:anaconda

ARG REPO_OWNER
ARG REPO_NAME
WORKDIR $HOME

# Clone the workflow
RUN git clone https://github.com/datajoint-company/sciops-dev_sabatini.git

# Install the workflow
RUN pip install ./${REPO_NAME}
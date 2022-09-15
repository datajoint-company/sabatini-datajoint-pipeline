ARG PY_VER
ARG WORKER_BASE_HASH
FROM datajoint/djbase:py${PY_VER}-debian-${WORKER_BASE_HASH}

USER root
RUN apt-get update && \
    apt-get install -y ssh git vim nano

RUN pip install pyzmq==23.2.1

USER anaconda:anaconda

ARG REPO_OWNER
ARG REPO_NAME
WORKDIR $HOME

# Clone the workflow
RUN git clone -b sciops-dev https://github.com/datajoint-company/sciops-dev_sabatini.git

# Install C++ compilers for CaImAn
RUN cp ./${REPO_NAME}/apt_requirements.txt /tmp/
RUN /entrypoint.sh echo "Installed dependencies."

# Install the workflow 
# ssh keyscan required due to "support" pipeline dependency being private
ARG DEPLOY_KEY
COPY --chown=anaconda $DEPLOY_KEY $HOME/.ssh/id_ed25519
RUN ssh-keyscan github.com >> $HOME/.ssh/known_hosts && \
    pip install ./${REPO_NAME}




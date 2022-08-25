ARG PY_VER
ARG WORKER_BASE_HASH
FROM datajoint/djbase:py${PY_VER}-debian-${WORKER_BASE_HASH}

USER root
RUN apt-get update && \
    apt-get install -y ssh git vim nano

USER anaconda:anaconda

ARG REPO_OWNER
ARG REPO_NAME
WORKDIR $HOME

# Install Deeplabcut
RUN git clone https://github.com/DeepLabCut/DeepLabCut.git
RUN pip install ./DeepLabCut

# Clone the workflow
RUN git clone -b sciops-dev https://github.com/datajoint-company/sciops-dev_sabatini.git

# Install the workflow
ARG DEPLOY_KEY
COPY --chown=anaconda $DEPLOY_KEY $HOME/.ssh/id_ed25519
RUN ssh-keyscan github.com >> $HOME/.ssh/known_hosts && \
    pip install ./${REPO_NAME}

# Install C++ compilers for CaImAn
RUN cp ./${REPO_NAME}/apt_requirements.txt /tmp/
RUN /entrypoint.sh echo "Installed dependencies."


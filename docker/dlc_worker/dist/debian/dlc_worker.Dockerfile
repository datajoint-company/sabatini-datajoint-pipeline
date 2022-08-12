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

# Install Deeplabcut
RUN git clone https://github.com/DeepLabCut/DeepLabCut.git
RUN pip install ./DeepLabCut

# Clone the workflow
RUN git clone https://github.com/datajoint-company/sciops-dev_sabatini.git

# Install C++ compilers for CaImAn
RUN cp ./${REPO_NAME}/apt_requirements.txt /tmp/
RUN /entrypoint.sh echo "Installed dependencies."

# Install Facemap
RUN git clone https://github.com/MouseLand/facemap.git
RUN pip install ./facemap

# Install the workflow
RUN pip install ./${REPO_NAME}



ARG PY_VER
ARG DIST
FROM datajoint/djbase:py${PY_VER}-${DIST}

USER root
RUN apt update && \
    apt-get install -y ssh git

USER anaconda:anaconda
ARG DEPLOY_KEY
COPY --chown=anaconda --chmod=700 $DEPLOY_KEY $HOME/.ssh/sciops_deploy.ssh

ARG REPO_OWNER
ARG REPO_NAME
WORKDIR $HOME
RUN ssh-keyscan github.com >> ~/.ssh/known_hosts && \
    GIT_SSH_COMMAND="ssh -i $HOME/.ssh/sciops_deploy.ssh" \
    git clone git@github.com:${REPO_OWNER}/${REPO_NAME}.git

RUN GIT_SSH_COMMAND="ssh -i $HOME/.ssh/sciops_deploy.ssh" \
    pip install ./${REPO_NAME}
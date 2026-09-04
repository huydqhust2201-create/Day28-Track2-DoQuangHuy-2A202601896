FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

# The base image ships an unsigned dl.yarnpkg.com apt source that this
# Python-only project does not need. Left in place it breaks `apt-get
# update` inside the docker-in-docker feature's own install step, which
# silently falls the whole devcontainer build back to a bare recovery
# container with no uv and no Docker CLI.
RUN rm -f /etc/apt/sources.list.d/yarn.list

RUN python -m pip install --no-cache-dir uv==0.12.9

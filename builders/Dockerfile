FROM ubuntu:24.04 AS buildtime

ARG TARGETARCH

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# ---- Build dependencies and some runtime tools

RUN apt-get update && apt-get install -y --no-install-recommends \
        # Core build tools \
        build-essential \
        gfortran \
        cmake ninja-build pkg-config ccache \
        clang llvm libclang-dev clang-tools \
        binutils-dev gperf libtool \
        # Version control & downloads \
        git git-lfs \
        wget curl \
        ca-certificates \
        # Compression & utilities \
        bzip2 \
        xz-utils \
        zlib1g-dev \
        libbz2-dev \
        liblz4-dev \
        liblzma-dev \
        # Libraries \
        libssl-dev \
        libcap-dev \
        libevent-dev \
        libjemalloc-dev \
        libsnappy-dev \
        libiberty-dev \
        libc-ares-dev \
        libsodium-dev \
        libdouble-conversion-dev \
        libboost-all-dev \
        libgflags-dev \
        libgoogle-glog-dev \
        libkrb5-dev \
        libsasl2-dev \
        libnuma-dev \
        # Python/build deps \
        libreadline-dev \
        libsqlite3-dev \
        sqlite3 \
        libncursesw5-dev \
        tk-dev \
        libxml2-dev \
        libxmlsec1-dev \
        libffi-dev \
        # Tools/editors \
        tmux htop neovim nano mc jq && \
    apt-get remove -y xserver-xorg-legacy && \
    apt remove -y cmake && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*
RUN if [ "${TARGETARCH}" = "amd64" ]; then export CMAKE_ARCH="x86_64"; else export CMAKE_ARCH="aarch64"; fi && \
    wget https://github.com/Kitware/CMake/releases/download/v3.31.11/cmake-3.31.11-linux-${CMAKE_ARCH}.sh && \
    chmod +x cmake-3.31.11-linux-${CMAKE_ARCH}.sh && \
    ./cmake-3.31.11-linux-${CMAKE_ARCH}.sh --skip-license --exclude-subdir --prefix=/usr/local && \
    apt-get clean && \
    apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false && \
    rm -rf /var/lib/apt/lists/* \
    && rm cmake-3.31.11-linux-${CMAKE_ARCH}.sh
RUN wget https://archive.ubuntu.com/ubuntu/ubuntu/pool/universe/f/fast-float/libfast-float-dev_8.0.0-0.1_amd64.deb && \
    dpkg -i libfast-float-dev_8.0.0-0.1_amd64.deb

# ---- Create user home and install Python

ARG UID=20000
ARG GID=20000

RUN groupadd -g ${GID} actionengine && \
    useradd --create-home --shell /bin/bash \
      --system -u ${UID} -g ${GID} actionengine && \
    mkdir -p /opt/actionengine/actionengine && \
    chown -R actionengine:actionengine /opt/actionengine && \
    chmod -R g+rw /opt/actionengine

USER actionengine

ENV HOME=/home/actionengine
ENV PYENV_ROOT=/home/actionengine/.pyenv
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"
RUN git clone https://github.com/pyenv/pyenv ${PYENV_ROOT}

ARG PYTHON_VERSION=3.12.6
RUN pyenv install ${PYTHON_VERSION} && \
    pyenv global ${PYTHON_VERSION} && \
    pyenv rehash && \
    python -m ensurepip && \
    python -m pip install --no-cache-dir -U pip pip-tools && \
    pip cache purge

# ---- Build dependencies and some runtime tools

USER root
COPY --chown=actionengine:actionengine ./.git /opt/actionengine/actionengine/.git
USER actionengine
RUN cd /opt/actionengine/actionengine && \
    git submodule update --init --recursive

# ---- Build Proxygen (would be great to refactor into a build step: it's long!)
USER root
COPY --chown=actionengine:actionengine ./third_party/proxygen /opt/actionengine/actionengine/third_party/proxygen
RUN cd /opt/actionengine/actionengine/third_party/proxygen/proxygen && \
    ./build.sh
RUN cd /opt/actionengine/actionengine/third_party/proxygen/proxygen && \
    ./install.sh

# ---- Build Action Engine in unpriviledged mode

COPY --chown=actionengine:actionengine . /opt/actionengine/actionengine
USER actionengine

ARG CMAKE_BUILD_TYPE=Release
ENV CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}

WORKDIR /opt/actionengine/actionengine

RUN cd /opt/actionengine/actionengine && \
    git config --global --add safe.directory /opt/actionengine/actionengine && \
    pip install build wheel && \
    python -m build -w

#RUN cd /opt/actionengine/actionengine && \
#    git config --global --add safe.directory /opt/actionengine/actionengine && \
#    ./scripts/configure.sh
#
#RUN cd /opt/actionengine/actionengine && \
#    ./scripts/build_python.sh
#
#RUN mkdir -p /opt/actionengine/py_build && \
#    cd /opt/actionengine/py_build && \
#    cp /opt/actionengine/actionengine/py/actionengine/*.so /opt/actionengine/py_build
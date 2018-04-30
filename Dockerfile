FROM ubuntu:xenial

ADD . /rain
WORKDIR /rain

RUN apt-get update && \
    apt-get install -y capnproto curl libsqlite3-dev python3-dev python3-pip && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y && \
    . $HOME/.cargo/env && \
    cargo install capnpc && \
    rustup component add rustfmt-preview && \
    pip3 install pycapnp cloudpickle flake8 pytest pytest-timeout cbor pyarrow requests && \
    cargo build --all-features --release --verbose && \
    cd python && \
    python3 setup.py install

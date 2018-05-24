FROM ubuntu:xenial

ADD . /rain
WORKDIR /rain
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/local/lib

RUN apt-get update && \
    apt-get install -y capnproto curl libsqlite3-dev python3-dev python3-pip git cmake pkg-config && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y && \
    . $HOME/.cargo/env && \
    cargo install capnpc && \
    rustup component add rustfmt-preview && \
    pip3 install pycapnp cloudpickle flake8 pytest pytest-timeout cbor pyarrow requests && \
    cargo build --all-features --release --verbose && \
    cd /rain/python && python3 setup.py install && \
    cd / && git clone https://github.com/PJK/libcbor.git && cd libcbor && mkdir _build && cd _build && \
    cmake .. && make && make install && \
    cd /rain/cpp/tasklib && mkdir _build && cd _build && \
    cmake .. && make
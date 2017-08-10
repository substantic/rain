#!/bin/sh
cd "$(dirname "$0")"
mkdir -p src/capnp_gen
capnp compile capnp/*.capnp -o rust:src/capnp_gen/ --src-prefix=capnp

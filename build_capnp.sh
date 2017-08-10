#!/bin/sh
cd "$(dirname "$0")"
mkdir -p src/capnp
capnp compile capnp/*.capnp -o rust:src/

#!/bin/sh

cd `dirname $0`/../..

# Python style check
flake8 python/rain || exit 1
flake8 tests || exit 1
flake8 utils || exit 1

# Rust style check
# NOTE: Disabled until rustfmt stabilizes a bit more
cargo fmt -- --write-mode=diff || exit 0 

echo "Style is ok"

#!/bin/sh

cd `dirname $0`/../..

# Python style check
flake8 python/rain || exit 1
flake8 tests || exit 1

# Rust style check
cargo fmt -- --write-mode=diff || exit 1

echo "Style is ok"

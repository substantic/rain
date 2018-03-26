#!/bin/sh

cd `dirname $0`

./stylecheck.sh || exit 1

cd ../.. || exit 1

# Build debug version
cargo build || exit 1

# Run Rust tests
cargo test || exit 1

# Run python tests
cd tests || exit 1
if [ -x "$(command -v py.test-3)" ]; then
	py.test-3 -x -v --timeout=20 || exit 1
else
	py.test -x -v --timeout=20 || exit 1
fi

echo "--------------"
echo "| Rain is OK |"
echo "--------------"

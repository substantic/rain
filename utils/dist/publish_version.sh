#!/bin/bash
set -u -e

function checkver () {
    egrep '^([0-9]+)\.([0-9]+)\.([0-9]+)(-[a-zA-Z0-9-]+)?$' >/dev/null <<<"$1" || {
        echo "Invalid version format '$1'."
        exit 1
    }
}

# update "REGEXP" "STR" "FILE"
function update () {
    grep "$1" "$3" > /dev/null || {
        echo "Failed to replace '$1' with '$2' in '$3': pattern not found."
        exit 1
    }
    sed -i "s%$1%$2%" "$3"
    grep "$2" "$3" > /dev/null || {
        echo "Failed to replace '$1' with '$2' in '$3'."
        exit 1
    }
}

if [ ! -f "utils/dist/publish_version.sh" ]; then
    echo "Run from workspace root"
    exit 1
fi

if [ "$1" == "-f" ]; then
    GO="T"
    VER="$2"
else
    echo "Only doing local run, use '$0 -f VER' to actually publish."
    echo "Use 'git reset --hard HEAD~1' to roll back the version change."
    GO=""
    VER="$1"
fi

OLDVER=`sed -n 's/^version *= *"\(.*\)"$/\1/p' < Cargo.toml`
OLDPAT=`echo $OLDVER | sed 's/\./\\\\./g'`
TAG="v$VER"
echo "Updating from version '$OLDVER' to '$VER' (tag '$TAG') ..."
checkver "$VER"
checkver "$OLDVER"

GITST=`git status --porcelain`
if [ -n "$GITST" ]; then
    echo "Working directory not clean:"
    echo "$GITST"
    exit 1
fi

echo "Updating README.md"
update "^\\$ wget https://github.com/substantic/rain/releases/download/v$OLDPAT/rain-v$OLDPAT-linux-x64.tar.xz" "$ wget https://github.com/substantic/rain/releases/download/v$VER/rain-v$VER-linux-x64.tar.xz" README.md
update "^\\$ tar xvf rain-v$OLDPAT-linux-x64.tar.xz" "$ tar xvf rain-v$VER-linux-x64.tar.xz" README.md
update "^\\$ ./rain-v$OLDPAT-linux-x86/rain start --simple" "$ ./rain-v$VER-linux-x86/rain start --simple" README.md
git add README.md

echo "Updating docs/guide/install.rst"
update "^   \\$ wget https://github.com/substantic/rain/releases/download/v$OLDPAT/rain-v$OLDPAT-linux-x64.tar.xz" "   $ wget https://github.com/substantic/rain/releases/download/v$VER/rain-v$VER-linux-x64.tar.xz" docs/guide/install.rst
update "^   \\$ tar xvf rain-v$OLDPAT-linux-x64.tar.xz" "   $ tar xvf rain-v$VER-linux-x64.tar.xz" docs/guide/install.rst
git add docs/guide/install.rst

echo "Updating utils/deployment/exoscale/README.md"
update "^python3 exoscale.py install --env default.env --rain-download $OLDPAT" "python3 exoscale.py install --env default.env --rain-download $VER" utils/deployment/exoscale/README.md
git add utils/deployment/exoscale/README.md

( grep -r "$OLDPAT" docs/guide/ || grep -r "$OLDPAT" README.md || grep -r "$OLDPAT" utils/deployment/ ) && {
    echo "Found '$OLDPAT' in an unexpected location, quitting"
    exit 1
}

echo "Disabling [patch]"
update '^rain_core = { path = "rain_core" }$' '#rain_core = { path = "rain_core" }' Cargo.toml
update '^rain_task = { path = "rain_task" }$' '#rain_task = { path = "rain_task" }' Cargo.toml

echo "Updating root version"
update '^version *= *"'$OLDPAT'"$' 'version = "'$VER'"' Cargo.toml
git add Cargo.toml

echo "Updating rain_core"
cd rain_core
update '^version *= *"'$OLDPAT'"$' 'version = "'$VER'"' Cargo.toml
if [ "$GO" == "T" ]; then
    echo "Publishing ..."
    cargo publish --allow-dirty
fi
git add Cargo.toml
cd ..

echo "Updating rain_server"
cd rain_server
update '^version *= *"'$OLDPAT'"$' 'version = "'$VER'"' Cargo.toml
update '^rain_core *= *"'$OLDPAT'"$' 'rain_core = "'$VER'"' Cargo.toml
if [ "$GO" == "T" ]; then
    echo "Publishing ..."
    cargo publish --allow-dirty
fi
git add Cargo.toml
cd ..

echo "Updating rain_task"
cd rain_task
update '^version *= *"'$OLDPAT'"$' 'version = "'$VER'"' Cargo.toml
update '^rain_core *= *"'$OLDPAT'"$' 'rain_core = "'$VER'"' Cargo.toml
if [ "$GO" == "T" ]; then
    echo "Publishing ..."
    cargo publish --allow-dirty
fi
git add Cargo.toml
cd ..

echo "Updating rain_task_test"
cd rain_task_test
update '^version *= *"'$OLDPAT'"$' 'version = "'$VER'"' Cargo.toml
update '^rain_task *= *"'$OLDPAT'"$' 'rain_task = "'$VER'"' Cargo.toml
if [ "$GO" == "T" ]; then
    echo "Testing ..."
    cargo test
fi
git add Cargo.toml
cd ..

echo "Re-enabling [patch]"
update '^#rain_core = { path = "rain_core" }$' 'rain_core = { path = "rain_core" }' Cargo.toml
update '^#rain_task = { path = "rain_task" }$' 'rain_task = { path = "rain_task" }' Cargo.toml
git add Cargo.toml

if [ "$GO" == "T" ]; then
    echo "Updating rain_* in Cargo.lock"
    cargo update -p rain_core -p rain_task -p rain_server -p rain_task_test
    git add Cargo.lock
fi

git commit -m "Releasing version $VER"

if [ "$GO" == "T" ]; then
    git tag "$TAG"
    echo "Created tag $TAG (not pushed automatically)"
fi



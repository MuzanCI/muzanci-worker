#!/usr/bin/env bash

set -o errexit
set -o nounset

cargo build -p muzanci-config

mkdir -p ./embed
cp ../target/debug/config ./embed/config

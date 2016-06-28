#!/usr/bin/env bash

set -e
set -x

mkdir -p vendor
glide --no-color install
go test $(go list ./... | grep -v /vendor/)
go build -o $1

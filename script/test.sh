#!/usr/bin/env bash

set -e
set -x

trap "{ rm -rf vendor; }" EXIT

glide --no-color install
go test $(go list ./... | grep -v /vendor/)
go build -o $1

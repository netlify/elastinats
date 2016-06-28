#!/usr/bin/env bash

set -e
set -x

WORKSPACE=/go/src/github.com/netlify/$1

docker run \
	--volume $(pwd):$WORKSPACE \
	--workdir $WORKSPACE \
	--rm \
	calavera/go-glide:0.10.2 script/test.sh $1

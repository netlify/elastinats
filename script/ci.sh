#!/usr/bin/env bash

set -e
set -x

PROJECT=elastinats
WORKSPACE=/go/src/github.com/netlify/$PROJECT

docker run \
	--volume $(pwd):$WORKSPACE \
	--workdir $WORKSPACE \
	--rm \
	calavera/go-glide:0.10.2 script/test.sh $PROJECT

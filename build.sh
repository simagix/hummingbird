#! /usr/bin/env bash
# Copyright Kuei-chun Chen, 2022-present. All rights reserved.
die() { echo "$*" 1>&2 ; exit 1; }
VERSION="v$(cat version)"
REPO=$(basename "$(dirname "$(pwd)")")/$(basename "$(pwd)")
TAG="neutrino"
LDFLAGS="-X main.version=$VERSION -X main.repo=$TAG"
[[ "$(which go)" = "" ]] && die "go command not found"
[[ "$GOPATH" = "" ]] && die "GOPATH not set"
[[ "${GOPATH}/src/github.com/$REPO" != "$(pwd)" ]] && die "building neutrino should be under ${GOPATH}/src/github.com/$REPO"
mkdir -p dist
go mod vendor
if [[ "$1" == "docker" ]]; then
  DOCKERTAG="simagix/$TAG"
  docker build -f Dockerfile . -t $DOCKERTAG
  id=$(docker create $DOCKERTAG)
  docker cp $id:/dist - | tar vx
else
  env CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -ldflags "$LDFLAGS" -o dist/neutrino-osx-x64 main/hummingbird.go
  env CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$LDFLAGS" -o dist/neutrino-linux-x64 main/hummingbird.go
  ls -l ./dist
  if [ "$(uname)" == "Darwin" ]; then
    ./dist/neutrino-osx-x64 -version
  elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    ./dist/neutrino-linux-x64 -version
  fi
fi

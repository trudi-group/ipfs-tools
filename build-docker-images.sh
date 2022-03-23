#!/bin/bash

docker build -t ipfs-tools-builder -f Dockerfile.builder .
docker build -t wantlist-client -f Dockerfile.wantlist-client .

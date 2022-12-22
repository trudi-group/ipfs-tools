#!/bin/bash -e

docker build -t ipfs-tools-builder -f Dockerfile.builder .
docker build -t bitswap-monitoring-client -f Dockerfile.bitswap-monitoring-client .

#!/bin/bash -e

docker build -t ipfs-tools-builder -f Dockerfile.builder .
docker build -t bitswap-monitoring-client -f Dockerfile.bitswap-monitoring-client .
docker build -t monitoring-size-estimator -f Dockerfile.monitoring-size-estimator .

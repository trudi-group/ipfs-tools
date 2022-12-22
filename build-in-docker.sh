#!/bin/bash -e

mkdir -p out

echo "Building images..."
./build-docker-images.sh

echo "Extracting binaries..."
docker create --name extract ipfs-tools-builder
docker cp extract:/ipfs-tools/target/release/bitswap-discovery-probe ./out/
docker cp extract:/ipfs-tools/target/release/bitswap-monitoring-client ./out/
docker cp extract:/ipfs-tools/target/release/cid-decode ./out/
docker cp extract:/ipfs-tools/target/release/ipfs-gateway-finder ./out/
docker cp extract:/ipfs-tools/target/release/ipfs-json-to-csv ./out/
docker cp extract:/ipfs-tools/target/release/unify-bitswap-traces ./out/

docker rm extract
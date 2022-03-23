#!/bin/bash -e

mkdir -p out

docker build -t ipfs-tools-builder -f Dockerfile.builder .
docker create --name extract ipfs-tools-builder
docker cp extract:/ipfs-tools/target/release/cid-decode ./out/
docker cp extract:/ipfs-tools/target/release/csv-to-graph ./out/
docker cp extract:/ipfs-tools/target/release/ipfs-gateway-finder ./out/
docker cp extract:/ipfs-tools/target/release/ipfs-json-to-csv ./out/
docker cp extract:/ipfs-tools/target/release/ipfs-resolver ./out/
docker cp extract:/ipfs-tools/target/release/ipfs-resolver-db-exporter ./out/
docker cp extract:/ipfs-tools/target/release/ipfs-walk-tree ./out/
docker cp extract:/ipfs-tools/target/release/unify-bitswap-traces ./out/
docker cp extract:/ipfs-tools/target/release/wantlist-client ./out/

docker rm extract


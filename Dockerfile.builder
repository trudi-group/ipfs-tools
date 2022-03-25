# Docker environment to build the various IPFS tools in a stable environment.
# This builds against debian bullseye, which gives us a relatively old libc,
# such that stuff works on older machines as well.
# Compiled binaries can be found in /ipfs-tools/target/release/.
# Sources are copied into /ipfs-tools/.

FROM rust:1-bullseye AS chef

# Cargo-chef is used to build dependencies and cache them, for faster
# incremental builds.
RUN cargo install cargo-chef
WORKDIR ipfs-tools

FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder

# Install OS-level dependencies.
RUN apt-get update && apt-get install -y \
  libncursesw5-dev \
  libssl-dev \
  protobuf-compiler

# Get a list of Rust dependencies to build.
COPY --from=planner /ipfs-tools/recipe.json recipe.json

# Build dependencies - this should be cached by docker.
RUN cargo chef cook --release --recipe-path recipe.json

# Build our project.
COPY . .
RUN cargo build --release --locked

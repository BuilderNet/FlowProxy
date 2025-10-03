#
# Stage 1: Build shadow
#
FROM rust:1.88.0-slim-trixie AS build-shadow

# Parameters used in follow-up installation scripts
ENV CONTAINER="debian:"
ENV CC="gcc"
ENV BUILDTYPE="release"

ENV CARGO_TERM_COLOR=always

SHELL ["/bin/bash", "-c"]

# Install basic packages
RUN apt-get update
RUN apt-get install -y git

# Download shadow
RUN git clone https://github.com/shadow/shadow.git /root/shadow

# Install shadow dependencies
WORKDIR /root/shadow
RUN ci/container_scripts/install_deps.sh

# Install a version of the golang std library that supports dynamic linking
# Taken from shadow/ci/container_scripts/install_extra_deps.sh
RUN go install -buildmode=shared -linkshared std

# Install shadow
RUN ci/container_scripts/build_and_install.sh


#
# Stage 2: Build orderflow proxy
#
FROM build-shadow AS build-proxy

# Packages for the orderflow proxy
RUN apt-get install -y libssl-dev

# Note: the docker context must be the root of the git project due to this COPY
COPY . /root/buildernet-orderflow-proxy

# Build the orderflow proxy
WORKDIR /root/buildernet-orderflow-proxy
RUN cargo build --profile profiling

# Build the mock of the hub used in shadow
WORKDIR /root/buildernet-orderflow-proxy/simulation/mockhub
RUN cargo build --release

WORKDIR /root/buildernet-orderflow-proxy/simulation/submitter
RUN cargo build --release

RUN cargo install --locked flamegraph

#
# Stage 3: Slim image for runtime
#
FROM debian:trixie-slim AS runtime
WORKDIR /root

# Packages for shadow runtime
RUN apt-get update
RUN apt-get install -y libglib2.0-0 libglib2.0-dev linux-perf procps

COPY simulation/testdata/ /root/testdata/

# Copy the compiled binaries and libraries to the fresh image
COPY --from=build-proxy /root/.local/bin/shadow .
COPY --from=build-proxy /root/buildernet-orderflow-proxy/target/profiling/buildernet-orderflow-proxy .
COPY --from=build-proxy /root/buildernet-orderflow-proxy/simulation/mockhub/target/release/mockhub .
COPY --from=build-proxy /root/buildernet-orderflow-proxy/simulation/submitter/target/release/submitter .
# These shadow libraries have to be in /lib according to readelf
COPY --from=build-proxy /root/.local/lib /lib

COPY --from=build-proxy /usr/local/cargo/bin/flamegraph .

# Copy the receiver proxy, rename to buildernet-orderflow-proxy so our scripts work
COPY --from=build-proxy /root/buildernet-orderflow-proxy/simulation/receiver-proxy /root/buildernet-orderflow-proxy
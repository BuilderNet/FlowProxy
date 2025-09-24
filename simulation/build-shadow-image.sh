#!/bin/bash
# From shadow's CI scripts

# Prepare a docker image to run shadow in it

set -euo pipefail

DRYTAG=0
BUILD_IMAGE=0
PUSH=0
NOCACHE=
REPO=shadowsim/shadow-ci
CONTAINER=ubuntu:24.04
CC=gcc
BUILDTYPE=debug

show_help () {
    cat <<EOF
Usage: $0 ...
  -h
  -?             Show help
  -c CONTAINER   Set container
  -C CC          Set C compiler
  -b BUILDTYPE   Set build-types
  -n             nocache when building Docker images
  -p             push image to dockerhub
  -r             set Docker repository
  -t             just get the image tag for the requested configuration

Run default configuration:

  $0

EOF
}

while getopts "h?ipc:C:b:nr:t" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    i)  BUILD_IMAGE=1
        ;;
    c)  CONTAINER="$OPTARG"
        ;;
    C)  CC="$OPTARG"
        ;;
    b)  BUILDTYPE="$OPTARG"
        ;;
    n)  NOCACHE=--no-cache
        ;;
    p)  PUSH=1
        ;;
    r)  REPO="$OPTARG"
        ;;
    t)  DRYTAG=1
        ;;
    esac
done

build () {
    # Replace the single ':' with a '-'
    CONTAINER_FOR_TAG=${CONTAINER/:/-}
    # Replace all forward slashes with '-'
    CONTAINER_FOR_TAG="${CONTAINER_FOR_TAG//\//-}"

    TAG="$REPO:$CONTAINER_FOR_TAG-$CC-$BUILDTYPE"

    if [ "${DRYTAG}" == "1" ]; then
        echo "$TAG"
        return 0
    fi

    echo "Building $TAG"

    # Build and tag a Docker image with the given configuration.
    docker build -t "$TAG" $NOCACHE -f- . <<EOF
        FROM $CONTAINER

        ENV CARGO_TERM_COLOR=always

		# Used in follow-up installation scripts
        ENV CONTAINER "$CONTAINER"

        SHELL ["/bin/bash", "-c"]

        # Install basic packages
        RUN apt-get update
        RUN apt-get install -y git

        # Download shadow
        RUN mkdir /root/shadow
        RUN git clone https://github.com/shadow/shadow.git /root/shadow

        # Install common dependencies, it's split for reusability across images
        WORKDIR /root/shadow
        RUN ci/container_scripts/install_deps.sh

        # Install specific CC/BUILDTYPE dependencies
        ENV CC="$CC" BUILDTYPE="$BUILDTYPE"
        RUN ci/container_scripts/install_extra_deps.sh
        ENV PATH /root/.cargo/bin:\$PATH

        # Install shadow
        RUN ci/container_scripts/build_and_install.sh

        # Packages for buildernet-orderflow-proxy-v2
        RUN apt-get install -y libssl-dev

		# Packages for humans
        # RUN apt-get install -y emacs less

		# Packages for flashbots contender
        # RUN apt-get install -y libsqlite3-dev libssl-dev

		# Packages for buildernet-hub
		# RUN apt-get install -y musl-dev postgresql
EOF

    if [ "${PUSH}" == "1" ]; then
        docker push "${TAG}"
    fi
}

build

#!/bin/bash
# From shadow's CI scripts

# Runs a simulation of the orderflow proxy using shadow

set -euo pipefail

DRYTAG=0
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
  -t             just get the image tag for the requested configuration

Run default configuration:

  $0

EOF
}

while getopts "h?c:C:b:t" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    c)  CONTAINER="$OPTARG"
        ;;
    C)  CC="$OPTARG"
        ;;
    b)  BUILDTYPE="$OPTARG"
        ;;
    t)  DRYTAG=1
        ;;
    esac
done

run () {
    # Replace the single ':' with a '-'
    CONTAINER_FOR_TAG=${CONTAINER/:/-}
    # # Replace all forward slashes with '-'
    CONTAINER_FOR_TAG="${CONTAINER_FOR_TAG//\//-}"

    TAG="$REPO:$CONTAINER_FOR_TAG-$CC-$BUILDTYPE"

    if [ "${DRYTAG}" == "1" ]; then
        echo "$TAG"
        return 0
    fi

    # Run the tests inside the image we just built
    echo "Testing $TAG"

    # Start the container and copy the most recent code.
    DOCKER_CREATE_FLAGS=()

    # Shadow needs some extra space allocated to /dev/shm
    DOCKER_CREATE_FLAGS+=("--shm-size=1024g")

    # Docker's default seccomp policy disables the `personality` syscall, which
    # shadow uses to disable ASLR. This causes shadow's determinism tests to fail.
    # https://github.com/moby/moby/issues/43011
    # It also appears to cause a substantial (~3s) pause when the syscall fails,
    # causing the whole test suite to take much longer, and some tests to time out.
    #
    # If we remove this flag we then need `--cap-add=SYS_PTRACE`, which causes
    # Docker's seccomp policy to allow `ptrace`, `process_vm_readv`, and
    # `process_vm_writev`.
    DOCKER_CREATE_FLAGS+=("--security-opt=seccomp=unconfined")

    # Re-running the installation scripts in case of dependency update, likely a no-op
    CONTAINER_ID=$(docker create "${DOCKER_CREATE_FLAGS[@]}" "${TAG}" /bin/bash -c \
        "cd /root/buildernet-orderflow-proxy-v2 \
         && cargo build \
         && cd /root \
         && /root/.local/bin/shadow /root/buildernet.yaml")

    # Copy the source files
    docker cp .. "${CONTAINER_ID}":/root/buildernet-orderflow-proxy-v2/

    # Copy the shadow configuration files
    scenario=buildernet.yaml
    docker cp ${scenario} "${CONTAINER_ID}":/root

    # Start the container and execute the commands listed above
    RV=0
    docker start --attach "${CONTAINER_ID}" || RV=$?

    # Recover the shadow output files
    docker cp "${CONTAINER_ID}":/root/shadow.data/ .

    # Cleanup
    if [ "${RV}" != "0" ]; then
        echo "Exiting due to docker container failure (${TAG})"
        echo -n "Removing container: "
        docker rm -f "${CONTAINER_ID}"
        exit 1
    fi
}

run


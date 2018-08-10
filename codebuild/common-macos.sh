#!/bin/bash

# Until CodeBuild supports macOS, this script is just used by Travis.

set -e

CMAKE_ARGS="$@"

function install_library {
    git clone https://github.com/awslabs/$1.git
    cd $1
    mkdir build
    cd build

    cmake -DCMAKE_INSTALL_PREFIX=../../install -DENABLE_SANITIZERS=ON $CMAKE_ARGS ../
    make install
}

cd ../

mkdir install

install_library s2n
install_library aws-c-common
install_library aws-c-io

cd ../..

cd aws-c-mqtt
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=../../install -DENABLE_SANITIZERS=ON $CMAKE_ARGS ../

make

LSAN_OPTIONS=verbosity=1:log_threads=1 ctest --output-on-failure

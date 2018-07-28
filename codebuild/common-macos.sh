#!/bin/bash

# Until CodeBuild supports macOS, this script is just used by Travis.

cd ../

mkdir install

git clone https://github.com/awslabs/aws-c-common.git
cd aws-c-common
mkdir build
cd build

cmake -DCMAKE_INSTALL_PREFIX=../../install -DENABLE_SANITIZERS=ON ../ || exit 1
make install || exit 1

cd ../..

cd aws-c-mqtt
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=../../install -DENABLE_SANITIZERS=ON $@ ../ || exit 1

make || exit 1

LSAN_OPTIONS=verbosity=1:log_threads=1 ctest --output-on-failure || exit 1

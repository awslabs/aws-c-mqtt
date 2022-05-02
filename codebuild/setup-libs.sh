#!/bin/bash

set -e

env

INSTALL_PATH="/tmp/install"
mkdir $INSTALL_PATH

function install_library {
    git clone https://github.com/awslabs/$1.git

    if [ -n "$2" ]; then
        git checkout $2
    fi

    cmake -S $1 -B $1/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH
    cmake --build $1/build --target install
}

install_library aws-lc
install_library s2n-tls
install_library aws-c-common
install_library aws-c-cal
install_library aws-c-io
install_library aws-c-compression
install_library aws-c-http

# build mqtt5 package
cmake -S $CODEBUILD_SRC_DIR -B $CODEBUILD_SRC_DIR/build -DCMAKE_INSTALL_PREFIX=$INSTALL_PATH
cmake --build $CODEBUILD_SRC_DIR/build --target install

cert=$(aws secretsmanager get-secret-value --secret-id "unit-test/certificate" --query "SecretString" | cut -f2 -d":" | cut -f2 -d\") && echo -e "$cert" > /tmp/certificate.pem
key=$(aws secretsmanager get-secret-value --secret-id "unit-test/privatekey" --query "SecretString" | cut -f2 -d":" | cut -f2 -d\") && echo -e "$key" > /tmp/privatekey.pem
endpoint=$(aws secretsmanager get-secret-value --secret-id "unit-test/privatekey" --query "SecretString" | cut -f2 -d":" | cut -f2 -d\")
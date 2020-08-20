#!/bin/bash

PACKAGE_DIR=`pwd`

function install_package {
    MODULE_DIR=$1
    cd ${MODULE_DIR}
    python setup.py install
    rm -rf build
    rm -rf dist
    rm -rf *.egg-info
}

install_package ${PACKAGE_DIR}/common.utility
install_package ${PACKAGE_DIR}/common.database
install_package ${PACKAGE_DIR}/common.message
install_package ${PACKAGE_DIR}/common.flow
install_package ${PACKAGE_DIR}/common.server
install_package ${PACKAGE_DIR}/common.exception
install_package ${PACKAGE_DIR}/common.graph
install_package ${PACKAGE_DIR}/common.http
install_package ${PACKAGE_DIR}/common.configparser
install_package ${PACKAGE_DIR}/common.mns

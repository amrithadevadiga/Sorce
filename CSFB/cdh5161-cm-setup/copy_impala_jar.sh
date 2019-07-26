#!/bin/env bash
#
# Copyright (c) 2015, BlueData Software, Inc.
#
# This script copies the BlueData impala frontend jar

SELF=$(readlink -nf $0)
BASE_DIR=$(dirname ${SELF})

source ${BASE_DIR}/logging.sh
source ${BASE_DIR}/utils.sh

NODE_GROUP="$(invoke_bdvcli --get node.nodegroup_id)"
DTAP_JAR="/opt/bluedata/bluedata-dtap.jar"
CLOUDERA_INSTALL_DIR="/opt/cloudera/parcels/CDH/lib"

log_exec ln -s ${DTAP_JAR} $CLOUDERA_INSTALL_DIR/impala/lib
log_exec ln -s ${DTAP_JAR} /var/lib/impala

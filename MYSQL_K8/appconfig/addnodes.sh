#!/usr/bin/env bash

SELF=${BASH_SOURCE[0]}
export CONFIG_BASE_DIR=$(dirname ${SELF})

source ${CONFIG_BASE_DIR}/logging.sh
source ${CONFIG_BASE_DIR}/utils.sh
source ${CONFIG_BASE_DIR}/macros.sh

while [ $# -gt 0 ]; do
    case $1 in
        --nodegroup)
            # Nodegroup
            NODEGROUP=$2
            shift 2
            ;;
        --role)
            ROLE=$2
            shift 2
            ;;
        --fqdns)
            FQDNS=$2
            shift 2
            ;;
        *)
            echo "WARNING: Unknown command line argument: $1"
            exit 1
            ;;
    esac
done

# ADDNODES event specific configuration below

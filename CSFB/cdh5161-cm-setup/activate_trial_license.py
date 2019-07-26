#!/bin/env python

#
# Copyright (c) 2015, BlueData Software, Inc.
#
# The main cloudera deployment script
from __future__ import print_function
import socket, sys, time, os
import logging, traceback
import cm_client
from cm_client.rest import ApiException
from pprint import pprint
from optparse import OptionParser
from bd_vlib import *
from bdmacro import *
from math import log as ln
from pprint import pprint

setup_logger = logging.getLogger("bluedata_setup_cluster")
setup_logger.setLevel(logging.DEBUG)
hdlr = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
hdlr.setFormatter(formatter)
hdlr.setLevel(logging.DEBUG)
setup_logger.addHandler(hdlr)


# CM admin account info
ADMIN_USER = "admin"
ADMIN_PASS = "admin"

## Get configuration of current cluster from BDVLIB
ConfigMeta = BDVLIB_ConfigMetadata()
NodeMacro = BDMacroNode()
DISTRO_ID = ConfigMeta.getWithTokens(["node", "distro_id"])
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])

CM_HOST = ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'controller', 'fqdns'])[0]
CM_PORT = '7180'
API_VERSION = 'v18'

def main():
    cm_client.configuration.username = ADMIN_USER
    cm_client.configuration.password = ADMIN_PASS
    # Create an instance of the API class
    api_url = "http://{}:{}/api/{}".format(CM_HOST, CM_PORT, API_VERSION)
    api_client = cm_client.ApiClient(api_url)
    api_instance = cm_client.ClouderaManagerResourceApi(api_client)
    try:
        setup_logger.info("Activating Trial License...")
        api_instance.begin_trial()
        setup_logger.info("License Activated...")
    except:
        setup_logger.error("Failed to Activate License...")
if __name__ == "__main__":
    main()

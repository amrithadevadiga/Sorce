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

setup_logger = logging.getLogger("bluedata_edit_cluster")
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
CLUSTER_NAME = ConfigMeta.getWithTokens(["cluster", "name"])
SENTRY_HA_HOST= ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'master2', 'fqdns'])[0]


def fetch_hostid(HostsApi, hostname):
    api_response = HostsApi.read_hosts(view='summary')
    api_response=api_response.to_dict()
    for item in api_response['items']:
        if hostname== item['hostname']:
            return item['host_id']

def main():
    cm_client.configuration.username = ADMIN_USER
    cm_client.configuration.password = ADMIN_PASS
    # Create an instance of the API class
    api_host = 'http://'+ CM_HOST
    port = '7180'
    api_version = 'v18'
    api_url = api_host + ':' + port + '/api/' + api_version
    api_client = cm_client.ApiClient(api_url)
    host_api_instance = cm_client.HostsResourceApi(api_client)
    service_api_instance = cm_client.ServicesResourceApi(api_client)
    sentry_host_id=fetch_hostid(HostsApi=host_api_instance, hostname=SENTRY_HA_HOST)
    cluster_name = CLUSTER_NAME
    service_name = "Sentry"
    body = cm_client.ApiEnableSentryHaArgs(new_sentry_host_id=sentry_host_id, new_sentry_role_name= "Sentry-Server-2", zk_service_name="ZOOKEEPER")
    setup_logger.info("Enabling Sentry HA.....")
    time.sleep(60)
    res=service_api_instance.enable_sentry_ha_command(cluster_name, service_name, body=body)
    time.sleep(60)
    setup_logger.info("Sentry HA Enabled...")
if __name__ == "__main__":
    main()

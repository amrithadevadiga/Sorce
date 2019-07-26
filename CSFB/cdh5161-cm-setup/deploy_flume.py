#!/bin/env python

#
# Copyright (c) 2015, BlueData Software, Inc.
#
# The main cloudera deployment script

import socket, sys, time, os
import logging, traceback
from subprocess import Popen, PIPE, STDOUT
from cm_api.api_client import ApiResource, ApiException
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo
from optparse import OptionParser
from bd_vlib import *
from bdmacro import *
from math import log as ln

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

ALL_HOSTS = ConfigMeta.getLocalGroupHosts()
ALL_HOSTS.sort()
HOST_INDEX_MAP = {}

# Can only use this for config api version 5
NodeMacro = BDMacroNode()
for H in ALL_HOSTS:
    HOST_INDEX_MAP[H] = NodeMacro.getNodeIndexFromFqdn(H)

FQDN = ConfigMeta.getWithTokens(["node", "fqdn"])
DISTRO_ID = ConfigMeta.getWithTokens(["node", "distro_id"])
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])
DTAP_JAR = "/opt/bluedata/bluedata-dtap.jar"
HIVE_SERVICE_NAME = "HIVE"
ZOOKEEPER_SERVICE_NAME = "ZOOKEEPER"
YARN_SERVICE_NAME = "YARN"
HADOOP_MR_SERVICE_NAME = YARN_SERVICE_NAME
HDFS_SERVICE_NAME = "HDFS"
CDH_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "cdh_parcel_repo"])
GATEWAY_HOSTS=ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'edge', 'fqdns'])
PROCESS_SWAP_ALERT_OFF = {'process_swap_memory_thresholds': '{"warning":"never", "critical":"never"}'}

def service_swap_alert_off(Service):
    Roles = Service.get_all_roles()
    for Role in Roles:
        try:
            if Role.type != 'GATEWAY':
                Role.update_config(PROCESS_SWAP_ALERT_OFF)
            else:
                # gateway roles are not processes
                pass
        except:
            setup_logger.warn("Exception turning off swap for role " + str(Role))

def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))

def get_hosts_for_service(serviceKey, returnSingle=False):
    keylist = ConfigMeta.searchForToken(serviceKey, "fqdns")
    valuelist = []
    for key in keylist:
        valuelist.extend(ConfigMeta.getWithTokens(key))
    if returnSingle:
        if len(valuelist) > 1:
            setup_logger.warn("returnSingle arg for get_hosts_for_service is True, but > 1 service nodes for key " + str(serviceKey))
        elif len(valuelist) > 0:
            return valuelist[0]
    return valuelist

CM_HOST = get_hosts_for_service(["services", "cloudera_scm_server", NODE_GROUP_ID], True)
CMD_TIMEOUT = 1800
CM_CONFIG = {
  'MISSED_HB_BAD' : 1000,
  'MISSED_HB_CONCERNING' : 1000,
  'TSQUERY_STREAMS_LIMIT' : 1000,
  'REMOTE_PARCEL_REPO_URLS' : CDH_PARCEL_REPO
}


### Flume ###
FLUME_SERVICE_NAME = "FLUME"
FLUME_SERVICE_CONFIG = {
  'hdfs_service': HDFS_SERVICE_NAME
}
FLUME_AGENT_HOSTS = get_hosts_for_service(["services", "flume", NODE_GROUP_ID])
FLUME_AGENT_CONFIG = { }


def deploy_flume(cluster, flume_service_name, flume_service_config, flume_agent_hosts, flume_agent_config):
    
    flume_service = cluster.create_service(flume_service_name, "FLUME")
    flume_service.update_config(flume_service_config)
   
    gw = flume_service.get_role_config_group("{0}-AGENT-BASE".format(flume_service_name))
    gw.update_config(flume_agent_config)
    setup_logger.info("Deploying Flume on hosts: {}".format(",".join(flume_agent_hosts)))
    for host in flume_agent_hosts:
        node = HOST_INDEX_MAP[host]
        flume_service.create_role("{0}-agent-".format(flume_service_name) + str(node), "AGENT", host)
    return flume_service
   



def main():
    (api, cluster) = get_cluster()
    setup_logger.info("Initial API object " + str(api))
    if cluster == None:
        setup_logger.warn("No Cluster Object Found..")
        sys.exit(0)
    cm = api.get_cloudera_manager()
    setup_logger.info("Deploying Flume...")
    flume_service = deploy_flume(cluster,FLUME_SERVICE_NAME, FLUME_SERVICE_CONFIG, FLUME_AGENT_HOSTS, FLUME_AGENT_CONFIG)
    service_swap_alert_off(flume_service)
    flume_service.start().wait()
    setup_logger.info("Deployed Flume..")
    setup_logger.info("Successfully added flume to the cluster")
if __name__ == "__main__":
    main()

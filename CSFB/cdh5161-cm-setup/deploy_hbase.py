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

FQDN = ConfigMeta.getWithTokens(["node", "fqdn"])
DISTRO_ID = ConfigMeta.getWithTokens(["node", "distro_id"])
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])
DTAP_JAR = "/opt/bluedata/bluedata-dtap.jar"
CDH_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata",
  NODE_GROUP_ID, "cdh_parcel_repo"])

ALL_HOSTS = ConfigMeta.getLocalGroupHosts()
ALL_HOSTS.sort()
HOST_INDEX_MAP = {}

# Can only use this for config api version 5
for H in ALL_HOSTS:
    HOST_INDEX_MAP[H] = NodeMacro.getNodeIndexFromFqdn(H)


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

## Utility function to pass in a serviceKey list and get back the list of
## nodes that run this service. For services running on single node
## (Ex. jobtracker), pass True for the returnSingle argument and get back a
## string instead of a list of 1 node.

HDFS_SERVICE_NAME = "HDFS"
ZOOKEEPER_SERVICE_NAME = "ZOOKEEPER"

### Hbase ###
HBASE_SERVICE_NAME = "HBASE"
HBASE_SERVICE_CONFIG = {
  'hdfs_service': HDFS_SERVICE_NAME,
  'zookeeper_service': ZOOKEEPER_SERVICE_NAME,
  'hbase_service_env_safety_valve': 'HBASE_CLASSPATH=$HBASE_CLASSPATH:' + DTAP_JAR,
  'service_health_suppression_hbase_master_health': 'true'
}

HBASE_HM_HOSTS = get_hosts_for_service(["services", "hbase_master", NODE_GROUP_ID])

# Safety valve to update hbase.rootdir with dtap prefix
HBASE_HM_CONFIG = {}

HBASE_RS_HOSTS = get_hosts_for_service(["services", "hbase_regionserver", NODE_GROUP_ID])
HBASE_RS_CONFIG = {
}



def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))

# Deploys Hbase.
def deploy_hbase(cluster, hbase_service_name, hbase_service_config, hbase_hm_hosts, hbase_hm_config, hbase_rs_hosts, hbase_rs_config):
    hbase_service = cluster.create_service(hbase_service_name, "HBASE")
    hbase_service.update_config(hbase_service_config)

    hm = hbase_service.get_role_config_group("{0}-MASTER-BASE".format(hbase_service_name))
    hm.update_config(hbase_hm_config)
    setup_logger.info("Deploying Hbase Master on hosts: {}".format(",".join(hbase_hm_hosts)))    
    for host in hbase_hm_hosts:
        master = HOST_INDEX_MAP[host]
        hbase_service.create_role("{0}-hm".format(hbase_service_name)+ str(master), "MASTER", host)

    rs = hbase_service.get_role_config_group("{0}-REGIONSERVER-BASE".format(hbase_service_name))
    rs.update_config(hbase_rs_config)
    setup_logger.info("Deploying Hbase Region Server on hosts: {}".format(",".join(hbase_rs_hosts)))
    for host in hbase_rs_hosts:
        regionserver = HOST_INDEX_MAP[host]
        hbase_service.create_role("{0}-rs-".format(hbase_service_name) + str(regionserver), "REGIONSERVER", host)

    run_cm_command({'hbase_service': hbase_service}, "hbase_service.create_hbase_root()", "Create HBase rootdir")

    return hbase_service

# Utility function to execute a CM command. If the command fails with a
# timeout from CM (seen when there is resource constraint), we retry
# the command upto 10 times.
def run_cm_command(LOCAL_ENV, COMMAND, MESSAGE, IGNORE_FAILURE=True):
    # retry cm commands if they fail with a timeout
    setup_logger.info("Executing command to " + str(MESSAGE))
    retry_count = 0
    while retry_count < 10000:
        retry_count = retry_count + 1
        try:
            ret = eval(COMMAND, globals(), LOCAL_ENV)
        except ApiException as E:
            if IGNORE_FAILURE:
                setup_logger.warn("Ignoring exception " + str(E))
                break
            if "Error while committing the transaction" in str(E):
                setup_logger.warn("Error while committing the transaction. Retrying...")
                continue
            else:
                raise
        ret = ret.wait(CMD_TIMEOUT)
        if ret.active:
            setup_logger.warn("Command failed to complete within the timeout period." + \
                        " Giving up and continuing with the rest of configuration")
            break
        else:
            if not ret.success:
                if "Command timed-out" in ret.resultMessage:
                   setup_logger.warn("Cloudera manager could not complete the " + \
                               "command within the timeout period. Retrying...")
                elif IGNORE_FAILURE:
                   setup_logger.warn("Command failed with message " + ret.resultMessage)
                   break
                else:
                   raise Exception("Command failed with message " + ret.resultMessage)
            else:
                setup_logger.info("Successfully ran command to " + str(MESSAGE))
                break



def main():
    (api, cluster) = get_cluster()
    setup_logger.info("Initial API object " + str(api))
    if cluster == None:
        setup_logger.warn("No Cluster Object Found..")
        sys.exit(0)
    setup_logger.info("Deploying Hbase..")    
    cm = api.get_cloudera_manager()
    hbase_service = deploy_hbase(cluster, HBASE_SERVICE_NAME, HBASE_SERVICE_CONFIG, HBASE_HM_HOSTS, HBASE_HM_CONFIG, HBASE_RS_HOSTS, HBASE_RS_CONFIG)
    service_swap_alert_off(hbase_service)
    setup_logger.info("Starting Hbase..") 
    hbase_service.start().wait()
    setup_logger.info("Deployed Hbase..")
    setup_logger.info("Successfully added Hbase to the cluster")

if __name__ == "__main__":
    main()

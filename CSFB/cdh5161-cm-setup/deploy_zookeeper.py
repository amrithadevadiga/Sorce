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

setup_logger = logging.getLogger("bluedata_setup_cloudera_cluster")
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
CLUSTER_NAME = ConfigMeta.getWithTokens(["cluster", "name"])

ALL_HOSTS = ConfigMeta.getLocalGroupHosts()
ALL_HOSTS.sort()
HOST_INDEX_MAP = {}

# Can only use this for config api version 5
NodeMacro = BDMacroNode()
for H in ALL_HOSTS:
    HOST_INDEX_MAP[H] = NodeMacro.getNodeIndexFromFqdn(H)

CDH_PARCEL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "cdh_parcel_version"])
CDH_MAJOR_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "cdh_major_version"])
CDH_FULL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "cdh_full_version"])
CDH_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "cdh_parcel_repo"])

##Little hacky , should not be hardcoded
HOST_JAVA_HOME = {"java_home":'/opt/jdk1.8.0_162'}

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

## Cloudera manager configuration
## Set 30 minute timeout for cloudera manager commands
CM_HOST = get_hosts_for_service(["services", "cloudera_scm_server", NODE_GROUP_ID], True)


# Zookeeper Configuration
ZOOKEEPER_SERVICE_NAME = "ZOOKEEPER"
ZOOKEEPER_SERVICE_CONFIG = {
   'zookeeper_datadir_autocreate': 'true',
}

ZOOKEEPER_ROLE_CONFIG = {
    'quorumPort': 2888,
    'electionPort': 3888,
    'dataLogDir': '/var/lib/zookeeper',
    'dataDir': '/var/lib/zookeeper',
    'maxClientCnxns': '1024',
}


CMD_TIMEOUT = 900

def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))

def deploy_zookeeper(cluster):
    zookeeper_service = cluster.create_service(ZOOKEEPER_SERVICE_NAME,ZOOKEEPER_SERVICE_NAME)
    zookeeper_service.update_config(ZOOKEEPER_SERVICE_CONFIG)
    zookeeper_server = get_hosts_for_service(["services", "zookeeper", NODE_GROUP_ID])
    
    setup_logger.info("Deploying Zookeeper on Hosts: {}".format(",".join(zookeeper_server)))
    for zk_host in zookeeper_server:
        # Zookeeper IDs need to start from 1
        zk_id = str(int(HOST_INDEX_MAP[zk_host]) + 1)
        ZOOKEEPER_ROLE_CONFIG['serverId'] = zk_id
        role = zookeeper_service.create_role(ZOOKEEPER_SERVICE_NAME + "-" + str(zk_id), "SERVER", zk_host)
        role.update_config(ZOOKEEPER_ROLE_CONFIG)

    return zookeeper_service


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
    cm = api.get_cloudera_manager()
    zookeeper_service = deploy_zookeeper(cluster)
    time.sleep(60)
    zookeeper_service.start()
    service_swap_alert_off(zookeeper_service)
    setup_logger.info("Deployed Zookeeper..")
    setup_logger.info("Successfully added zookeeper to the cluster.")

if __name__ == "__main__":
    main()

#!/bin/env python

#

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
HIVE_SERVICE_NAME = "HIVE"
YARN_SERVICE_NAME = "YARN"
CDH_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "cdh_parcel_repo"])
HADOOP_MR_SERVICE_NAME = YARN_SERVICE_NAME
ZOOKEEPER_SERVICE_NAME = "ZOOKEEPER"

ALL_HOSTS = ConfigMeta.getLocalGroupHosts()
ALL_HOSTS.sort()
HOST_INDEX_MAP = {}

# Can only use this for config api version 5
for H in ALL_HOSTS:
    HOST_INDEX_MAP[H] = NodeMacro.getNodeIndexFromFqdn(H)


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

HIVE_SAFETY_VALVE_CONFIG = {
   'hive_service_config_safety_valve': '<property> \
      <name>hive.aux.jars.path</name> \
      <value>file://' + DTAP_JAR + '</value> \
    </property>'
}

HIVE_HS2_HOSTS = get_hosts_for_service(["services", "hive_server", NODE_GROUP_ID])
HIVE_HMS_HOSTS = get_hosts_for_service(["services", "hive_metastore", NODE_GROUP_ID])
HIVE_GW_HOSTS = ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'edge', 'fqdns'])

HIVE_SERVICE_CONFIG = {
  'hive_metastore_database_host': CM_HOST,
  'hive_metastore_database_name': 'hive',
  'hive_metastore_database_password': 'hive_password',
  'hive_metastore_database_user': 'hive',
  'hive_metastore_database_port': 3306,
  'hive_metastore_database_type': 'mysql',
  'mapreduce_yarn_service': HADOOP_MR_SERVICE_NAME,
  'zookeeper_service': ZOOKEEPER_SERVICE_NAME,
}
HIVE_HMS_CONFIG = {
  'hive_metastore_java_heapsize': 85306784,
  'metastore_canary_health_enabled': 'false',
}


HIVE_HS2_CONFIG = {
  "hiveserver2_spark_executor_cores" : "4"
}
HIVE_GW_CONFIG = {}

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

# Deploys Hive - hive metastore, hiveserver2, webhcat, gateways
def deploy_hive(cluster, hive_service_name, hive_service_config, hive_hms_hosts, hive_hms_config, hive_hs2_hosts, hive_hs2_config, hive_gw_hosts, hive_gw_config):
    hive_service = cluster.create_service(hive_service_name, "HIVE")
    hive_service.update_config(hive_service_config)
    hive_service.update_config(HIVE_SAFETY_VALVE_CONFIG)

    hms = hive_service.get_role_config_group("{0}-HIVEMETASTORE-BASE".format(hive_service_name))
    hms.update_config(hive_hms_config)
    
    setup_logger.info("Deploying Hive Metastore on host: {}".format(",".join(hive_hms_hosts)))
    for host in hive_hms_hosts:
        hms = HOST_INDEX_MAP[host]
        hive_service.create_role("{0}-hms".format(hive_service_name)+ str(hms), "HIVEMETASTORE", host)

    hs2 = hive_service.get_role_config_group("{0}-HIVESERVER2-BASE".format(hive_service_name))
    hs2.update_config(hive_hs2_config)

    setup_logger.info("Deploying Hive Server on host: {}".format(",".join(hive_hs2_hosts)))
    for host in hive_hs2_hosts:
        hs2 = HOST_INDEX_MAP[host]
        hive_service.create_role("{0}-hs2".format(hive_service_name) + str(hs2), "HIVESERVER2", host)
   
   
    gw = hive_service.get_role_config_group("{0}-GATEWAY-BASE".format(hive_service_name))
    gw.update_config(hive_gw_config)

    setup_logger.info("Deploying Hive Gateway on hosts: {}".format(",".join(hive_gw_hosts)))
    for host in hive_gw_hosts:
        gateway = HOST_INDEX_MAP[host]
        hive_service.create_role("{0}-gw-".format(hive_service_name) + str(gateway), "GATEWAY", host)

    run_cm_command({"hive_service": hive_service}, "hive_service.create_hive_metastore_tables()","Create Hive Metastore tables")
    run_cm_command({"hive_service": hive_service}, "hive_service.create_hive_userdir()","Create Hive User directory")
    run_cm_command({"hive_service": hive_service}, "hive_service.create_hive_warehouse()","Create Hive warehouse directory")

    return hive_service


def run_cm_command(LOCAL_ENV, COMMAND, MESSAGE):
    # retry cm commands if they fail with a timeout
    setup_logger.info("Executing command to " + str(MESSAGE))
    retry_count = 0
    while retry_count < 10000:
        retry_count = retry_count + 1
        try:
            ret = eval(COMMAND, globals(), LOCAL_ENV)
        except ApiException as E:
            if "Error while committing the transaction" in str(E):
                setup_logger.warn("Error while committing the transaction. Retrying...")
                continue
            else:
                raise
        ret = ret.wait(CMD_TIMEOUT)
        if ret.active:
            setup_logger.warn("Command failed to complete within the timeout period. Giving up and continuing with the rest of configuration")
            break
        else:
            if not ret.success:
                if "Command timed-out" in ret.resultMessage:
                    setup_logger.warn("Cloudera manager could not complete the command within the timeout period. Retrying...")
                else:
                    setup_logger.warn("Command failed with message " + ret.resultMessage)
                    break
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
    setup_logger.info("Deploying Hive...")
    hive_service = deploy_hive(cluster, HIVE_SERVICE_NAME, HIVE_SERVICE_CONFIG, HIVE_HMS_HOSTS, HIVE_HMS_CONFIG, HIVE_HS2_HOSTS, HIVE_HS2_CONFIG, HIVE_GW_HOSTS, HIVE_GW_CONFIG)
    service_swap_alert_off(hive_service)
    setup_logger.info("Starting Hive...")
    hive_service.start().wait()
    setup_logger.info("Deployed Hive...")
    setup_logger.info("Successfully added Hive to the cluster")

if __name__ == "__main__":
    main()

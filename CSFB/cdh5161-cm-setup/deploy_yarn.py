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

setup_logger = logging.getLogger("bluedata_setup_yarn")
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
HADOOP_DATA_DIR = "/data"
ZOOKEEPER_SERVICE_NAME = "ZOOKEEPER"
HDFS_SERVICE_NAME = "HDFS"
YARN_SERVICE_NAME = "YARN"

HDFS_DATANODE_HOSTS = get_hosts_for_service(["services", "hdfs_dn", NODE_GROUP_ID])
YARN_RM_HOST = ConfigMeta.getWithTokens(["services", "yarn_rm", NODE_GROUP_ID, "master1", "fqdns"])[0]
YARN_JHS_HOST = get_hosts_for_service(["services", "job_history_server", NODE_GROUP_ID])[0]
YARN_NM_HOSTS = get_hosts_for_service(["services", "yarn_nm", NODE_GROUP_ID])
YARN_GW_HOSTS =  ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'edge', 'fqdns']) 

YARN_SERVICE_CONFIG = {
  'yarn_application_classpath': '$HADOOP_CLIENT_CONF_DIR,$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,' + DTAP_JAR,
  'hdfs_service': HDFS_SERVICE_NAME,
  'yarn_service_env_safety_valve': 'HADOOP_CLASSPATH=$HADOOP_CLASSPATH:' + DTAP_JAR,
}
YARN_RM_CONFIG = {
  'resourcemanager_config_safety_valve': '<property>\
      <name>yarn.resourcemanager.zk-timeout-ms</name> \
      <value>30000</value> \
    </property> \
    <property> \
      <name>yarn.resourcemanager.zk-num-retries</name> \
      <value>100</value> \
    </property>'
}

YARN_JHS_CONFIG = { }
YARN_NM_CONFIG = {
  'yarn_nodemanager_local_dirs': HADOOP_DATA_DIR + '/yarn/nm',
}
YARN_GW_CONFIG = {
  'mapred_submit_replication': max(1, len(HDFS_DATANODE_HOSTS)),
  'mapreduce_client_env_safety_valve': 'HADOOP_CLASSPATH=$HADOOP_CLASSPATH:' + DTAP_JAR,
  'mapreduce_client_config_safety_valve': '<property>\
     <name>mapreduce.job.max.split.locations</name> \
       <value>' + str(len(ALL_HOSTS)) + '</value> \
     </property>'
}

HADOOP_MR_SERVICE_NAME = YARN_SERVICE_NAME
YARN_SERVICE_CONFIG['zookeeper_service']= ZOOKEEPER_SERVICE_NAME



CMD_TIMEOUT = 900

def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))

def deploy_yarn(cluster, yarn_service_name, yarn_service_config, yarn_rm_host, yarn_rm_config, yarn_jhs_host, yarn_jhs_config, yarn_nm_hosts, yarn_nm_config, yarn_gw_hosts, yarn_gw_config):
    yarn_service = cluster.create_service(yarn_service_name, "YARN")
    yarn_service.update_config(yarn_service_config)

    rm = yarn_service.get_role_config_group("{0}-RESOURCEMANAGER-BASE".format(yarn_service_name))
    rm.update_config(yarn_rm_config)

    setup_logger.info("Deploying YARN Resource Manager on Host: {}".format(yarn_rm_host))
    yarn_service.create_role("{0}-rm".format(yarn_service_name), "RESOURCEMANAGER", yarn_rm_host)

    jhs = yarn_service.get_role_config_group("{0}-JOBHISTORY-BASE".format(yarn_service_name))
    jhs.update_config(yarn_jhs_config)
    
    setup_logger.info("Deploying YARN Job History Server on Host: {}".format(yarn_jhs_host))
    yarn_service.create_role("{0}-jhs".format(yarn_service_name), "JOBHISTORY", yarn_jhs_host)

    nm = yarn_service.get_role_config_group("{0}-NODEMANAGER-BASE".format(yarn_service_name))
    nm.update_config(yarn_nm_config)
    
    setup_logger.info("Deploying YARN Node Manager on Hosts: {}".format(",".join(yarn_nm_hosts)))
    for host in yarn_nm_hosts:
        nodemanager = HOST_INDEX_MAP[host]
        yarn_service.create_role("{0}-nm-".format(yarn_service_name) + str(nodemanager), "NODEMANAGER", host)

    gw = yarn_service.get_role_config_group("{0}-GATEWAY-BASE".format(yarn_service_name))
    gw.update_config(yarn_gw_config)
    
    setup_logger.info("Deploying YARN Gateways on Hosts: {}".format(",".join(yarn_gw_hosts)))
    for host in yarn_gw_hosts:
        gateway = HOST_INDEX_MAP[host]
        yarn_service.create_role("{0}-gw-".format(yarn_service_name) + str(gateway), "GATEWAY", host)

    run_cm_command({'yarn_service': yarn_service}, "yarn_service.create_yarn_job_history_dir()","Create YARN job history directory")
    run_cm_command({'yarn_service': yarn_service}, "yarn_service.create_yarn_node_manager_remote_app_log_dir()","Create YARN nodemanager remote app log directory")
    return yarn_service

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
    setup_logger.info("Deploying YARN...")
    yarn_service = deploy_yarn(cluster,YARN_SERVICE_NAME,
                       YARN_SERVICE_CONFIG, YARN_RM_HOST,
                       YARN_RM_CONFIG, YARN_JHS_HOST, YARN_JHS_CONFIG,
                       YARN_NM_HOSTS, YARN_NM_CONFIG, YARN_GW_HOSTS,
                       YARN_GW_CONFIG)
    service_swap_alert_off(yarn_service)
    setup_logger.info("Deployed YARN service " + YARN_SERVICE_NAME +
                  " using ResourceManager on " + YARN_RM_HOST +
                  ", JobHistoryServer on " + YARN_JHS_HOST +
                  ", and NodeManagers on remaining nodes")
    setup_logger.info("Deployed YARN...")
    setup_logger.info("Successfully added yarn to the cluster")
    cluster.stop().wait()
    cluster.start().wait()
    setup_logger.info("Setup additinal HDFS directories")
    BDVLIB_TokenWake("APPS_READY")

if __name__ == "__main__":
     main()


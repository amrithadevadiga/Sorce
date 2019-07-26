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
CLUSTER_ID = ConfigMeta.getWithTokens(["cluster", "id"])
UNIQUE_NAME = "bdhdfs-" + str(CLUSTER_ID)
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


##Fetching Host of All HDFS Releated Service..
HDFS_NAMENODE_HOST = get_hosts_for_service(["services", "hdfs_nn", NODE_GROUP_ID])[0]
HDFS_SECONDARY_NAMENODE_HOST = get_hosts_for_service(["services", "hdfs_snn", NODE_GROUP_ID])[0]
HTTPFS_HOSTS = get_hosts_for_service(["services", "httpfs", NODE_GROUP_ID])
HDFS_DATANODE_HOSTS = get_hosts_for_service(["services", "hdfs_dn", NODE_GROUP_ID])
HDFS_GATEWAY_HOSTS = ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'edge', 'fqdns'])
HDFS_BALANCER_HOSTS = ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'master1', 'fqdns'])


### HDFS ###
HADOOP_DATA_DIR = "/data"
ZOOKEEPER_SERVICE_NAME="ZOOKEEPER"

HDFS_SERVICE_NAME = "HDFS"

HDFS_HTTPFS_SERVICE_NAME = "httpfs"
CORESITE_VALUE_CONFIG = {
   'core_site_safety_valve': '<property> \
      <name>fs.dtap.impl</name> \
      <value>com.bluedata.hadoop.bdfs.Bdfs</value> \
      <description>The FileSystem for BlueData dtap: URIs.</description> \
    </property> \
    <property> \
      <name>hadoop.tmp.dir</name> \
      <value>/data</value> \
    </property> \
    <property> \
      <name>fs.AbstractFileSystem.dtap.impl</name> \
      <value>com.bluedata.hadoop.bdfs.BdAbstractFS</value> \
      <description>The Abstract FileSystem for blue data system</description> \
    </property>\
    <property> \
      <name>fs.dtap.impl.disable.cache</name> \
      <value>false</value> \
    </property>'
}


DFS_REPLICATION = 1
if len(HDFS_DATANODE_HOSTS) >= 3:
  DFS_REPLICATION = 3

HDFS_SERVICE_CONFIG = {
  'dfs_replication': DFS_REPLICATION,
  'dfs_block_size': '268435456',
  'dfs_block_local_path_access_user': 'impala,hbase,mapred,spark,hdfs',
  'hdfs_service_env_safety_valve': 'HADOOP_CLASSPATH=$HADOOP_CLASSPATH:' + DTAP_JAR,
}

HDFS_NAMENODE_SERVICE_NAME = "nn"



CMD_TIMEOUT = 1800
HDFS_NAME_SERVICE = UNIQUE_NAME


NAMENODE_HANDLER_COUNT = max(int(ln(len(HDFS_DATANODE_HOSTS)) * 20), 30)
HDFS_NAMENODE_CONFIG = {
  'dfs_name_dir_list': HADOOP_DATA_DIR + '/namenode',
  'dfs_namenode_handler_count': NAMENODE_HANDLER_COUNT,
  'dfs_namenode_service_handler_count': NAMENODE_HANDLER_COUNT
}

HDFS_SECONDARY_NAMENODE_CONFIG = {
  'fs_checkpoint_dir_list': HADOOP_DATA_DIR + '/namesecondary',
}

HDFS_DATANODE_CONFIG = {
  'dfs_data_dir_list': HADOOP_DATA_DIR + '/datanode',
  'dfs_datanode_data_dir_perm': 755,
  'dfs_datanode_du_reserved': '1073741824'
}

HDFS_GATEWAY_CONFIG = {
  'dfs_client_use_trash' : 'true',
  'hdfs_client_env_safety_valve': 'HADOOP_CLASSPATH=$HADOOP_CLASSPATH:' + DTAP_JAR,
}

def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))


# Utility to execute shell command
def popen_util(COMMAND, OPERATION_NAME):
    setup_logger.info("Executing command: " + str(COMMAND))
    sp = Popen(COMMAND, shell=True, stdin=PIPE, stdout=PIPE,
          stderr=STDOUT, close_fds=True)
    sp_out = sp.communicate()
    if sp.returncode == 0:
        setup_logger.info("Successfully completed " + OPERATION_NAME)
    else:
        setup_logger.error("Failed to " + OPERATION_NAME + " due to \n" + str(sp_out))

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

# Initializes HDFS - format the file system
def init_hdfs(hdfs_service, hdfs_name, timeout):
    shell_command = ['sudo cp -f /opt/bluedata/bluedata-dtap.jar /opt/cloudera/parcels/CDH/jars']
    popen_util(shell_command, "copy dtap jar to cdh jars path")
    #hdfs_service.deploy_client_config().wait()
    # Format and start HDFS
    cmd = "hdfs_service.format_hdfs(\"{0}-nn\".format(hdfs_name))[0]"
    run_cm_command({'hdfs_service': hdfs_service, 'hdfs_name': hdfs_name}, cmd, "Format local HDFS")
    cmd = "hdfs_service.start().wait()"
    run_cm_command({'hdfs_service': hdfs_service}, cmd, "Start local HDFS")


def deploy_hdfs(cluster, hdfs_service_name, hdfs_config,
            hdfs_nn_service_name, hdfs_nn_host, hdfs_nn_config,
            hdfs_snn_host, hdfs_snn_config, hdfs_dn_hosts,
            hdfs_dn_config, hdfs_gw_hosts, hdfs_gw_config, httpfs_hosts, hdfs_balancer_hosts):
    
    hdfs_service = cluster.create_service(hdfs_service_name, "HDFS")
    hdfs_service.update_config(hdfs_config)

    nn_role_group = hdfs_service.get_role_config_group("{0}-NAMENODE-BASE".format(hdfs_service_name))
    nn_role_group.update_config(hdfs_nn_config)
    nn_service_pattern = "{0}-" + hdfs_nn_service_name
    
    setup_logger.info("Deploying Namenode on host: {}".format(hdfs_nn_host))
    hdfs_service.create_role(nn_service_pattern.format(hdfs_service_name),"NAMENODE", hdfs_nn_host)

    httpfs_role_group = hdfs_service.get_role_config_group("{0}-HTTPFS-BASE".format(hdfs_service_name))
    httpfs_service_pattern = "{0}-" + "httpfs"

    setup_logger.info("Deploying HTTPFS on host: {}".format(",".join(httpfs_hosts)))
    for host in httpfs_hosts:
        httpfsnode = HOST_INDEX_MAP[host]
        hdfs_service.create_role(httpfs_service_pattern.format(hdfs_service_name)+str(httpfsnode),"HTTPFS", host)

    snn_role_group = hdfs_service.get_role_config_group("{0}-SECONDARYNAMENODE-BASE".format(hdfs_service_name))
    snn_role_group.update_config(hdfs_snn_config)

    setup_logger.info("Deploying Secondary Namenode on host: {}".format(hdfs_snn_host))
    hdfs_service.create_role("{0}-snn".format(hdfs_service_name),"SECONDARYNAMENODE", hdfs_snn_host)

    dn_role_group = hdfs_service.get_role_config_group("{0}-DATANODE-BASE".format(hdfs_service_name))
    dn_role_group.update_config(hdfs_dn_config)

    setup_logger.info("Deploying DataNode on host: {}".format(",".join(hdfs_dn_hosts)))
    for host in hdfs_dn_hosts:
        datanode = HOST_INDEX_MAP[host]
        hdfs_service.create_role("{0}-dn-".format(hdfs_service_name) + str(datanode), "DATANODE", host)

    gw_role_group = hdfs_service.get_role_config_group("{0}-GATEWAY-BASE".format(hdfs_service_name))
    gw_role_group.update_config(hdfs_gw_config)

    setup_logger.info("Deploying HDFS Balancer on hosts: {}".format(",".join(hdfs_balancer_hosts)))
    for host in hdfs_balancer_hosts:
        balancer = HOST_INDEX_MAP[host]
        hdfs_service.create_role("{0}-BALANCER-".format(hdfs_service_name) + str(balancer),"BALANCER", host)

    setup_logger.info("Deploying HDFS Gateway on hosts: {}".format(",".join(hdfs_gw_hosts)))
    for host in hdfs_gw_hosts:
        gateway = HOST_INDEX_MAP[host]
        hdfs_service.create_role("{0}-gw-".format(hdfs_service_name) + str(gateway),"GATEWAY", host)
    hdfs_service.update_config(CORESITE_VALUE_CONFIG)

    return hdfs_service


def main():
    (api, cluster) = get_cluster()
    setup_logger.info("Initial API object " + str(api))
    if cluster == None:
        setup_logger.warn("No Cluster Object Found..")
        sys.exit(0)
    cm = api.get_cloudera_manager()
    setup_logger.info("Deploying HDFS Service....")
    hdfs_service = deploy_hdfs(cluster,HDFS_SERVICE_NAME,
        HDFS_SERVICE_CONFIG, HDFS_NAMENODE_SERVICE_NAME, HDFS_NAMENODE_HOST,
        HDFS_NAMENODE_CONFIG, HDFS_SECONDARY_NAMENODE_HOST,
        HDFS_SECONDARY_NAMENODE_CONFIG, HDFS_DATANODE_HOSTS,
        HDFS_DATANODE_CONFIG, HDFS_GATEWAY_HOSTS, HDFS_GATEWAY_CONFIG, HTTPFS_HOSTS, HDFS_BALANCER_HOSTS)
    init_hdfs(hdfs_service, HDFS_SERVICE_NAME, CMD_TIMEOUT)
    cluster.deploy_client_config().wait()
    service_swap_alert_off(hdfs_service)
    setup_logger.info("Deployed HDFS service " + HDFS_SERVICE_NAME +
                         " using NameNode on " + HDFS_NAMENODE_HOST +
                         ", SecondaryNameNode on " + HDFS_SECONDARY_NAMENODE_HOST
                         + ", and DataNodes running on remaining nodes")
    setup_logger.info("Deployed HDFS..")
    setup_logger.info("Successfully added hdfs to the cluster")

if __name__ == "__main__":
    main()

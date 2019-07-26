#!/bin/env python

#
# Copyright (c) 2016, BlueData Software, Inc.
#
# The main cloudera deployment script

import socket, sys, time, os
import logging, traceback, uuid
from subprocess import Popen, PIPE, STDOUT
from cm_api.api_client import ApiResource, ApiException
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo
from optparse import OptionParser
from bd_vlib import *
from math import log as ln

setup_logger = logging.getLogger("bluedata_cm_EnableHA_setup")
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
FQDN = ConfigMeta.getWithTokens(["node", "fqdn"])
DISTRO_ID = ConfigMeta.getWithTokens(["node", "distro_id"])
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])
DTAP_JAR = "/opt/bluedata/bluedata-dtap.jar"
CLUSTER_NAME = ConfigMeta.getWithTokens(["cluster", "name"])
ALL_HOSTS = ConfigMeta.getLocalGroupHosts()
ALL_HOSTS.sort()
CDH_PARCEL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata", "1", "cdh_parcel_version"])
CDH_MAJOR_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata", "1", "cdh_major_version"])
CDH_FULL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata", "1", "cdh_full_version"])

IS_KERBEROS_ENABLED = False
try:
    IS_KERBEROS_ENABLED = ConfigMeta.getWithTokens(["cluster", "config_choice_selections", "1", "kerberos"])
except:
    edit_logger.warn("Kerberos key does not exist")

## Utility function to parse the host index number from a bluedata hostname
def get_hostname_index(fqdn):
    hostname = fqdn.split(".")
    host = hostname[0]
    front = host[:len(host.rstrip("0123456789"))]
    return "".join(host.rsplit(front))

## Cloudera manager configuration
## Set 30 minute timeout for cloudera manager commands
CM_HOST = ConfigMeta.getWithTokens(["services", "cloudera_scm_server", "1", "controller", "fqdns"])[0]
CMD_TIMEOUT = 1800

HDFS_SERVICE_NAME = "HDFS"
ZOOKEEPER_SERVICE_NAME = "ZOOKEEPER"
YARN_SERVICE_NAME = "YARN"


## Utility function to pass in a serviceKey list and get back the list of
## nodes that run this service. For services running on single node
## (Ex. jobtracker), pass True for the returnSingle argument and get back a
## string instead of a list of 1 node.
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
NN_HOSTS = get_hosts_for_service(["services", "hdfs_nn", NODE_GROUP_ID])
HDFS_NAMENODE_HOST = NN_HOSTS[0]
YARN_RM_STANDBY = ConfigMeta.getWithTokens(["services", "yarn_rm", "1", "master2", "fqdns"])[0]
HDFS_NAMENODE_HOST_STANDBY = get_hosts_for_service(["services", "hdfs_snn", NODE_GROUP_ID])[0]
##Journal Nodes
HDFS_JOURNAL_NODE_ARGS=[]


JOURNAL_NODES=zookeeper_server = get_hosts_for_service(["services", "hdfs_jn", NODE_GROUP_ID])
for host in JOURNAL_NODES:
    HDFS_JOURNAL_NODE_ARGS.append({"jnHostId": host,"jnEditsDir": "/data/journal"})

def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))


def enable_ha_yarn(cluster, service_name,yarn_standby_host,zookeeper_service_name):
    try:
        service = cluster.get_service(service_name)
        service.enable_rm_ha(yarn_standby_host,zookeeper_service_name).wait()
        setup_logger.info("Enable HA : " + service_name)
    except ApiException as E:
        if "not found in cluster" in str(E):
            setup_logger.warn("The service is not exist")
        else:
            raise

def enable_journal_node(cluster, service_name,hdfs_nn_standby_host,hdfs_journal_node_args,zk_service):
    try:
        service = cluster.get_service(service_name)
        service.enable_nn_ha("HDFS-nn",hdfs_nn_standby_host, service_name, hdfs_journal_node_args,zk_service_name=zk_service).wait()
        setup_logger.info("Enable HA : " + service_name)
    except ApiException as E:
        if "not found in cluster" in str(E):
            setup_logger.warn("The service is not exist")
        else:
            raise

# Utility function to execute a CM command. If the command fails with a
# timeout from CM (seen when there is resource constraint), we retry
# the command upto 10 times.
def run_cm_command(LOCAL_ENV, COMMAND, MESSAGE):
   # retry cm commands if they fail with a timeout
   setup_logger.info("Executing command to " + str(MESSAGE))
   retry_count = 0
   while retry_count < 10:
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
    try:
        (CM, CLUSTER)  = get_cluster()
        setup_logger.info("Got cluster " + CLUSTER_NAME)

        cloudera_services = map(lambda Service: Service.name, CLUSTER.get_all_services())

       
        ## deploy client configurations and start all services
        setup_logger.info("deploying client configuration.")
        run_cm_command({'CLUSTER': CLUSTER}, "CLUSTER.deploy_client_config()", "deploy client configuration")
        setup_logger.info("client configuration deployed.")

       
        ##Enabling Journal Node
        setup_logger.info("Enabling HDFS HA.")
        if HDFS_SERVICE_NAME in cloudera_services:
            enable_journal_node(CLUSTER, HDFS_SERVICE_NAME,HDFS_NAMENODE_HOST_STANDBY,HDFS_JOURNAL_NODE_ARGS,ZOOKEEPER_SERVICE_NAME)
        setup_logger.info("HDFS HA Enabled.")
        time.sleep(60)
 
        #Enabling Yarn HA
        setup_logger.info("Enabling YARN HA.")
        if YARN_SERVICE_NAME in cloudera_services:
            enable_ha_yarn(CLUSTER,YARN_SERVICE_NAME,YARN_RM_STANDBY,ZOOKEEPER_SERVICE_NAME)
        setup_logger.info("YARN HA Enabled.")
      
        time.sleep(10)

        ## Wait for hdfs directories to be created.
        #BDVLIB_TokenWait("HDFS_DIR_CREATED", YARN_JHS_HOST) 
        setup_logger.info("Starting the cluster.")
        CLUSTER.restart(restart_only_stale_services=True).wait()
        setup_logger.info("Cluster configuration successful!")
    except:
        setup_logger.error("Failed to configure cluster")

        setup_logger.error(traceback.format_exc())

        BDVLIB_TokenWake(CDH_PARCEL_VERSION, 'error')

        BDVLIB_TokenWake(CDH_FULL_VERSION, 'error')

        sys.exit(1)

if __name__ == "__main__":
    main()

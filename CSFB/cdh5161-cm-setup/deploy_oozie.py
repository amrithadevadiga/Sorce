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

### Oozie ###
OOZIE_SERVICE_NAME = "OOZIE"
YARN_SERVICE_NAME = "YARN"
HADOOP_MR_SERVICE_NAME = YARN_SERVICE_NAME
OOZIE_SERVICE_CONFIG = {
  'mapreduce_yarn_service': HADOOP_MR_SERVICE_NAME,
}
OOZIE_SERVER_HOST =  get_hosts_for_service(["services", "oozie", NODE_GROUP_ID],True)
OOZIE_SERVER_CONFIG = {
   'oozie_java_heapsize': 207881018,
   'oozie_database_host': CM_HOST,
   'oozie_database_name': 'oozie',
   'oozie_database_password': 'oozie_password',
   'oozie_database_type': 'mysql',
   'oozie_database_user': 'oozie',
   'oozie_web_console' : True
}
OOZIE_SAFETY_VALVE = {
    'oozie_config_safety_valve': '<property> \
        <name>oozie.service.HadoopAccessorService.supported.filesystems</name> \
        <value>*</value> \
    </property> \
    <property> \
        <name>oozie.service.HadoopAccessorService.jobTracker.whitelist</name> \
        <value> </value> \
    </property> \
    <property> \
        <name>oozie.service.HadoopAccessorService.nameNode.whitelist</name> \
        <value> </value> \
    </property>'
}

def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))

# Deploys Oozie - oozie server.
def deploy_oozie(cluster, oozie_service_name, oozie_service_config, oozie_server_host, oozie_server_config):
   oozie_service = cluster.create_service(oozie_service_name, "OOZIE")
   oozie_service.update_config(oozie_service_config)
   oozie_server = oozie_service.get_role_config_group("{0}-OOZIE_SERVER-BASE".format(oozie_service_name))
   oozie_server.update_config(oozie_server_config)
   oozie_server.update_config(OOZIE_SAFETY_VALVE)
   setup_logger.info("Deploying Oozie on host: {}".format(oozie_server_host)) 
   oozie_service.create_role("{0}-server".format(oozie_service_name),"OOZIE_SERVER", oozie_server_host)
   run_cm_command({"oozie_service": oozie_service}, "oozie_service.create_oozie_db()", "Create oozie db")
   run_cm_command({"oozie_service": oozie_service}, "oozie_service.install_oozie_sharelib()", "Install oozie sharelib")
   return oozie_service

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
    (api, cluster) = get_cluster()
    setup_logger.info("Initial API object " + str(api))
    if cluster == None:
        setup_logger.warn("No Cluster Object Found..")
        sys.exit(0)
    setup_logger.info("Deploying Oozie..")    
    cm = api.get_cloudera_manager()
    oozie_service = deploy_oozie(cluster, OOZIE_SERVICE_NAME, OOZIE_SERVICE_CONFIG, OOZIE_SERVER_HOST, OOZIE_SERVER_CONFIG)
    service_swap_alert_off(oozie_service)
    setup_logger.info("Starting Oozie..") 
    oozie_service.start().wait()
    BDVLIB_TokenWake("OOZIE_READY")
    setup_logger.info("Deployed Oozie..")
    setup_logger.info("Successfully added Oozie to the cluster")

if __name__ == "__main__":
    main()

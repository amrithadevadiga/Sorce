#!/bin/env python

#
# Copyright (c) 2015, BlueData Software, Inc.
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
from bdmacro import *
from math import log as ln
#from adv_tuning import *

setup_logger = logging.getLogger("bluedata_cm_setup")
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
CLUSTER_ID = ConfigMeta.getWithTokens(["cluster", "id"])
UNIQUE_NAME = "bdhdfs-" + str(CLUSTER_ID)
DTAP_JAR = "/opt/bluedata/bluedata-dtap.jar"
CLUSTER_NAME = ConfigMeta.getWithTokens(["cluster", "name"])+"--2"
CDH_MAJOR_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "cdh_major_version"])
CDH_FULL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "cdh_full_version"])
CDH_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "cdh_parcel_repo"])
SPARK2_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "spark2_parcel_repo"])
SPARK2_PARCEL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "spark2_parcel_version"])
CDH_PARCEL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "cdh_parcel_version"])
HOST_SWAP_ALERT_OFF = {"host_memswap_thresholds":'{"warning":"never", "critical":"never"}'}
HOST_JAVA_HOME = {"java_home":'/opt/jdk1.8.0_162'}
PROCESS_SWAP_ALERT_OFF = {'process_swap_memory_thresholds': '{"warning":"never", "critical":"never"}'}
CM_CONFIG = {
  'MISSED_HB_BAD' : 1000,
  'MISSED_HB_CONCERNING' : 1000,
  'TSQUERY_STREAMS_LIMIT' : 1000,
  'REMOTE_PARCEL_REPO_URLS' : CDH_PARCEL_REPO
}
ALL_HOSTS = ConfigMeta.getLocalGroupHosts()
ALL_HOSTS.sort()

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



def init_cluster():
   # wait for all cloudera agent processes to come up
   BDVLIB_ServiceWait([["services", "cloudera_scm_agent",NODE_GROUP_ID, "kts"]])
   # make sure cloudera manager has received registration
   # for all new agents
   all_cloudera_hosts = get_hosts_for_service(["services", "cloudera_scm_agent"])
   api = ApiResource(CM_HOST, username="admin", password="admin")
   while True:
       current_all_hosts = map(lambda x: x.hostname, api.get_all_hosts())
       setup_logger.info("Currently registered hosts with CM " + str(current_all_hosts))
       if all(x in current_all_hosts for x in all_cloudera_hosts):
          break
       setup_logger.info("waiting for new nodes to register with cloudera manager")
       time.sleep(10)
   manager = api.get_cloudera_manager()
   manager.update_config(CM_CONFIG)
   cluster = api.create_cluster(CLUSTER_NAME, CDH_MAJOR_VERSION, CDH_FULL_VERSION)
   KTS_HOSTS=ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'kts', 'fqdns'])
   cluster.add_hosts(KTS_HOSTS)

   return (cluster, manager)

def main():
   try:
       (CLUSTER, MANAGER)  = init_cluster()
       setup_logger.info("Initialized cluster " + CLUSTER_NAME )
   except:
       setup_logger.error("Failed to configure cluster")
       setup_logger.error(traceback.format_exc())
       BDVLIB_TokenWake(CDH_PARCEL_VERSION, 'error')
       # Temporary use of CDH_FULL_VERSION token as end of cluster
       # configuration token
       BDVLIB_TokenWake(CDH_FULL_VERSION, 'error')
       sys.exit(1)

if __name__ == "__main__":
    main()

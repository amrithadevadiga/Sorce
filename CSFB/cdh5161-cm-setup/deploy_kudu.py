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

setup_logger = logging.getLogger("bluedata_setup_cluster")
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
CDH_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "cdh_parcel_repo"])

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


### Kudu ###
KUDU_SERVICE_NAME = "kudu"
KUDU_MASTER_HOSTS =  get_hosts_for_service(["services", "kudu_master", NODE_GROUP_ID])
KUDU_TABLET_SERVER_HOSTS = get_hosts_for_service(["services", "kudu_tablet_server", NODE_GROUP_ID])

KUDU_SERVICE_CONFIG = {}
KUDU_MASTER_CONFIG = {
        "fs_data_dirs" : "/var/lib/kudu/master/data",
        "fs_wal_dir" : "/var/lib/kudu/master/wal"
}
KUDU_TABLET_SERVER_CONFIG = {
   "fs_data_dirs" :  "/var/lib/kudu/tablet/data",
   "fs_wal_dir" : "/var/lib/kudu/tablet/wal"

}



def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))

# Deploys KUDU Service - Kudu Master and Kudu Tablet Server....
def deploy_kudu(cluster, kudu_service_name, kudu_service_config, kudu_master_hosts, kudu_master_config, kudu_tablet_server_hosts, kudu_tablet_server_config):

    kudu_service = cluster.create_service(kudu_service_name, "KUDU")
    kudu_service.update_config(kudu_service_config)
    kudu_master = kudu_service.get_role_config_group("{0}-KUDU_MASTER-BASE".format(kudu_service_name))
    kudu_master.update_config(kudu_master_config)
    setup_logger.info("Deploying Kudu Master on hosts: {}".format(",".join(kudu_master_hosts)))
    for host in kudu_master_hosts:
        kudu_id = HOST_INDEX_MAP[host]
        kudu_service.create_role("{0}-KUDU_MASTER".format(kudu_service_name) + str(kudu_id),"KUDU_MASTER", host)

    kudu_tserver = kudu_service.get_role_config_group("{0}-KUDU_TSERVER-BASE".format(kudu_service_name))
    kudu_tserver.update_config(kudu_tablet_server_config)
    setup_logger.info("Deploying Kudu Table Server on hosts: {}".format(",".join(kudu_tablet_server_hosts)))
    for host in kudu_tablet_server_hosts:
        kudu_tid = HOST_INDEX_MAP[host]
        kudu_service.create_role("{0}-KUDU_TSERVER".format(kudu_service_name) + str(kudu_tid),"KUDU_TSERVER", host)
    return kudu_service


def main():
    (api, cluster) = get_cluster()
    setup_logger.info("Initial API object " + str(api))
    if cluster == None:
        setup_logger.warn("No Cluster Object Found..")
        sys.exit(0)
    setup_logger.info("Deploying Kudu..")
    cm = api.get_cloudera_manager()
    kudu_service = deploy_kudu(cluster, KUDU_SERVICE_NAME, KUDU_SERVICE_CONFIG, KUDU_MASTER_HOSTS, KUDU_MASTER_CONFIG, KUDU_TABLET_SERVER_HOSTS, KUDU_TABLET_SERVER_CONFIG)
    service_swap_alert_off(kudu_service)
    setup_logger.info("Starting kudu..")
    kudu_service.start().wait()
    setup_logger.info("Deployed kudu..")
    setup_logger.info("Successfully added kudu to the cluster")

if __name__ == "__main__":
    main()


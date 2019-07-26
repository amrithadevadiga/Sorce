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
HIVE_SERVICE_NAME = "HIVE"
HBASE_SERVICE_NAME = "HBASE"
HDFS_SERVICE_NAME = "HDFS"
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

HDFS_DATANODE_HOSTS = get_hosts_for_service(["services", "hdfs_dn", NODE_GROUP_ID])

# Deploys Impala - statestore, catalogserver, impalads
### Impala ###
IMPALA_SERVICE_NAME = "IMPALA"
IMPALA_SERVICE_CONFIG = {
  'hdfs_service': HDFS_SERVICE_NAME,
  'hive_service': HIVE_SERVICE_NAME
}
IMPALA_SS_HOST = get_hosts_for_service(["services", "ISS", NODE_GROUP_ID],True) 
IMPALA_SS_CONFIG = { }
IMPALA_CS_HOST = get_hosts_for_service(["services", "ICS", NODE_GROUP_ID],True)
IMPALA_CS_CONFIG = { }
IMPALAD_HOSTS = HDFS_DATANODE_HOSTS
IMPALAD_CONFIG = { }

CM_HOST = get_hosts_for_service(["services", "cloudera_scm_server", NODE_GROUP_ID], True)
CMD_TIMEOUT = 1800
CM_CONFIG = {
  'MISSED_HB_BAD' : 1000,
  'MISSED_HB_CONCERNING' : 1000,
  'TSQUERY_STREAMS_LIMIT' : 1000,
  'REMOTE_PARCEL_REPO_URLS' : CDH_PARCEL_REPO
}

def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))

def deploy_impala(cluster, impala_service_name, impala_service_config,
          impala_ss_host, impala_ss_config, impala_cs_host,
          impala_cs_config, impala_id_hosts, impala_id_config):


    impala_service = cluster.create_service(impala_service_name, "IMPALA")
    impala_service.update_config(impala_service_config)

    ss = impala_service.get_role_config_group("{0}-STATESTORE-BASE".format(impala_service_name))
    ss.update_config(impala_ss_config)
   
    setup_logger.info("Deploying Impala State Store on host: {}".format(impala_ss_host))
    impala_service.create_role("{0}-ss".format(impala_service_name), "STATESTORE", impala_ss_host)

    cs = impala_service.get_role_config_group("{0}-CATALOGSERVER-BASE".format(impala_service_name))
    cs.update_config(impala_cs_config)

    setup_logger.info("Deploying Impala Catalog Store on host: {}".format(impala_cs_host))
    impala_service.create_role("{0}-cs".format(impala_service_name), "CATALOGSERVER", impala_cs_host)

    id = impala_service.get_role_config_group("{0}-IMPALAD-BASE".format(impala_service_name))
   
    setup_logger.info("Deploying Impala D on hosts: {}".format(",".join(impala_id_hosts)))
    for host in impala_id_hosts:
        impalad = HOST_INDEX_MAP[host]
        role = impala_service.create_role("{0}-id-".format(impala_service_name) + str(impalad), "IMPALAD", host)
        role.update_config(impala_id_config)
    copy_impala_jar()
    return impala_service

def copy_impala_jar():
    shell_command = ['sudo /opt/bluedata/vagent/guestconfig/cdh5-cm-setup' + '/copy_impala_jar.sh']
    popen_util(shell_command, "copy bluedata impala frontend jar")


# Utility to execute shell command
def popen_util(COMMAND, OPERATION_NAME):
    setup_logger.info("Executing command: " + str(COMMAND))
    sp = Popen(COMMAND, shell=True, stdin=PIPE, stdout=PIPE,stderr=STDOUT, close_fds=True)
    sp_out = sp.communicate()
    if sp.returncode == 0:
        setup_logger.info("Successfully completed " + OPERATION_NAME)
    else:
        setup_logger.error("Failed to " + OPERATION_NAME + " due to \n" + str(sp_out))

# Utility function to execute a CM command. If the command fails with a
# timeout from CM (seen when there is resource constraint), we retry
# the command upto 10 times.

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
    setup_logger.warn("Deploying Impala..")
    impala_service = deploy_impala(cluster, IMPALA_SERVICE_NAME, IMPALA_SERVICE_CONFIG, IMPALA_SS_HOST, IMPALA_SS_CONFIG, IMPALA_CS_HOST, IMPALA_CS_CONFIG, IMPALAD_HOSTS, IMPALAD_CONFIG)
    service_swap_alert_off(impala_service)
    setup_logger.warn("Starting Impala..")
    impala_service.start().wait()
    setup_logger.info("Deployed Impala....")
    setup_logger.info("Successfully added Impala to the cluster")

if __name__ == "__main__":
    main()

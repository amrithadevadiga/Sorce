#!/bin/env python

#

#Copyright (c) 2015, BlueData Software, Inc.
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
IMPALA_SERVICE_NAME = "IMPALA"
OOZIE_SERVICE_NAME = "OOZIE"
HDFS_SERVICE_NAME = "HDFS"
HDFS_HTTPFS_SERVICE_NAME = "httpfs"
CDH_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "cdh_parcel_repo"])
IS_KERBEROS_ENABLED = ConfigMeta.getWithTokens(["cluster", "config_choice_selections", NODE_GROUP_ID, "kerberos"])
ALL_HOSTS = ConfigMeta.getLocalGroupHosts()
ALL_HOSTS.sort()
HOST_INDEX_MAP = {}

# Can only use this for config api version 5
NodeMacro = BDMacroNode()
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

HUE_SERVER_HOSTS = get_hosts_for_service(["services", "hue", NODE_GROUP_ID])
HUE_SERVER_CONFIG = {
}

HUE_KTR_HOSTS = get_hosts_for_service(["services", "hue_ktr", NODE_GROUP_ID])
HUE_KTR_CONFIG = { }

id = HOST_INDEX_MAP[get_hosts_for_service(["services", "httpfs", NODE_GROUP_ID])[0]]

### HUE ###
HUE_SERVICE_NAME = "HUE"
HUE_SERVICE_CONFIG = {
  'hive_service': HIVE_SERVICE_NAME,
  'impala_service': IMPALA_SERVICE_NAME,
  'oozie_service': OOZIE_SERVICE_NAME,
  'hue_webhdfs': HDFS_SERVICE_NAME + "-" + HDFS_HTTPFS_SERVICE_NAME+str(id),
  'database_host': CM_HOST,
  'database_type': 'mysql',
  'database_password': 'hue_password'
}



CMD_TIMEOUT = 900


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

def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))

def deploy_hue(cluster, hue_service_name, hue_service_config, hue_server_hosts, hue_server_config, hue_ktr_hosts, hue_ktr_config):
    hue_service = cluster.create_service(hue_service_name, "HUE")
    hue_service.update_config(hue_service_config)

    hue_server = hue_service.get_role_config_group("{0}-HUE_SERVER-BASE".format(hue_service_name))
    hue_server.update_config(hue_server_config)
    setup_logger.info("Deploying Hue on hosts: {}".format(",".join(hue_server_hosts)))
    for host in hue_server_hosts:
        hue = HOST_INDEX_MAP[host]
        hue_service.create_role("{0}-server".format(hue_service_name)+str(hue), "HUE_SERVER", host)

    setup_logger.info("Deploying Hue Load balancer on hosts: {}".format(",".join(hue_server_hosts)))
    for host in hue_server_hosts:
        huelb = HOST_INDEX_MAP[host]
        hue_service.create_role("{0}-HUE_LOAD_BALANCER".format(hue_service_name)+str(huelb), "HUE_LOAD_BALANCER", host)
    if IS_KERBEROS_ENABLED:
        ktr = hue_service.get_role_config_group("{0}-KT_RENEWER-BASE".format(hue_service_name))
        ktr.update_config(hue_ktr_config)
        setup_logger.info("Deploying Hue Kerberos Ticket Renewer on hosts: {}".format(",".join(hue_ktr_hosts)))
        for host in hue_ktr_hosts:
            huektr = HOST_INDEX_MAP[host]
            hue_service.create_role("{0}-ktr".format(hue_service_name) + str(huektr), "KT_RENEWER", host)

    #copy the dtap jar to httpfs libs directory
    shell_command = ['sudo cp -f /opt/bluedata/bluedata-dtap.jar /opt/cloudera/parcels/CDH/lib/hadoop-httpfs/webapps/webhdfs/WEB-INF/lib/']
    popen_util(shell_command, "copy dtap jar to httpfs libs")
    return hue_service

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
    setup_logger.info("Deploying Hue..")
    hue_service = deploy_hue(cluster, HUE_SERVICE_NAME, HUE_SERVICE_CONFIG, HUE_SERVER_HOSTS, HUE_SERVER_CONFIG, HUE_KTR_HOSTS, HUE_KTR_CONFIG)
    service_swap_alert_off(hue_service)
    setup_logger.warn("Starting Hue..")
    hue_service.start().wait()
    setup_logger.info("Deployed Hue")
    setup_logger.info("Successfully added Hue to the cluster")

if __name__ == "__main__":
    main()

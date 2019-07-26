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

setup_logger = logging.getLogger("bluedata_cloudera_setup_cluster")
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

CMD_TIMEOUT = 900

### Spark ###
SPARK_SERVICE_NAME = "SPARK"
SPARK2_SERVICE_NAME = "SPARK2"

SPARK_SERVICE_CONFIG = {
  "yarn_service": "YARN"
}

SPARK2_SERVICE_CONFIG = {
  "yarn_service": "YARN"
}

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

# Utility function to execute a CM command. If the command fails with a
# timeout from CM (seen when there is resource constraint), we retry
# the command upto 10 times.
def run_cm_command(LOCAL_ENV, COMMAND, MESSAGE, IGNORE_FAILURE=True):
    # retry cm commands if they fail with a timeout
    setup_logger.info("Executing command to " + str(MESSAGE))
    retry_count = 0
    while retry_count < 10:
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


def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))




SPARK_HISTORY_HOST = get_hosts_for_service(["services", "spark_history", NODE_GROUP_ID], True) 
SPARK2_HISTORY_HOST = get_hosts_for_service(["services", "spark2_history", NODE_GROUP_ID], True)

SPARK_HISTORY_CONFIG = {
#   'master_max_heapsize': 67108864,
}

SPARK2_HISTORY_CONFIG = {
#   'master_max_heapsize': 67108864,
}

SPARK_GW_HOSTS = ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'edge', 'fqdns'])
SPARK_GW_CONFIG = {}

SPARK2_GW_HOSTS = ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'edge', 'fqdns'])
SPARK2_GW_CONFIG = {}

# Utility to execute shell command
def popen_util(COMMAND, OPERATION_NAME):
    setup_logger.info("Executing command: " + str(COMMAND))
    sp = Popen(COMMAND, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    sp_out = sp.communicate()
    if sp.returncode == 0:
        setup_logger.info("Successfully completed " + OPERATION_NAME)
    else:
        setup_logger.error("Failed to " + OPERATION_NAME + " due to \n" + str(sp_out))


def create_spark_history_server_directory():
    MASTER_FQDN=ConfigMeta.getWithTokens(['nodegroups', NODE_GROUP_ID, 'roles', 'master1', 'fqdns'])[0]
    shell_command = ['sudo -u hdfs hadoop fs -mkdir hdfs://{0}:{1}/user/spark'.format(MASTER_FQDN,8020)]
    popen_util(shell_command, "create spark directory")
    shell_command = ['sudo -u hdfs hadoop fs -mkdir hdfs://{0}:{1}/user/spark/applicationHistory'.format(MASTER_FQDN,8020)]
    popen_util(shell_command, "create applicationHistory directory")
    shell_command = ['sudo -u hdfs hadoop fs -mkdir hdfs://{0}:{1}/user/spark/spark2ApplicationHistory'.format(MASTER_FQDN,8020)]
    popen_util(shell_command, "create spark2ApplicationHistory directory")
    shell_command = ['sudo -u hdfs hadoop fs -chown -R spark:spark hdfs://{0}:{1}/user/spark'.format(MASTER_FQDN,8020)]
    popen_util(shell_command, "chown spark directory")
    shell_command = ['sudo -u hdfs hadoop fs -chmod 1777 hdfs://{0}:{1}/user/spark/applicationHistory'.format(MASTER_FQDN,8020)]
    popen_util(shell_command, "chmod applicationHistory directory")
    shell_command = ['sudo -u hdfs hadoop fs -chmod 1777 hdfs://{0}:{1}/user/spark/spark2ApplicationHistory'.format(MASTER_FQDN,8020)]
    popen_util(shell_command, "chmod spark2ApplicationHistory directory")


# Deploys spark history server
def deploy_spark(cluster, spark_service_name, spark_service_config, spark_server_host, spark_history_config, spark_gw_hosts):
    spark_service = cluster.create_service(spark_service_name, "SPARK_ON_YARN")
    spark_service.update_config(spark_service_config)
    create_spark_history_server_directory()
    setup_logger.info("Deploying Spark History Server on host: {}".format(spark_server_host))
    spark_service.create_role("{0}-historyserver".format(spark_service_name), "SPARK_YARN_HISTORY_SERVER", spark_server_host)
    spark_history = spark_service.get_role_config_group("{0}-SPARK_YARN_HISTORY_SERVER-BASE".format(spark_service_name))
    spark_history.update_config(spark_history_config)
    setup_logger.info("Deploying Spark Gateway on hosts: {}".format(",".join(spark_gw_hosts)))
    for host in spark_gw_hosts:
        gateway = HOST_INDEX_MAP[host]
        spark_service.create_role("{0}-gw-".format(spark_service_name) + str(gateway), "GATEWAY", host)
    return spark_service



# Deploys spark2 history server
def deploy_spark2(cluster, spark_service_name, spark_service_config, spark2_server_host, spark_history_config, spark2_gw_hosts):
    spark_service = cluster.create_service(spark_service_name, "SPARK2_ON_YARN")
    spark_service.update_config(spark_service_config)
    setup_logger.info("Deploying Spark2 History Server on host: {}".format(spark2_server_host))
    spark_service.create_role("{0}-historyserver".format(spark_service_name), "SPARK2_YARN_HISTORY_SERVER", spark2_server_host)
    spark_history = spark_service.get_role_config_group("{0}-SPARK2_YARN_HISTORY_SERVER-BASE".format(spark_service_name))
    spark_history.update_config(spark_history_config)
    setup_logger.info("Deploying Spark2 Gateway on hosts: {}".format(",".join(spark2_gw_hosts)))
    for host in spark2_gw_hosts:
        gateway = HOST_INDEX_MAP[host]
        spark_service.create_role("{0}-gw-".format(spark_service_name) + str(gateway), "GATEWAY", host)
    return spark_service


def main():
    (api, cluster) = get_cluster()
    setup_logger.info("Initial API object " + str(api))
    if cluster == None:
        setup_logger.warn("No Cluster Object Found..")
        sys.exit(0) 
    cm = api.get_cloudera_manager()

    # deploy spark only when on yarn
    spark_service = deploy_spark(cluster, SPARK_SERVICE_NAME, SPARK_SERVICE_CONFIG, SPARK_HISTORY_HOST, SPARK_HISTORY_CONFIG, SPARK_GW_HOSTS)

    setup_logger.info("Deployed SPARK service " + SPARK_SERVICE_NAME + " with SparkHistory server on " + SPARK_HISTORY_HOST)
    service_swap_alert_off(spark_service)
    spark_service.start().wait()
    spark2_service = deploy_spark2(cluster, SPARK2_SERVICE_NAME, SPARK2_SERVICE_CONFIG, SPARK2_HISTORY_HOST, SPARK2_HISTORY_CONFIG, SPARK2_GW_HOSTS)
    setup_logger.info("Deployed SPARK2 service " + SPARK_SERVICE_NAME + " with Spark2History server on " + SPARK2_HISTORY_HOST)
    service_swap_alert_off(spark2_service)
    spark2_service.start().wait()
    setup_logger.info("Spark and Spark2 History Server Successfully Added to Cluster.")

if __name__ == "__main__":
    main()

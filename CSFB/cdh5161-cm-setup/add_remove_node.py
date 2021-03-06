#!/bin/env python

#
# Copyright (c) 2015, BlueData Software, Inc.
#
# The main cloudera deployment script

import socket, sys, time, os
from datetime import datetime
import logging, traceback
from subprocess import Popen, PIPE, STDOUT
from cm_api.api_client import ApiResource, ApiException
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo
from optparse import OptionParser
from bd_vlib import *
from bdmacro import *
from math import log as ln

edit_logger = logging.getLogger("bluedata_edit_cluster")
edit_logger.setLevel(logging.DEBUG)
hdlr = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
hdlr.setFormatter(formatter)
hdlr.setLevel(logging.DEBUG)
edit_logger.addHandler(hdlr)

parser = OptionParser()
parser.add_option('', '--addnodes', action='store_true', dest='IS_ADD_NODE', default=False, help='')
parser.add_option('', '--delnodes', action='store_true', dest='IS_DEL_NODE', default=False, help='')
parser.add_option('', '--nodegroup',dest='NODE_GROUP', default='', help='')
parser.add_option('', '--fqdns', dest='FQDNS', default='', help='')
parser.add_option('', '--role', dest='role', default='', help='')

(options, args) = parser.parse_args()

CHANGED_HOSTS = list(options.FQDNS.split(','))
IS_ADD_NODE=options.IS_ADD_NODE
IS_DEL_NODE=options.IS_DEL_NODE
ADD_NODEGROUP=options.NODE_GROUP
ROLE=options.role

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
CM_HOST = ConfigMeta.getWithTokens(["distros", DISTRO_ID,
  NODE_GROUP_ID, "roles", "controller", "fqdns"])[0]
ALL_HOSTS = ConfigMeta.getLocalGroupHosts()
ALL_HOSTS.sort()
CDH_PARCEL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata",
    NODE_GROUP_ID, "cdh_parcel_version"])
CDH_MAJOR_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata",
    NODE_GROUP_ID, "cdh_major_version"])
CDH_FULL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata",
    NODE_GROUP_ID, "cdh_full_version"])
CDH_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata",
    NODE_GROUP_ID, "cdh_parcel_repo"])
##Little hacky , should not be hardcoded
HOST_JAVA_HOME = {"java_home":'/opt/jdk1.8.0_202'}

# the edge node will be expanded by master gw node
if(ADD_NODEGROUP != NODE_GROUP_ID and IS_ADD_NODE):
   BDVLIB_TokenWake(CDH_FULL_VERSION, 'success')
   sys.exit(0)

IS_KERBEROS_ENABLED = False
try:
    IS_KERBEROS_ENABLED = ConfigMeta.getWithTokens(["cluster", "config_choice_selections", NODE_GROUP_ID, "kerberos"])
except:
    edit_logger.warn("Kerberos key does not exist")

## Names based on the deploy naming conventions

YARN_SERVICE_NAME = "YARN"
YARN_NM_NAME = "NODEMANAGER"
YARN_NM_ROLE_PREFIX = "YARN-nm"

IMPALA_SERVICE_NAME = "IMPALA"
IMPALAD_NAME = "IMPALAD"
IMPALAD_ROLE_PREFIX = "IMPALA-id"

HDFS_SERVICE_NAME = "HDFS"
HDFS_DN_NAME = "DATANODE"
HDFS_DN_ROLE_PREFIX = "HDFS-dn"


SPARK_GW_SERVICE_NAME = "SPARK"
SPARK_GW_NAME = "GATEWAY"
SPARK_GW_ROLE_PREFIX = "SPARK-gw"

SPARK2_GW_SERVICE_NAME = "SPARK2"
SPARK2_GW_NAME = "GATEWAY"
SPARK2_GW_ROLE_PREFIX = "SPARK2-gw"

HIVE_GW_SERVICE_NAME = "HIVE"
HIVE_GW_NAME = "GATEWAY"
HIVE_GW_ROLE_PREFIX = "HIVE-gw"

HDFS_GW_SERVICE_NAME = "HDFS"
HDFS_GW_NAME = "GATEWAY"
HDFS_GW_ROLE_PREFIX = "HDFS-gw"

SENTRY_GW_SERVICE_NAME = "Sentry"
SENTRY_GW_NAME = "GATEWAY"
SENTRY_GW_ROLE_PREFIX = "Sentry-gw"

YARN_GW_SERVICE_NAME = "YARN"
YARN_GW_NAME = "GATEWAY"
YARN_GW_ROLE_PREFIX = "YARN-gw"

HIVE_SERVICE_NAME = "HIVE"
HIVE_HS2_NAME = "HIVESERVER2"
HIVE_HS2_ROLE_PREFIX = "HIVE-hs2"

HDFS_HTTPFS_NAME="HTTPFS"
HDFS_HTTPFS_ROLE_PREFIX="HDFS-httpfs"

FLUME_SERVICE_NAME = "FLUME"
FLUME_AGENT_NAME = "AGENT"
FLUME_ROLE_PREFIX = "FLUME-agent"

SQOOP_CLIENT_SERVICE_NAME = "SQOOP_CLIENT"
SQOOP_CLIENT_GW_NAME= "GATEWAY"
SQOOP_CLIENT_GW_ROLE_PREFIX = "SQOOP_CLIENT-gw"

HBASE_SERVICE_NAME = "HBASE"
HBASE_RS_NAME = "REGIONSERVER"
HBASE_RS_ROLE_PREFIX = "HBASE-rs"

SOLR_SERVICE_NAME = "SOLR"
SOLR_SERVER_NAME = "SOLR_SERVER"
SOLR_ROLE_PREFIX = "SOLR-SERVER"

SERVICE_NAMES = ["HBASE", "MAPRED", "YARN", "IMPALA", "HDFS", "SPARK", "HIVE", "SPARK2", "FLUME", "Sentry", "SQOOP_CLIENT", "SOLR", "Solr"]

CMD_TIMEOUT = 900

# Delete hostId list
deleted_hostId_list = []


def hosts_set_javahome(api):
    Hosts = api.get_all_hosts()
    for Host in Hosts:
        try:
            edit_logger.info("setting java_home for host: " + str(Host))
            Host.update_config(HOST_JAVA_HOME)
        except:
            setup_logger.warn("Exception setting java home for host" + str(Host))


def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    return (api, api.get_cluster(api.get_all_clusters()[0].name))

def add_worker(worker_hosts, service, service_name, role_name, role_prefix):
    for host in worker_hosts:
        worker_index = NodeMacro.getNodeIndexFromFqdn(host)
        epoch = int(time.mktime(datetime.now().timetuple()))
        worker_role_name = role_prefix + str(worker_index) + "-" + str(epoch)
        edit_logger.info("Assigning the role {0} (worker_role_name: {1}) to {2}".format(role_name, worker_role_name, host))
        service.create_role(worker_role_name, role_name, host)

def add_gateway(gateway_hosts, service, service_name, role_name, role_prefix):
    for host in gateway_hosts:
        gateway_index = NodeMacro.getNodeIndexFromFqdn(host)
        epoch = int(time.mktime(datetime.now().timetuple()))
        gateway_role_name = role_prefix + str(gateway_index) +"-" + str(epoch)
        edit_logger.info("Assigning the role {0} (gateway_role_name: {1}) to {2}".format(role_name, gateway_role_name, host))
        service.create_role(gateway_role_name, role_name, host)

def delete_hosts(api, cluster, hosts_list):
    for host in cluster.list_hosts():
        hostId = host.hostId
        hostname = api.get_host(hostId).hostname
        if hostname in hosts_list:
            delete_roles(cluster, hostId)
            cluster.remove_host(hostname)
            edit_logger.info("About to delete host " + str(hostId))
            A = api.delete_host(hostId)
            edit_logger.info("Deleted host " + str(A))
            deleted_hostId_list.append(hostId)

def find_snn_candidate():
    list_of_candidates = list(set(ALL_HOSTS) - set(CHANGED_HOSTS) - set([CM_HOST]))
    if len(list_of_candidates) >= 1:
        return list_of_candidates[0]
    else:
        return None

def delete_roles(cluster, hostId):
    for service_name in SERVICE_NAMES:
        try:
            service = cluster.get_service(service_name)
        except:
            edit_logger.warning("Cluster does not have service: " + service_name)
            continue
        roles = service.get_all_roles()
        for role in roles:
            try:
                if(role.hostRef.hostId == hostId):
                    roleName = role.name
                    if(role.type == "SECONDARYNAMENODE"):
                        edit_logger.warning("About to delete node hosting SNN. Trying to migrate this service to another node.")
                        new_snn_host = find_snn_candidate()
                        if new_snn_host:
                            edit_logger.info("Deploying SNN on " + str(new_snn_host))
                            service.create_role("{0}-snn".format(HDFS_SERVICE_NAME),
                                "SECONDARYNAMENODE", new_snn_host)
                        else:
                            edit_logger.warning("Failed to find a SNN canndidate")
                    service.delete_role(roleName)
            except:
                edit_logger.error("Exception during delete")
                edit_logger.error(traceback.format_exc())
                continue

def activate_parcel(cluster):
    p = cluster.get_parcel('CDH', CDH_PARCEL_VERSION)
    while True:
        p = cluster.get_parcel('CDH', CDH_PARCEL_VERSION)
        state = p.state
        if p.stage == "ACTIVATED" and state.progress == state.totalProgress:
            break
        if p.state.errors:
            BDVLIB_TokenWake(CDH_PARCEL_VERSION, 'error')
            raise Exception(str(p.state.errors))
        edit_logger.info(p)
        time.sleep(10)
    # wake up anybody waiting on parcel distribution
    BDVLIB_TokenWake(CDH_PARCEL_VERSION)

# Utility function to execute a CM command. If the command fails with a
# timeout from CM (seen when there is resource constraint), we retry
# the command upto 10 times.
def run_cm_command(LOCAL_ENV, COMMAND, MESSAGE, IGNORE_FAILURE=True):
    # retry cm commands if they fail with a timeout
    edit_logger.info("Executing command to " + str(MESSAGE))
    retry_count = 0
    while retry_count < 10:
        retry_count = retry_count + 1
        try:
            ret = eval(COMMAND, globals(), LOCAL_ENV)
        except ApiException as E:
            if IGNORE_FAILURE:
                edit_logger.warn("Ignoring exception " + str(E))
                break
            if "Error while committing the transaction" in str(E):
                edit_logger.warn("Error while committing the transaction. Retrying...")
                continue
            else:
                raise
        ret = ret.wait(CMD_TIMEOUT)
        if ret.active:
            edit_logger.warn("Command failed to complete within the timeout period." + \
                        " Giving up and continuing with the rest of configuration")
            break
        else:
            if not ret.success:
                if "Command timed-out" in ret.resultMessage:
                   edit_logger.warn("Cloudera manager could not complete the " + \
                               "command within the timeout period. Retrying...")
                elif IGNORE_FAILURE:
                   edit_logger.warn("Command failed with message " + ret.resultMessage)
                   break
                else:
                   raise Exception("Command failed with message " + ret.resultMessage)
            else:
                edit_logger.info("Successfully ran command to " + str(MESSAGE))
                break

# This is a hack until dtap returns the right number of block locations
def set_split_locations(yarn_service):
    gw_role = yarn_service.get_role_config_group("{0}-GATEWAY-BASE".format(YARN_SERVICE_NAME))
    YARN_GW_CONFIG = {
        'mapreduce_client_config_safety_valve': '<property>\
         <name>mapreduce.job.max.split.locations</name> \
           <value>' + str(len(ALL_HOSTS)) + '</value> \
         </property>'
    }
    edit_logger.info("Updating YARN GW config with {0}".format(YARN_GW_CONFIG))
    gw_role.update_config(YARN_GW_CONFIG)

def fix_hdfs_config(hdfs_service):
    datanode_role_config = hdfs_service.get_role_config_group("HDFS-DATANODE-BASE")
    num_datanodes = len(datanode_role_config.get_all_roles())
    current_replication = hdfs_service.get_config(view='full')[0].get('dfs_replication')
    dfs_replication = 1
    if num_datanodes >= 3:
        dfs_replication = 3
    hdfs_service_config = {
        'dfs_replication': dfs_replication,
    }
    hdfs_service.update_config(hdfs_service_config)
    edit_logger.info("Current replication factor is " + str(current_replication))
    edit_logger.info("Updating the replication factor to " + str(dfs_replication))

    # fix namenode config
    namenode_role = hdfs_service.get_role_config_group("HDFS-NAMENODE-BASE")
    datanode_role_config = hdfs_service.get_role_config_group("HDFS-DATANODE-BASE")
    num_datanodes = len(datanode_role_config.get_all_roles())
    namenode_handler_count = max(int(ln(num_datanodes) * 20), 30)
    namenode_config = {
        'dfs_namenode_handler_count': namenode_handler_count,
        'dfs_namenode_service_handler_count': namenode_handler_count
    }
    namenode_role.update_config(namenode_config)
    edit_logger.info("Updating namenode config with {0}".format(namenode_config))

def host_swap_alert_off(api):
    swap_alert_off = {"host_memswap_thresholds":'{"warning":"never", "critical":"never"}'}
    Hosts = api.get_all_hosts()
    for Host in Hosts:
      try:
         Host.update_config(swap_alert_off)
      except:
         edit_logger.warn("Exception turning off swap for host " + str(Host))

def main():
    try:
        (api, cluster) = get_cluster()
        if cluster == None:
            if IS_ADD_NODE:
                sys.exit(1)
            else:
                # always exit with 0, for delete
                sys.exit(0)
        cm = api.get_cloudera_manager()
        cloudera_services = map(lambda Service: Service.name, cluster.get_all_services())
        if ROLE=="worker":
            if IS_ADD_NODE:
                # wait for all cloudera agents to be up
                BDVLIB_ServiceWait([["services", "cloudera_scm_agent", NODE_GROUP_ID, "worker"]])
                # make sure cloudera manager has received registration#for all new agents
                while True:
                    current_all_hosts = map(lambda x: x.hostname, api.get_all_hosts())
                    edit_logger.info("Currently registered hosts with CM " + str(current_all_hosts))
                    if all(x in current_all_hosts for x in CHANGED_HOSTS):
                        break
                    edit_logger.info("waiting for new nodes to register with cloudera manager")
                    time.sleep(10)
                cluster.add_hosts(CHANGED_HOSTS)
                activate_parcel(cluster)
                # turn off swap alerting for the new hosts
                host_swap_alert_off(api)
                # set java_home for all hosts
                hosts_set_javahome(api)
                # add datanode and other worker services to new node
                if HDFS_SERVICE_NAME in cloudera_services:
                    hdfs_service = cluster.get_service(HDFS_SERVICE_NAME)
                    add_worker(CHANGED_HOSTS, hdfs_service,HDFS_SERVICE_NAME, HDFS_DN_NAME, HDFS_DN_ROLE_PREFIX)
                if YARN_SERVICE_NAME in cloudera_services:
                    yarn_service = cluster.get_service(YARN_SERVICE_NAME)
                    add_worker(CHANGED_HOSTS, yarn_service , YARN_SERVICE_NAME, YARN_NM_NAME, YARN_NM_ROLE_PREFIX)
                    set_split_locations(yarn_service)
                if IMPALA_SERVICE_NAME in cloudera_services:
                    impala_service = cluster.get_service(IMPALA_SERVICE_NAME)
                    add_worker(CHANGED_HOSTS, impala_service, IMPALA_SERVICE_NAME, IMPALAD_NAME, IMPALAD_ROLE_PREFIX)
                if FLUME_SERVICE_NAME in cloudera_services:
                    flume_service=cluster.get_service(FLUME_SERVICE_NAME)
                    add_worker(CHANGED_HOSTS, flume_service, FLUME_SERVICE_NAME, FLUME_AGENT_NAME, FLUME_ROLE_PREFIX)
                if SOLR_SERVICE_NAME in cloudera_services:
                    solr_service =cluster.get_service(SOLR_SERVICE_NAME)
                    add_worker(CHANGED_HOSTS, solr_service, SOLR_SERVICE_NAME, SOLR_SERVER_NAME, SOLR_ROLE_PREFIX)
                
                if SQOOP_CLIENT_SERVICE_NAME in cloudera_services:
                   sqoop_client = cluster.get_service(SQOOP_CLIENT_SERVICE_NAME)
                   add_gateway(CHANGED_HOSTS, sqoop_client , SQOOP_CLIENT_SERVICE_NAME , SQOOP_CLIENT_GW_NAME, SQOOP_CLIENT_GW_ROLE_PREFIX)
                if HBASE_SERVICE_NAME in cloudera_services:
                    hbase_service = cluster.get_service(HBASE_SERVICE_NAME)
                    add_worker(CHANGED_HOSTS, hbase_service, HBASE_SERVICE_NAME, HBASE_RS_NAME, HBASE_RS_ROLE_PREFIX)

                if IS_KERBEROS_ENABLED:
                    run_cm_command({'cluster':cluster}, "cluster.deploy_client_config()", "Deploy client configuration")
                    run_cm_command({'cluster': cluster}, "cluster.deploy_cluster_client_config()", "Deploy kerberos client configuration")
                    run_cm_command({'cm': cm}, "cm.generate_credentials()", "Generate missing kerberos credentials", False)
            else:
                edit_logger.info("Issuing command to decommission hosts")
                cm.hosts_decommission(CHANGED_HOSTS).wait()
                edit_logger.info("Decommission complete. Deleting the host from cluster")
                delete_hosts(api, cluster, CHANGED_HOSTS)
                if YARN_SERVICE_NAME in cloudera_services:
                    yarn_service = cluster.get_service(YARN_SERVICE_NAME)
                    set_split_locations(yarn_service)

            # fix the replication factor of HDFS after the add/delete worker
            if HDFS_SERVICE_NAME in cloudera_services:
                hdfs_service = cluster.get_service(HDFS_SERVICE_NAME)
                fix_hdfs_config(hdfs_service)
                # the refresh command does not offer wait() call, hence this
                hdfs_service.refresh("HDFS-nn")
                while len(hdfs_service.get_commands()) > 0:
                    edit_logger.info("waiting for refresh HDFS to complete")
                    time.sleep(5)
            # refresh YARN service
            if YARN_SERVICE_NAME in cloudera_services:
                yarn_service = cluster.get_service(YARN_SERVICE_NAME)
                yarn_service.refresh("YARN-rm")
                while len(yarn_service.get_commands()) > 0:
                    edit_logger.info("waiting for refresh YARN to complete")
                    time.sleep(5)
            # wait for all existing commands to quiesce before continuing
            while len(cluster.get_commands()) > 0:
                edit_logger.info("waiting for cluster to quiesce")
                time.sleep(5)
            run_cm_command({'cluster': cluster}, "cluster.deploy_client_config()",'deploy client configuration')
            if IS_ADD_NODE:
                # start the new cluster services if they are not already started
                run_cm_command({'cluster': cluster}, "cluster.start()", "start cluster services which are down")
                run_cm_command({'cluster': cluster},"cluster.restart(restart_only_stale_services=True, redeploy_client_configuration=True)","restart stale cluster and redeploying client configuration")

            if IS_DEL_NODE:
                for deleted_hostId in deleted_hostId_list:
                    try:
                        api.delete_host(deleted_hostId)
                        run_cm_command({'cluster': cluster}, "cluster.deploy_client_config()",'deploy client configuration')
                        run_cm_command({'cluster': cluster},"cluster.restart(restart_only_stale_services=True, redeploy_client_configuration=True)","restart stale cluster and redeploying client configuration")
                        edit_logger.info("Host was deleted: " + deleted_hostId)
                    except:
                        edit_logger.info("Host was already deleted: " + deleted_hostId)
                        continue
            edit_logger.info("Successfully modified the number of nodes in the cluster")

        if ROLE=="edge":
            if IS_ADD_NODE:
                # wait for all cloudera agents to be up
                BDVLIB_ServiceWait([["services", "cloudera_scm_agent",
                NODE_GROUP_ID, "edge"]])
                # make sure cloudera manager has received registration#for all new agents
                while True:
                    current_all_hosts = map(lambda x: x.hostname, api.get_all_hosts())
                    edit_logger.info("Currently registered hosts with CM " + str(current_all_hosts))
                    if all(x in current_all_hosts for x in CHANGED_HOSTS):
                        break
                    edit_logger.info("waiting for new nodes to register with cloudera manager")
                    time.sleep(10)
                cluster.add_hosts(CHANGED_HOSTS)
                activate_parcel(cluster)
                # turn off swap alerting for the new hosts
                host_swap_alert_off(api)
                # set java_home for all hosts
                hosts_set_javahome(api)
                if HIVE_SERVICE_NAME in cloudera_services:
                    hive_service = cluster.get_service(HIVE_SERVICE_NAME)
                    add_gateway(CHANGED_HOSTS, hive_service, HIVE_GW_SERVICE_NAME, HIVE_GW_NAME, HIVE_GW_ROLE_PREFIX)
                if HDFS_SERVICE_NAME in cloudera_services:
                    hdfs_service=cluster.get_service(HDFS_SERVICE_NAME)
                    add_gateway(CHANGED_HOSTS, hdfs_service, HDFS_GW_SERVICE_NAME, HDFS_GW_NAME, HDFS_GW_ROLE_PREFIX)
                if YARN_SERVICE_NAME in cloudera_services:
                    yarn_service = cluster.get_service(YARN_SERVICE_NAME)
                    add_gateway(CHANGED_HOSTS, yarn_service, YARN_GW_SERVICE_NAME, YARN_GW_NAME, YARN_GW_ROLE_PREFIX)
                if SPARK_GW_SERVICE_NAME in cloudera_services:
                    spark_service=cluster.get_service(SPARK_GW_SERVICE_NAME)
                    add_gateway(CHANGED_HOSTS, spark_service, SPARK_GW_SERVICE_NAME, SPARK_GW_NAME, SPARK_GW_ROLE_PREFIX)
                if SQOOP_CLIENT_SERVICE_NAME in cloudera_services:
                   sqoop_client = cluster.get_service(SQOOP_CLIENT_SERVICE_NAME)
                   add_gateway(CHANGED_HOSTS, sqoop_client , SQOOP_CLIENT_SERVICE_NAME , SQOOP_CLIENT_GW_NAME, SQOOP_CLIENT_GW_ROLE_PREFIX)
                if SPARK2_GW_SERVICE_NAME in cloudera_services:
                    spark2_service = cluster.get_service(SPARK2_GW_SERVICE_NAME)
                    add_gateway(CHANGED_HOSTS, spark2_service, SPARK2_GW_SERVICE_NAME, SPARK2_GW_NAME, SPARK2_GW_ROLE_PREFIX)
                if SENTRY_GW_SERVICE_NAME in cloudera_services:
                    sentry_service=cluster.get_service(SENTRY_GW_SERVICE_NAME)
                    add_gateway(CHANGED_HOSTS, sentry_service, SENTRY_GW_SERVICE_NAME, SENTRY_GW_NAME,SENTRY_GW_ROLE_PREFIX)

                if IS_KERBEROS_ENABLED:
                    run_cm_command({'cluster':cluster}, "cluster.deploy_client_config()", "Deploy client configuration")
                    run_cm_command({'cluster': cluster}, "cluster.deploy_cluster_client_config()", "Deploy kerberos client configuration")
                    run_cm_command({'cm': cm}, "cm.generate_credentials()", "Generate missing kerberos credentials", False)
            else:
                edit_logger.info("Issuing command to decommission hosts")
                cm.hosts_decommission(CHANGED_HOSTS).wait()
                edit_logger.info("Decommission complete. Deleting the host from cluster")
                delete_hosts(api, cluster, CHANGED_HOSTS)
            while len(cluster.get_commands()) > 0:
                edit_logger.info("waiting for cluster to quiesce")
                time.sleep(5)
            run_cm_command({'cluster': cluster}, "cluster.deploy_client_config()",'deploy client configuration')
            if IS_ADD_NODE:
                # start the new cluster services if they are not already started
                run_cm_command({'cluster': cluster}, "cluster.start()", "start cluster services which are down")
                run_cm_command({'cluster': cluster},"cluster.restart(restart_only_stale_services=True, redeploy_client_configuration=True)","restart stale cluster and redeploying client configuration")

            if IS_DEL_NODE:
                for deleted_hostId in deleted_hostId_list:
                    try:
                        api.delete_host(deleted_hostId)
                        run_cm_command({'cluster': cluster}, "cluster.deploy_client_config()",'deploy client configuration')
                        run_cm_command({'cluster': cluster},"cluster.restart(restart_only_stale_services=True, redeploy_client_configuration=True)","restart stale cluster and redeploying client configuration")
                        edit_logger.info("Host was deleted: " + deleted_hostId)
                    except:
                        edit_logger.info("Host was already deleted: " + deleted_hostId)
                        continue
            edit_logger.info("Successfully modified the number of nodes in the cluster")
    except:
        edit_logger.error(traceback.format_exc())
        if IS_ADD_NODE:
            edit_logger.error("Failed to add nodes to cluster")
            BDVLIB_TokenWake(CDH_PARCEL_VERSION, 'error')
            sys.exit(1)
        else:
            edit_logger.error("Failed to delete nodes from cluster")
            BDVLIB_TokenWake(CDH_PARCEL_VERSION)
            # always exit with 0, for delete
            sys.exit(0)

if __name__ == "__main__":
   main()

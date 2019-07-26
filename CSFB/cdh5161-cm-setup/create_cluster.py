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
FQDN = ConfigMeta.getWithTokens(["node", "fqdn"])
DISTRO_ID = ConfigMeta.getWithTokens(["node", "distro_id"])
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])
CLUSTER_ID = ConfigMeta.getWithTokens(["cluster", "id"])
UNIQUE_NAME = "bdhdfs-" + str(CLUSTER_ID)
DTAP_JAR = "/opt/bluedata/bluedata-dtap.jar"
CLUSTER_NAME = ConfigMeta.getWithTokens(["cluster", "name"])
CDH_MAJOR_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "cdh_major_version"])
CDH_FULL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "cdh_full_version"])
CDH_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "cdh_parcel_repo"])
CDH_PARCEL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "cdh_parcel_version"])
SPARK2_PARCEL_REPO = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "spark2_parcel_repo"])
SPARK2_PARCEL_VERSION = ConfigMeta.getWithTokens(["cluster", "config_metadata",NODE_GROUP_ID, "spark2_parcel_version"])


HOST_SWAP_ALERT_OFF = {"host_memswap_thresholds":'{"warning":"never", "critical":"never"}'}
HOST_JAVA_HOME = {"java_home":'/opt/jdk1.8.0_202'}

PROCESS_SWAP_ALERT_OFF = {'process_swap_memory_thresholds': '{"warning":"never", "critical":"never"}'}

CM_CONFIG = {
  'MISSED_HB_BAD' : 1000,
  'MISSED_HB_CONCERNING' : 1000,
  'TSQUERY_STREAMS_LIMIT' : 1000,
  'REMOTE_PARCEL_REPO_URLS' : CDH_PARCEL_REPO
}

ALL_HOSTS = ConfigMeta.getLocalGroupHosts()
ALL_HOSTS.sort()



### Management Services ###
# If using the embedded postgresql database, the database passwords can be found in /etc/cloudera-scm-server/db.mgmt.properties.
# The values change every time the cloudera-scm-server-db process is restarted.

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


MGMT_SERVICENAME = "MGMT"
MGMT_SERVICE_CONFIG = {
   'zookeeper_datadir_autocreate': 'false',
}
MGMT_ROLE_CONFIG = {
   'quorumPort': 2888,
}
AMON_ROLENAME = "ACTIVITYMONITOR"
AMON_ROLE_CONFIG = {
   'firehose_database_host': CM_HOST,
   'firehose_database_user': 'amon',
   'firehose_database_password': 'amon_password',
   'firehose_database_type': 'mysql',
   'firehose_database_name': 'amon',
   'firehose_heapsize': '400000000',
}
APUB_ROLENAME = "ALERTPUBLISHER"
APUB_ROLE_CONFIG = { }
ESERV_ROLENAME = "EVENTSERVER"
ESERV_ROLE_CONFIG = {
   'event_server_heapsize': '268435456'
}
HMON_ROLENAME = "HOSTMONITOR"
HMON_ROLE_CONFIG = {
  'firehose_non_java_memory_bytes' : '6442450944'
}
SMON_ROLENAME = "SERVICEMONITOR"
SMON_ROLE_CONFIG = {
  'firehose_non_java_memory_bytes' : '6442450944'
}
NAV_ROLENAME = "NAVIGATOR"
NAV_ROLE_CONFIG = {
   'navigator_database_host': CM_HOST ,
   'navigator_database_user': 'nav',
   'navigator_database_password': 'nav_password',
   'navigator_database_type': 'mysql',
   'navigator_database_name': 'nav',
   'navigator_heapsize': '268435456',
}
NAVMS_ROLENAME = "NAVIGATORMETASERVER"
NAVMS_ROLE_CONFIG = {
   'nav_metaserver_database_host': CM_HOST ,
   'nav_metaserver_database_user': 'navms',
   'nav_metaserver_database_password': 'navms_password',
   'nav_metaserver_database_type': 'mysql',
   'nav_metaserver_database_name': 'navms',
   'navigator_heapsize': '600000000',
}
RMAN_ROLENAME = "REPORTMANAGER"
RMAN_ROLE_CONFIG = {
   'headlamp_database_host': CM_HOST ,
   'headlamp_database_user': 'rman',
   'headlamp_database_password': 'rman_password',
   'headlamp_database_type': 'mysql',
   'headlamp_database_name': 'rman',
   'headlamp_heapsize': '285964392',
}

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


def hosts_swap_alert_off(api):
   Hosts = api.get_all_hosts()
   for Host in Hosts:
      try:
         Host.update_config(HOST_SWAP_ALERT_OFF)
      except:
         setup_logger.warn("Exception turning off swap for host " + str(Host))


##Nanda: Add Spark parcel function call
def add_spark2_repo(api):
    ## Add Spark2 URL
    cm = api.get_cloudera_manager()
    config = cm.get_config(view='summary')
    parcel_urls = config.get("REMOTE_PARCEL_REPO_URLS","").split(",")
    if SPARK2_PARCEL_REPO in parcel_urls:
        # already there, skip
        setup_logger.info("SPARK2 repo url already included")
    else:
        parcel_urls.append(SPARK2_PARCEL_REPO)
        config["REMOTE_PARCEL_REPO_URLS"] = ",".join(parcel_urls)
        cm.update_config(config)
        # wait to make sure parcels are refreshed
        time.sleep(10)
        setup_logger.info("Added SPARK2 repo url")

###Nanda: Set java home function ###
def hosts_set_javahome(api):
   Hosts = api.get_all_hosts()
   for Host in Hosts:
      try:
         Host.update_config(HOST_JAVA_HOME)
      except:
         setup_logger.warn("Exception setting java home for host" + str(Host))



def init_cluster():
    # wait for all cloudera agent processes to come up
    setup_logger.info("Creating Clutser.")
    BDVLIB_ServiceWait([["services", "cloudera_scm_agent", NODE_GROUP_ID]])
    # make sure cloudera manager has received registration
    # for all new agents
    all_cloudera_hosts = get_hosts_for_service(["services", "cloudera_scm_agent"])
    api = ApiResource(CM_HOST, username = ADMIN_USER, password = ADMIN_PASS)
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
    cluster.add_hosts(ALL_HOSTS)
    
    # turn off host swap alerting
    hosts_swap_alert_off(api)
   
    setup_logger.info("Setting Up SPARK2 Repo....")
    add_spark2_repo(api)
    ##Set java home
    setup_logger.info("Setting Up Java Path....")
    hosts_set_javahome(api)

    return (cluster, manager)

def activate_parcel(cluster):
    ## after we create a cluster, parcels may not show up immediately
    ## retry the operation to get parcels. We never timeout here.
    retry_count = 1
    while len(cluster.get_all_parcels()) == 0:
        setup_logger.info("Retrying parcel get operation")
        time.sleep(10)
    p = cluster.get_parcel('CDH', CDH_PARCEL_VERSION)
    if p.stage != "DOWNLOADED" and p.stage != "ACTIVATED":
        raise Exception("parcels should have been downloaded in the image")
    p.start_distribution()
    while True:
        p = cluster.get_parcel('CDH', CDH_PARCEL_VERSION)
        state = p.state
        if p.stage == "DISTRIBUTED" and state.progress == state.totalProgress:
            break
        if p.state.errors:
            raise Exception(str(p.state.errors))
        setup_logger.info(p)
        time.sleep(10)
    p.activate()
    while True:
        p = cluster.get_parcel('CDH', CDH_PARCEL_VERSION)
        state = p.state
        if p.stage == "ACTIVATED" and state.progress == state.totalProgress:
            break
        if p.state.errors:
            raise Exception(str(p.state.errors))
        setup_logger.info(p)
        time.sleep(10)


def activate_spark2(cluster):
    ## after we create a cluster, parcels may not show up immediately
    ## retry the operation to get parcels. We never timeout here.
    retry_count = 1
    while len(cluster.get_all_parcels()) == 0:
        setup_logger.info("Retrying parcel get operation")
        time.sleep(10)
    ## Start downloadig
    p = cluster.get_parcel('SPARK2', SPARK2_PARCEL_VERSION)
    if p.stage != "DOWNLOADED" and p.stage != "ACTIVATED":
        raise Exception("parcels should have been downloaded in the image")
    p.start_distribution()
    while True:
        p = cluster.get_parcel('SPARK2', SPARK2_PARCEL_VERSION)
        state = p.state
        if p.stage == "DISTRIBUTED" and state.progress == state.totalProgress:
            break
        if p.state.errors:
            raise Exception(str(p.state.errors))
        setup_logger.info(p)
        time.sleep(10)
    p.activate()
    while True:
        p = cluster.get_parcel('SPARK2', SPARK2_PARCEL_VERSION)
        state = p.state
        if p.stage == "ACTIVATED" and state.progress == state.totalProgress:
            break
        if p.state.errors:
            raise Exception(str(p.state.errors))
        setup_logger.info(p)
        time.sleep(10)


def deploy_management(manager, mgmt_servicename, mgmt_service_conf, mgmt_role_conf, amon_role_name, amon_role_conf, apub_role_name, apub_role_conf, eserv_role_name, eserv_role_conf, hmon_role_name, hmon_role_conf, smon_role_name, smon_role_conf, nav_role_name, nav_role_conf, navms_role_name, navms_role_conf, rman_role_name, rman_role_conf):
    
    setup_logger.info("Deploying Management Service on Host: {}".format(CM_HOST))
    mgmt = manager.create_mgmt_service(ApiServiceSetupInfo())

    # create roles. Note that host id may be different from host name (especially in CM 5). Look it it up in /api/v5/hosts
    mgmt.create_role(amon_role_name + "-1", "ACTIVITYMONITOR", CM_HOST)
    mgmt.create_role(apub_role_name + "-1", "ALERTPUBLISHER", CM_HOST)
    mgmt.create_role(eserv_role_name + "-1", "EVENTSERVER", CM_HOST)
    mgmt.create_role(hmon_role_name + "-1", "HOSTMONITOR", CM_HOST)
    mgmt.create_role(smon_role_name + "-1", "SERVICEMONITOR", CM_HOST)
    mgmt.create_role(nav_role_name + "-1", "NAVIGATOR", CM_HOST)
    mgmt.create_role(navms_role_name + "-1", "NAVIGATORMETASERVER", CM_HOST)
    mgmt.create_role(rman_role_name + "-1", "REPORTSMANAGER", CM_HOST)

    # Copy dtap and hadoop jars to cloudera manager shared libs
    shell_command = ['sudo cp -f /opt/bluedata/bluedata-dtap.jar /usr/share/cmf/lib']
    popen_util(shell_command, "copy dtap jar to cm shared libs path")

    # now configure each role
    for group in mgmt.get_all_role_config_groups():
        if group.roleType == "ACTIVITYMONITOR":
            group.update_config(amon_role_conf)
        elif group.roleType == "ALERTPUBLISHER":
            group.update_config(apub_role_conf)
        elif group.roleType == "EVENTSERVER":
            group.update_config(eserv_role_conf)
        elif group.roleType == "HOSTMONITOR":
            group.update_config(hmon_role_conf)
        elif group.roleType == "SERVICEMONITOR":
            group.update_config(smon_role_conf)
        elif group.roleType == "NAVIGATOR":
            group.update_config(nav_role_conf)
        elif group.roleType == "NAVIGATORMETASERVER":
            group.update_config(navms_role_conf)
        elif group.roleType == "REPORTSMANAGER":
            group.update_config(rman_role_conf)

    # now start the management service
    mgmt.start().wait()

    return mgmt

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


def main():
    try:
        (CLUSTER, MANAGER)  = init_cluster()
        setup_logger.info("Initialized cluster " + CLUSTER_NAME + " which uses CDH version " + CDH_FULL_VERSION)

        activate_parcel(CLUSTER)
        setup_logger.info("Activated parcels")
        
        activate_spark2(CLUSTER)
        setup_logger.info("Activated Spark2 parcels")
        
        ## Start the rest after activating spark2 parcel
        BDVLIB_TokenWake(CDH_PARCEL_VERSION)
        mgmt = deploy_management(MANAGER, MGMT_SERVICENAME, MGMT_SERVICE_CONFIG, MGMT_ROLE_CONFIG, AMON_ROLENAME, AMON_ROLE_CONFIG, APUB_ROLENAME, APUB_ROLE_CONFIG, ESERV_ROLENAME, ESERV_ROLE_CONFIG, HMON_ROLENAME, HMON_ROLE_CONFIG, SMON_ROLENAME, SMON_ROLE_CONFIG, NAV_ROLENAME, NAV_ROLE_CONFIG, NAVMS_ROLENAME, NAVMS_ROLE_CONFIG, RMAN_ROLENAME, RMAN_ROLE_CONFIG)
        #turn off management services swap alerting
        service_swap_alert_off(mgmt)
        setup_logger.info("Deployed CM management service " + MGMT_SERVICENAME + " to run on " + CM_HOST)
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

#!/bin/env python

#
# Copyright (c) 2015, BlueData Software, Inc.
#
# YARN tuning script based on HORTONWORKS documentation

from bd_vlib import *
import math
import logging, sys

tuning_logger = logging.getLogger("bluedata_adv_tuning")
tuning_logger.setLevel(logging.DEBUG)
hdlr = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
hdlr.setFormatter(formatter)
hdlr.setLevel(logging.DEBUG)
tuning_logger.addHandler(hdlr)

ConfigMeta = BDVLIB_ConfigMetadata()
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])
DISTRO_ID = ConfigMeta.getWithTokens(["node", "distro_id"])

## Get the V_CPU of workers if they are defined, fall back to other roles
## if not. This is used to set the maximum number of cores
worker_vcpu = None
arbiter_vcpu = None
standby_vcpu = None
controller_vcpu = None
for flavor_cores in ConfigMeta.searchForToken(["distros", DISTRO_ID,
   NODE_GROUP_ID], "cores"):
    if "worker" in flavor_cores:
        worker_vcpu = ConfigMeta.getWithTokens(flavor_cores)
        break
    if "arbiter" in flavor_cores:
        arbiter_vcpu = ConfigMeta.getWithTokens(flavor_cores)
    if "standby" in flavor_cores:
        standby_vcpu = ConfigMeta.getWithTokens(flavor_cores)
    if "cmserver" in flavor_cores:
        controller_vcpu = ConfigMeta.getWithTokens(flavor_cores)

V_CPU = 1
V_MEM = None
if worker_vcpu:
    V_CPU = int(worker_vcpu)
    V_MEM = int(ConfigMeta.getWithTokens(["distros", DISTRO_ID,
        NODE_GROUP_ID, "roles", "worker", "flavor", "memory"]))
elif arbiter_vcpu:
    V_CPU = int(arbiter_vcpu)
    V_MEM = int(ConfigMeta.getWithTokens(["distros", DISTRO_ID,
        NODE_GROUP_ID, "roles", "arbiter", "flavor", "memory"]))
elif standby_vcpu:
    V_CPU = int(standby_vcpu)
    V_MEM = int(ConfigMeta.getWithTokens(["distros", DISTRO_ID,
        NODE_GROUP_ID, "roles", "standby", "flavor", "memory"]))
elif controller_vcpu:
    V_CPU = int(controller_vcpu)
    V_MEM = int(ConfigMeta.getWithTokens(["distros", DISTRO_ID,
        NODE_GROUP_ID, "roles", "controller", "flavor", "memory"]))

RESERVED_STACK = {4096:1024, 8192:2048, 16384:2048, 24576:4096,
                  49152:6144, 65536:8192, 73728:8192, 98304:12288,
                  131072:24576, 262144:32768, 524288:65536}
RESERVED_HBASE = {4096:1024, 8192:1024, 16384:2048, 24576:4096,
                  49152:8192, 65536:8192, 73728:8192, 98304:16384,
                  131072:24576, 262144:32768, 524288:65536}

def interpolate_value(map, memory):
    if map.has_key(memory):
        return map[memory]
    keys = map.keys()
    list.sort(keys)
    min_key = keys[0]
    max_key = keys[len(keys) - 1]
    if memory < min_key:
        return map[min_key]
    if memory > max_key:
        return map[max_key]
    for key in keys[1:]:
        if memory < key:
            max_key = key
            break
        min_key = key
    min_value = map[min_key]
    max_value = map[max_key]
    tuning_logger.info("Min_val={0}, max_val={1}".format(min_value, max_value))
    ret = min_value +\
          ( 1.0 * (memory - min_key)/(max_key - min_key) *\
          (max_value - min_value))
    return ret

def get_reserved_hbase(memory):
    return interpolate_value(RESERVED_HBASE, memory)

def get_reserved_stack(memory):
    return interpolate_value(RESERVED_STACK, memory)

def get_min_container_size(memory):
    if (memory <= 4096):
        return 256
    elif (memory <= 8192):
        return 512
    elif (memory <= 24576):
        return 1024
    else:
        return 2048
    pass

def get_rounded_memory(memory):
    denominator = 128
    if (memory > 4096):
        denominator = 1024
    elif (memory > 2048):
        denominator = 512
    elif (memory > 1024):
        denominator = 256
    else:
        denominator = 128
    return int(math.floor(memory/denominator)) * denominator

def tune_yarn(cluster):
    yarn_service = cluster.get_service("YARN")
    min_container_size = get_min_container_size(V_MEM)
    reserved_memory = get_reserved_stack(V_MEM)
    tuning_logger.info("Calculated min_container_size={0}, reserved_memory={1}".format(min_container_size, reserved_memory))
    if IS_HBASE_ENABLED:
        hbase_memory = get_reserved_hbase(V_MEM)
        reserved_memory += hbase_memory
    memory = V_MEM - reserved_memory
    if (V_MEM < 2):
        memory = 2
    min_container_size = get_min_container_size(memory)
    containers = int(max(3, min(2 * V_CPU, memory/min_container_size)))
    # ram per container in mb
    ram_per_container =  get_rounded_memory(abs(memory/containers))
    tuning_logger.info("Calculated containers={0}, ram_per_container={1}".format(containers, ram_per_container))
    mb = 1024 * 1024
    YARN_NM_CONFIG = {
        'yarn_nodemanager_resource_cpu_vcores': V_CPU,
        'yarn_nodemanager_resource_memory_mb': containers * ram_per_container
    }
    tuning_logger.info("Setting YARN NM config to {0}".format(YARN_NM_CONFIG))
    yarn_nm_role = yarn_service.get_role_config_group("YARN-NODEMANAGER-BASE")
    yarn_nm_role.update_config(YARN_NM_CONFIG)

    YARN_RM_CONFIG = {
        'yarn_scheduler_minimum_allocation_mb': ram_per_container,
        'yarn_scheduler_maximum_allocation_mb': containers * ram_per_container,
        'yarn_scheduler_maximum_allocation_vcores': V_CPU
    }
    tuning_logger.info("Setting YARN RM config to {0}".format(YARN_RM_CONFIG))
    yarn_rm_role = yarn_service.get_role_config_group("YARN-RESOURCEMANAGER-BASE")
    yarn_rm_role.update_config(YARN_RM_CONFIG)

    YARN_GW_CONFIG ={
        'yarn_app_mapreduce_am_resource_mb': int(math.floor(2 * ram_per_container)),
        'yarn_app_mapreduce_am_max_heap': int(math.floor(0.8 * 2 * ram_per_container * mb)),
        'mapreduce_map_memory_mb': ram_per_container,
        'mapreduce_reduce_memory_mb': 2 * ram_per_container,
        'mapreduce_map_java_opts_max_heap': int(math.floor(0.8 * ram_per_container * mb)),
        'mapreduce_reduce_java_opts_max_heap': int(math.floor(0.8 * 2 * ram_per_container * mb)),
        'io_sort_mb': int(min(math.floor(0.4 * ram_per_container), 2047))
    }
    tuning_logger.info("Setting YARN GW config to {0}".format(YARN_GW_CONFIG))
    yarn_gw_role = yarn_service.get_role_config_group("YARN-GATEWAY-BASE")
    yarn_gw_role.update_config(YARN_GW_CONFIG)

def tune_mrv1(cluster):
    mapred_service = cluster.get_service("MAPRED")
    #It is best practice to set the number of reducers to half the cores
    REDUCE_TASKS = 1 if V_CPU == 1 else V_CPU/2
    MAPRED_TT_CONFIG = {
        'mapred_tasktracker_reduce_tasks_maximum': REDUCE_TASKS,
        'mapred_tasktracker_map_tasks_maximum': V_CPU,
    }
    tuning_logger.info("Setting MAPRED TT config to {0}".format(MAPRED_TT_CONFIG))
    mapred_tt_role = mapred_service.get_role_config_group("MAPRED-TASKTRACKER-BASE")
    mapred_tt_role.update_config(MAPRED_TT_CONFIG)
    MAPRED_GW_CONFIG = {
        'mapred_reduce_tasks': REDUCE_TASKS
    }
    tuning_logger.info("Setting MAPRED GW config to {0}".format(MAPRED_GW_CONFIG))
    mapred_gw_config = mapred_service.get_role_config_group("MAPRED-GATEWAY-BASE")
    mapred_gw_config.update_config(MAPRED_GW_CONFIG)

def tune_hbase(cluster):
    hbase_memory = int(get_reserved_hbase(V_MEM))
    tuning_logger.info("Calculated HBase memory is {0}".format(hbase_memory))
    HBASE_RS_CONFIG = {
        'hbase_regionserver_java_heapsize': hbase_memory * 1024 * 1024
    }
    tuning_logger.info("Setting HBASE RS config to {0}".format(HBASE_RS_CONFIG))
    hbase_service = cluster.get_service("HBASE")
    rs_config = hbase_service.get_role_config_group("HBASE-REGIONSERVER-BASE")
    rs_config.update_config(HBASE_RS_CONFIG)

def tune_spark(cluster):
    # We just give spark the full quota of memory and cores here, as
    # though there was just a spark cluster running. Ideally we should
    # manage spark through yarn
    SPARK_ENV = 'SPARK_WORKER_CORES=' + str(V_CPU)
    if V_MEM:
        SPARK_WORKER_MEMORY = str(V_MEM - 1024) + 'm'
        SPARK_ENV = SPARK_ENV + '\nSPARK_WORKER_MEMORY=' + SPARK_WORKER_MEMORY
    else:
        tuning_logger.warn("Cannot determine worker memory. Spark may run in degraded state")
    SPARK_SERVICE_CONFIG = {
        # For CDH5.4.3.: 'config/spark-env.sh_service_safety_valve': SPARK_ENV
        #'SPARK_service_env_safety_valve': SPARK_ENV
    }
    tuning_logger.info("Setting SPARK service config to {0}".format(SPARK_SERVICE_CONFIG))
    spark_service = cluster.get_service("SPARK")
    spark_service.update_config(SPARK_SERVICE_CONFIG)

def tune_impala(cluster):
    # TODO: manage memory through yarn?
    # For now just give impalad 1G or 2G based on flavor
    if V_MEM > 2048:
        IMPALA_MEMORY = str(2048 * 1024 * 1024)
    else:
        IMPALA_MEMORY = str(1024 * 1024 * 1024)
    IMPALA_ID_CONFIG = {"impalad_memory_limit":IMPALA_MEMORY}
    impala_service = cluster.get_service("IMPALA")
    tuning_logger.info("Setting IMPALA daemon config to {0}".format(IMPALA_ID_CONFIG))

    impalad_config = impala_service.get_role_config_group("IMPALA-IMPALAD-BASE")
    impalad_config.update_config({'impalad_memory_limit':str(2048 * 1024 * 1024)})

def tune_cluster(cluster):
    tuning_logger.info("Using worker flavor, CORES={0}, MEMORY={1}, IS_HBASE_ENABLED={2}".format(V_CPU, V_MEM, IS_HBASE_ENABLED))
    if FRAMEWORK != "yarn":
        tune_mrv1(cluster)
    else:
        tune_yarn(cluster)
    if IS_HBASE_ENABLED:
        tune_hbase(cluster)
    if IS_SPARK_ENABLED:
        tune_spark(cluster)
    if IS_APPS_ENABLED:
        tune_impala(cluster)

if __name__ == '__main__':
    tune_yarn_cluster(None)

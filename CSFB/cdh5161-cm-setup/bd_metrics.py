#!/usr/bin/env python

###----------------------------------------------------------------------------------------------
### File    : bd_metrics.py
###
### Content : gmond plugin to support cpu and memory metrics based on cgroup
###
### Copyright 2015 BlueData Software, Inc.
###-----------------------------------------------------------------------------------------------

import re
import os
import multiprocessing
import sys
import time
 
CGROUP_DIR="/cgroup"

# Fetch current container id which will be required for all the functions
# below
try:
    with open('/proc/self/cgroup', 'r') as f:
        for line in f.readlines():
            if 'cpuacct' in line:
                CONTAINER_PATH=line.split(":")[2].strip()
                break
except:
    sys.exit(-1)

cpuCount = multiprocessing.cpu_count()
clockTicks = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
cpus = multiprocessing.cpu_count()

descriptors = []

CPU_PATH="/cgroup/cpu/" + CONTAINER_PATH
CPUACCT_PATH="/cgroup/cpuacct/" + CONTAINER_PATH
MEM_PATH="/cgroup/memory/" + CONTAINER_PATH

Desc_Skel = {
    'name'        : 'XXX',
    'orig_name'   : '',
    'time_max'    : 60,
    'value_type'  : 'float',
    'format'      : '%.1f',
    'units'       : 'XXX',
    'slope'       : 'both',  # zero|positive|negative|both
    'description' : 'XXX',
    'groups'      : 'XXX'
}

def create_desc(skel, prop):
    d = skel.copy()
    for k, v in prop.iteritems():
        d[k] = v
    return d

def getMemTotal():
    with open (MEM_PATH + "/memory.limit_in_bytes") as f:
        return int(f.read())

def getMemUsage():
    with open (MEM_PATH + "/memory.usage_in_bytes") as f:
        return int(f.read())

def getSwapTotal():
    swapTotal = 0
    with open (MEM_PATH + "/memory.memsw.limit_in_bytes") as f:
        return int(f.read())

def getSwapUsage():
    with open (MEM_PATH + "/memory.memsw.usage_in_bytes") as f:
        return int(f.read())

def getMemoryStat():
    with open (MEM_PATH + "/memory.stat") as f:
        return dict(((k, int(v)) for k, v in (line.split(' ') for line in f)))

# Return total memory/swap in KB
def get_mem_info(name):
    if name == "mem_total":
        return getMemTotal()/1024
    elif name == "mem_free":
        return (getMemTotal() - getMemUsage())/1024
    elif name == "mem_cached":
        return getMemoryStat()['total_cache']/1024
    elif name == "swap_total":
        return getSwapTotal()/1024
    elif name == "swap_free":
        return (getSwapTotal() - getSwapUsage())/1024
    elif name == "mem_shared":
        # cgroup doesn't provide this metric.
        return 0
    else:
        # Unknown name
        return 0

def getCgroupUsage():
    with open(CPUACCT_PATH + "/cpuacct.usage", 'rb') as f:
        return int(f.read())

def get_cpu_info(name):
    with open (CPU_PATH + "/cpu.shares") as f:
        cpuShares = int(f.read())

    with open(CPUACCT_PATH + '/cpuacct.stat', 'r') as f:
        cpuStat = dict(((k, int(v)) for k, v in (line.split(' ') for line in f)))

    # A fake representation of number of cpus, we are just going to use
    # the shares to mimic the number of cpus here.
    if name == "cpu_num":
        return cpuShares/1024

    startUsage = getCgroupUsage()
    time.sleep(1)
    endUsage = getCgroupUsage()

    delta = float((endUsage - startUsage) / 1000000000)/cpus

    # gmond requires cpu_system and cpu_idle. This information is not part of 
    # cgroups, we won't be able to provide this. What our UI cares about is cpu_user
    if name == "cpu_user":
        return delta * 100
    elif name == "cpu_system":
        return 0
    elif name == "cpu_idle":
        return 0
    else:
        return 1

def initCPUMetrics():
    global descriptors, Desc_Skel

    # Register all metrics here
    descriptors.append(create_desc(Desc_Skel, {
                "name"       : "cpu_num",
                "description": "Total number of CPUs",
                "units" : "uint32",
                "groups" : 'cpu',
                'slope' : "zero",
                'call_back' : get_cpu_info
                }))
    descriptors.append(create_desc(Desc_Skel, {
                "name"       : "cpu_user",
                "description": "Percentage of CPU utilization that occurred while executing at the user level",
                "units" : "%",
                "groups" : 'cpu',
                'call_back' : get_cpu_info
                }))
    descriptors.append(create_desc(Desc_Skel, {
                "name"       : "cpu_system",
                "description": "Percentage of CPU utilization that occurred while executing at the system level",
                "units" : "%",
                "groups" : 'cpu',
                'call_back' : get_cpu_info
                }))
    descriptors.append(create_desc(Desc_Skel, {
                "name"       : "cpu_idle",
                "description": "Percentage of time that the CPU or CPUs were idle and the system did not have an outstanding disk I/O request",
                "units" : "%",
                "groups" : 'cpu',
                'call_back' : get_cpu_info
                }))

def initMemoryMetrics():
    global descriptors, Desc_Skel

    descriptors.append(create_desc(Desc_Skel, {
                "name"          : "mem_total",
                "units"         : "KB",
                "description"   : "Total amount of memory displayed in KBs",
                "groups"        : "memory",
                'call_back'     : get_mem_info,
                'slope' : "zero"
                }))

    descriptors.append(create_desc(Desc_Skel, {
                "name"       : "mem_free",
                "units"      : "KB",
                "description": "Amount of available memory",
                "groups" : "memory",
                "call_back" : get_mem_info
                }))

    descriptors.append(create_desc(Desc_Skel, {
                "name"       : "mem_shared",
                "units"      : "KB",
                "description": "Amount of shared memory",
                "groups" : "memory",
                "call_back" : get_mem_info
                }))

    descriptors.append(create_desc(Desc_Skel, {
                "name"       : "mem_cached",
                "units"      : "KB",
                "description": "Amount of cached memory",
                "groups" : "memory",
                "call_back" : get_mem_info
                }))

    descriptors.append(create_desc(Desc_Skel, {
                "name"       : "swap_free",
                "units"      : "KB",
                "description": "Amount of available swap memory",
                "groups"     : "memory",
                "call_back"  : get_mem_info
                }))

    descriptors.append(create_desc(Desc_Skel, {
                "name"       : "swap_total",
                "units"      : "KB",
                "description": "Total amount of swap space displayed in KBs",
                "groups" : "memory",
                'call_back' : get_mem_info,
                'slope' : "zero"
                }))

def metric_init(params):
    global descriptors, Desc_Skel

    initCPUMetrics()
    initMemoryMetrics()
    return descriptors

def metric_cleanup():
    pass

# Used only for debug. gmond will directly invoke metric_init function
if __name__ == '__main__':
    metric_init({})
    for d in descriptors:
        v = d['call_back'](d['name'])
        print 'value for %s is %u' % (d['name'],  v)        

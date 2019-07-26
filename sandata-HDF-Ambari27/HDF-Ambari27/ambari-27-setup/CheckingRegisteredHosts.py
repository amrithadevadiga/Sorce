#!/bin/env python

from bd_vlib import *
from subprocess import check_call
import requests, time, sys, json, logging
from optparse import OptionParser
import json,os
import time
ConfigMeta = BDVLIB_ConfigMetadata()
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])
AMBARI_SERVER = ConfigMeta.getWithTokens(["services","ambari", NODE_GROUP_ID, "controller", "fqdns"])[0]
AMBARI_URL = 'http://' + AMBARI_SERVER + ':8080/api/v1'
AUTH = ('admin', 'admin')
HEADERS = {'X-Requested-By': 'ambari'}
ALL_HOSTS = ConfigMeta.getClusterHostsFQDN()
NODEGROUP_HOSTS = ConfigMeta.getNodeGroupFQDN(NODE_GROUP_ID)
AMBARI_USER= ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "ambari_user"])
AMBARI_PASSWORD= ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "ambari_password"])
AMBARI_PORT= ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "ambari_port"])
#SERVICES=['ZOOKEEPER', 'STREAMLINE', 'NIFI', 'KAFKA', 'STORM', 'REGISTRY', 'NIFI_REGISTRY', 'AMBARI_METRICS']
#SERVICES=['ZOOKEEPER', 'NIFI', 'KAFKA', 'STORM', 'AMBARI_METRICS', 'AMBARI_INFRA','NIFI_REGISTRY']
SERVICES=['ZOOKEEPER', 'STREAMLINE', 'NIFI', 'KAFKA', 'STORM', 'REGISTRY', 'NIFI_REGISTRY', 'AMBARI_METRICS']
AMBARI_STACK_NAME="HDF"
HDF_VERSION="3.3"
base_dir = os.environ["CONFIG_BASE_DIR"]


def wait_for_agents():
    while True:
        try:
            resp = requests.get(AMBARI_URL + '/hosts', auth=AUTH, headers=HEADERS)
            if not resp.ok:
                print "Error: Received response from ambari " + str(resp.text) + "retrying"
            else:
                print "Total number of hosts currently registered with Ambari " + str(len(resp.json().get('items')))
                if (len(resp.json().get('items')) == len(ALL_HOSTS)):
                    break
                else:
                    print "Waiting for agents to register."
                    time.sleep(5)
                print "All ambari agents successfully registered"
        except requests.exceptions.ConnectionError:
            print "Ambari Server API Not up yet"
            time.sleep(10)



def generate_host_group_configuration():
    data={
      "recommend" : "host_groups",
      "services" : SERVICES,
      "hosts" : ALL_HOSTS
    }
    with open(base_dir+'/request-host_groups.json', 'w') as fp:
        json.dump(data, fp)

def generate_request_configuration():
    data={
      "recommend" : "configurations",
      "services" : SERVICES,
      "hosts" : ALL_HOSTS
    }
    with open(base_dir+'/request-configurations.json', 'w') as fp:
        json.dump(data, fp)

#Genreate recommend Configuration Json
def generate_configuration():
    url = AMBARI_URL + '/stacks/'+AMBARI_STACK_NAME+'/versions/'+HDF_VERSION+'/recommendations'
    resp1 = requests.post(url , auth=AUTH, headers=HEADERS,data=open(base_dir+'/request-configurations.json', 'rb'))
    resp2=requests.post(url , auth=AUTH, headers=HEADERS,data=open(base_dir+'/request-host_groups.json', 
'rb'))
    if not resp1.ok:
        print resp1.text
        resp1.raise_for_status()
    with open(base_dir+'/configurations.json', 'w') as fp:
        json.dump(resp1.json(), fp)

    if not resp2.ok:
        print resp2.text
        resp2.raise_for_status()
    with open(base_dir+'/host_groups.json', 'w') as fp:
        json.dump(resp2.json(), fp)

def main():
    wait_for_agents()
    generate_host_group_configuration()
    generate_request_configuration()
    generate_configuration()
if __name__ == '__main__':
    main()

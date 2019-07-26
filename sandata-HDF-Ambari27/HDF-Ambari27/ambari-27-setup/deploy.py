#!/usr/bin/python
from bd_vlib import *
import requests, os, time, re, json

ConfigMeta = BDVLIB_ConfigMetadata()
# get cluster name and make it hygienic
ALL_HOSTS = ConfigMeta.getClusterHostsFQDN()
CLUSTER_NAME = re.sub(r'\W+', '', ConfigMeta.getWithTokens(["cluster", "name"]))
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])
AMBARI_SERVER = ConfigMeta.getWithTokens(["services","ambari", NODE_GROUP_ID, "controller", "fqdns"])[0]
AMBARI_URL = 'http://' + AMBARI_SERVER + ':8080/api/v1'
AUTH = ('admin', 'admin')
HEADERS = {'X-Requested-By': 'ambari'}
BLUEPRINT_NAME="bd-hdf"

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

def deploy_blueprint():
    url = AMBARI_URL + '/blueprints/'+BLUEPRINT_NAME
    resp = requests.post(url , auth=AUTH, headers=HEADERS,data=open(base_dir+'/blueprint.json', 'rb'))
    if not resp.ok:
        print resp.text
        resp.raise_for_status()
    else:
        print "Blueprint Deploy Successfully"



def deploy_cluster():
    url = AMBARI_URL + '/clusters/'+CLUSTER_NAME
    resp = requests.post(url , auth=AUTH, headers=HEADERS,data=open(base_dir+'/cluster.json', 'rb'))
    if not resp.ok:
        print resp.text
        resp.raise_for_status()
    else:
        print "Cluster Deploy Successfully"


def check_status():
    url = AMBARI_URL + '/clusters/'+CLUSTER_NAME+'/requests/1'
    resp = requests.get(url, auth=AUTH, headers=HEADERS)
    if not resp.ok:
        print resp.text
        resp.raise_for_status()
    else:
        print resp.json()

def main():
    wait_for_agents()
    print "Deploying Blueprint"
    deploy_blueprint()
    print "Deploying Cluster"
    deploy_cluster()
    print "Status Of Cluster"
    check_status()
    print "done"

if __name__ == '__main__':
    main()



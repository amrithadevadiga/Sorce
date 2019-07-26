
#!/bin/env python2.6
#
# Copyright (c) 2016, BlueData Software, Inc.
#
# Script which makes ambari rest calls

from bd_vlib import *
from subprocess import check_call
import requests, time, sys, json, logging
from optparse import OptionParser

ConfigMeta = BDVLIB_ConfigMetadata()
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])
AMBARI_SERVER = ConfigMeta.getWithTokens(["services","ambari", NODE_GROUP_ID, "controller", "fqdns"])[0]
AMBARI_URL = 'http://' + AMBARI_SERVER + ':8080/api/v1'
AUTH = ('admin', 'admin')
HEADERS = {'X-Requested-By': 'ambari'}
ALL_HOSTS = ConfigMeta.getClusterHostsFQDN()
NODEGROUP_HOSTS = ConfigMeta.getNodeGroupFQDN(NODE_GROUP_ID)
HOST_LIST = sys.argv[len(sys.argv)-1]
AMBARI_USER= ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "ambari_user"])
AMBARI_PASSWORD= ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "ambari_password"])
AMBARI_PORT= ConfigMeta.getWithTokens(["cluster", "config_metadata", NODE_GROUP_ID, "ambari_port"])

parser = OptionParser()
parser.add_option('', '--addnodes', action='store_true', dest='IS_ADD_NODE', default=False, help='')
parser.add_option('', '--delnodes', action='store_true', dest='IS_DEL_NODE', default=False, help='')
parser.add_option('', '--nodegroup',dest='NODE_GROUP', default='', help='')
parser.add_option('', '--fqdns', dest='FQDNS', default='', help='')
parser.add_option('', '--role', dest='role', default='', help='')

modify_hosts_logger = logging.getLogger('modify_hosts_logger')
modify_hosts_logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
modify_hosts_logger.addHandler(handler)

(options, args) = parser.parse_args()

CHANGED_HOSTS = list(options.FQDNS.split(','))
IS_ADD_NODE=options.IS_ADD_NODE
IS_DEL_NODE=options.IS_DEL_NODE
ADD_NODEGROUP=options.NODE_GROUP
ROLE=options.role

def get_blueprint_for_adding_hosts(hosts, role):
    host_blueprint = []
    for host in hosts:
        host_blueprint_entry = {}
        host_blueprint_entry['blueprint'] = 'bd-hdp'
        host_blueprint_entry['host_group'] = role
        host_blueprint_entry['host_name'] = host
        host_blueprint.append(host_blueprint_entry)
    return host_blueprint

def expand_cluster(add_hosts_blueprint):
    print "Expanding Cluster. Adding Hosts: " + json.dumps(add_hosts_blueprint)
    add_hosts_url = AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/hosts'
    resp = requests.post(add_hosts_url , json.dumps(add_hosts_blueprint), auth=AUTH, headers=HEADERS)
    if not resp.ok:
      print resp.text
      resp.raise_for_status()
    response_json=json.loads(resp.text)
    if response_json:
        wait_for_request(str(response_json['Requests']['id']))


def delete_host(host):
    print "Deleting host " + host
    url = AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/hosts/' + host
    retry= True
    while retry:
        resp = requests.delete(url, auth=AUTH, headers=HEADERS)
        if not resp.ok:
            response_json=json.loads(resp.text)
            if response_json and "rolled" in response_json['message']:
                continue
            else:
                print resp.text
                resp.raise_for_status()
        retry = False


def start_components(host):
       components = ['NODEMANAGER','DATANODE','HBASE_REGIONSERVER']
       #components = ['DATANODE']
       for component in components:
         data='{"HostRoles": {"state": "STARTED"}}'
         resp = requests.put(AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/hosts/' + host + '/host_components/' + component, data,auth=AUTH, headers=HEADERS)
         if not resp.ok:
          print resp.text
          resp.raise_for_status()
         wait_for_hostcomponent(component,host,"STARTED")
         print component + " successfully started"

def wait_for_hostcomponent(hostcomponent,host,state):
    count = 0
    while True:
        if count > 600:
            print "Waited 10 mins for host component to come up ..." + hostcomponent + " is still down check ambari UI and try a manual restart !!"
            return
        print "Secs waited .." + str(count)
        url = AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/hosts/' + host + '/host_components/' + hostcomponent
        print "URL:"
        print url
        resp = requests.get(url , auth=AUTH, headers=HEADERS)
        if not resp.ok:
          print resp.text
          resp.raise_for_status()
        response_json=json.loads(resp.text)
        if response_json and ["HostRoles"]["state"] in state:
            return
        else:
            time.sleep(5)
            count = count + 5

def wait_for_request(requestId):
    count = 0
    modify_hosts_logger.info("Waiting for Request: " + requestId + " to be completed...")
    url = AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/requests/' + requestId
    modify_hosts_logger.info("URL:" + url)
    while True:
        if count > 1200:
            modify_hosts_logger.info("ERROR:Waited 20 mins for request" + requestId + " to be completed ...")
            return
        modify_hosts_logger.info("Secs waited .." + str(count))
        resp = requests.get(url , auth=AUTH, headers=HEADERS)
        if not resp.ok:
            modify_hosts_logger.info(resp.text)
            resp.raise_for_status()
        response_json=json.loads(resp.text)
        if response_json and response_json["Requests"]["request_status"] in "COMPLETED":
            return
        else:
            time.sleep(5)
            count = count + 5

def get_host_count():

  url = AMBARI_URL + '/hosts/'
  resp = requests.get(url , auth=AUTH, headers=HEADERS)
  if not resp.ok:
    print resp.text
    resp.raise_for_status()
  response_json=json.loads(resp.text)
  return len(response_json["items"])

def get_request_count():

    url = AMBARI_URL + '/requests/'
    resp = requests.get(url , auth=AUTH, headers=HEADERS)
    if not resp.ok:
        modify_hosts_logger.info(resp.text)
        resp.raise_for_status()
    response_json=json.loads(resp.text)
    modify_hosts_logger.info("Current request count: " + str(len(response_json["items"])))
    return len(response_json["items"])

def decommission_slave(hosts,slave_name):
  print "Decomissioning " + slave_name + "S"
  decommission_request= {}
  decommission_request['RequestInfo'] = {}
  decommission_request['RequestInfo']['context'] = "Decommission DataNodes" if slave_name == "DATANODE" else ("Decommission Nodemanagers" if
                                                   slave_name == "NODEMANAGER" else "Decommission Regionservers")
  decommission_request['RequestInfo']['command'] = "DECOMMISSION"
  decommission_request['RequestInfo']['parameters'] = {}
  decommission_request['RequestInfo']['parameters']['slave_type'] = slave_name
  decommission_request['RequestInfo']['parameters']['excluded_hosts'] = ','.join(hosts)
  decommission_request['RequestInfo']['operation_level']={}
  decommission_request['RequestInfo']['operation_level']['level']="HOST_COMPONENT"
  decommission_request['RequestInfo']['operation_level']['cluster_name']=CLUSTER_NAME
  decommission_request['Requests/resource_filters'] = []
  resource_filter_dict = {}
  resource_filter_dict['service_name'] = "HDFS" if slave_name == "DATANODE" else ("YARN"
                                         if slave_name == "NODEMANAGER" else "HBASE")
  resource_filter_dict['component_name'] = "NAMENODE" if slave_name == "DATANODE" else ("RESOURCEMANAGER"
                                           if slave_name == "NODEMANAGER" else "HBASE_MASTER")
  decommission_request['Requests/resource_filters'].append(resource_filter_dict)
  url = AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/requests/'
  print "URL: " + url
  resp = requests.post(url , json.dumps(decommission_request), auth=AUTH, headers=HEADERS)
  if not resp.ok:
    print resp.text
    resp.raise_for_status()
  response_json=json.loads(resp.text)
  if response_json:
      wait_for_request(str(response_json['Requests']['id']))

def get_components_on_host(host):
  url = AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/hosts/' + host + "/host_components/"
  print "URL: " + url
  resp = requests.get(url , auth=AUTH, headers=HEADERS)
  if not resp.ok:
    print resp.text
    resp.raise_for_status()
  response_json=json.loads(resp.text)
  component_list = []
  for component_details in response_json['items']:
    component_list.append(component_details['HostRoles']['component_name'])
  return component_list

def stop_host_components(host):
    stop_host_components_payload = {
    	"RequestInfo": {
    		"context": "Stop All Host Components"
    	},
    	"Body": {
    		"HostRoles": {
    			"state": "INSTALLED"
    		}
    	}
      }
    url = AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/hosts/' + host + "/host_components/"
    print "Stopping all components on host " + host
    resp = requests.put(url , json.dumps(stop_host_components_payload), auth=AUTH, headers=HEADERS)
    if not resp.ok:
        print resp.text
        resp.raise_for_status()
    response_json=json.loads(resp.text)
    if response_json:
        wait_for_request(str(response_json['Requests']['id']))

def delete_host_components(host):
    url = AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/hosts/' + host + "/host_components/"
    print "Deleting host components on host " + host
    resp = requests.delete(url, auth=AUTH, headers=HEADERS)
    if not resp.ok:
      print resp.text
      resp.raise_for_status()

def stop_services():
  url = AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/services/'
  stop_services_payload = {"ServiceInfo": {"state" : "INSTALLED"}}
  print "URL: " + url
  resp = requests.put(url, json.dumps(stop_services_payload), auth=AUTH, headers=HEADERS)
  if not resp.ok:
    print resp.text
    resp.raise_for_status()
  response_json=json.loads(resp.text)
  if response_json:
      wait_for_request(str(response_json['Requests']['id']))

def start_services():
  url = AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/services/'
  start_services_payload = {"ServiceInfo": {"state" : "STARTED"}}
  print "URL: " + url
  resp = requests.put(url , json.dumps(start_services_payload), auth=AUTH, headers=HEADERS)
  if not resp.ok:
    print resp.text
    resp.raise_for_status()
  response_json=json.loads(resp.text)
  if response_json:
      wait_for_request(str(response_json['Requests']['id']))

def restart_cluster():
  stop_services()
  start_services()

def fix_hdfs_replication():
    url= AMBARI_URL + '/clusters/' + CLUSTER_NAME + '/services/HDFS/components/DATANODE'
    print "URL: " + url
    resp = requests.get(url, auth=AUTH, headers=HEADERS)
    if not resp.ok:
        print resp.text
        resp.raise_for_status()
    response_json=json.loads(resp.text)
    datanode_host_count = len(response_json['host_components'])
    if datanode_host_count < 3:
        check_call(["/var/lib/ambari-server/resources/scripts/configs.py", "-u", AMBARI_USER, "-p", AMBARI_PASSWORD,
                    "-t", AMBARI_PORT, "-a", "set", "-l", "localhost", "-n", CLUSTER_NAME, "-c", "hdfs-site", "-k",
                    "dfs.replication", "-v", "1"])
    else:
        check_call(["/var/lib/ambari-server/resources/scripts/configs.py", "-u", AMBARI_USER, "-p", AMBARI_PASSWORD,
                    "-t", AMBARI_PORT, "-a", "set", "-l", "localhost", "-n", CLUSTER_NAME, "-c", "hdfs-site", "-k",
                    "dfs.replication", "-v", "3"])

def getClusterName():
    resp = requests.get(AMBARI_URL + '/clusters/', auth=AUTH, headers=HEADERS)
    if(resp.status_code != 200):
      print "Unable to get running cluster name"
    else:
      #Get the most recent running cluster name , release noted that they cannot
      # create multiple clusters manually through bluedata running ambari server
      c_name=resp.json().get('items')[0].get('Clusters').get('cluster_name')
      print "cluster name is :",c_name
      return c_name

def main():
    while(get_request_count() !=0):
        modify_hosts_logger.info("Waiting for ongoing cluster operations to finish:")
    if IS_ADD_NODE:
        #wait for ambari agent to come up on the new worker
        modify_hosts_logger.info("Waitng for new Ambari agents to register with Ambari Server..")
        while True:
            if get_host_count() == len(ALL_HOSTS):
                modify_hosts_logger.info("Total hosts registered: " + str(len(ALL_HOSTS)))
                break
        add_hosts_blueprint = get_blueprint_for_adding_hosts(HOST_LIST.split(","), ROLE)
        expand_cluster(add_hosts_blueprint)
        if ROLE != "edge":
            check_call(["/var/lib/ambari-server/resources/scripts/configs.py", "-u", AMBARI_USER, "-p", AMBARI_PASSWORD, "-t", AMBARI_PORT, "-a", "set", "-l", "localhost", "-n", CLUSTER_NAME, "-c", "mapred-site", "-k", "mapreduce.job.max.split.locations", "-v", str(len(NODEGROUP_HOSTS)-1)])
            restart_cluster()

    else:
        if ROLE != "edge":
            decommission_slave(CHANGED_HOSTS, "HBASE_REGIONSERVER")
            decommission_slave(CHANGED_HOSTS, "DATANODE")
            decommission_slave(CHANGED_HOSTS, "NODEMANAGER")

            for host in CHANGED_HOSTS:
                stop_host_components(host)
                delete_host_components(host)
                delete_host(host)

            fix_hdfs_replication()
            check_call(["/var/lib/ambari-server/resources/scripts/configs.py", "-u", AMBARI_USER, "-p", AMBARI_PASSWORD, "-t", AMBARI_PORT, "-a", "set", "-l", "localhost", "-n", CLUSTER_NAME, "-c", "mapred-site", "-k", "mapreduce.job.max.split.locations", "-v", str(len(NODEGROUP_HOSTS)-1)])
            restart_cluster()
        else:
            for host in CHANGED_HOSTS:
                delete_host_components(host)
                delete_host(host)

CLUSTER_NAME = getClusterName()

if __name__ == '__main__':
        main()

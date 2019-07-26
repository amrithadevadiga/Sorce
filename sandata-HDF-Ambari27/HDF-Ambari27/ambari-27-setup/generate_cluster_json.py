#!/bin/env python

#
# Copyright (c) 2016, BlueData Software, Inc.
#
# Script to generate the cluster json file

from bd_vlib import *
import json, os, uuid

ConfigMeta = BDVLIB_ConfigMetadata()
FQDN = ConfigMeta.getWithTokens(["node", "fqdn"])
DISTRO_ID = ConfigMeta.getWithTokens(["node", "distro_id"])
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])

def main():
    cluster_json = {}
    cluster_json['blueprint'] = "bd-hdf"
    base_dir = os.environ["CONFIG_BASE_DIR"]
    host_groups_list = []
    cluster_json['host_groups'] = host_groups_list
    nodegroups = ConfigMeta.getWithTokens(["nodegroups"])
    roles = [ role for ng in nodegroups   for role in ConfigMeta.getWithTokens(["nodegroups", ng, "roles"])]
    fqdn_tokens = ConfigMeta.searchForToken('nodegroups', 'fqdns')
    for role in roles:
        host_group = {}
        fqdns = [ fqdn for fqdn_token in fqdn_tokens if role in fqdn_token for fqdn in ConfigMeta.getWithTokens(fqdn_token)]
        host_fqdns = []
        for fqdn in fqdns:
            host_fqdns.append({"fqdn":fqdn})
        host_group["hosts"] = host_fqdns
        host_group["name"] = role
        host_groups_list.append(host_group)

    with open(base_dir + '/cluster.json', "w") as outfile:
        json.dump(cluster_json, outfile)

if __name__ == "__main__":
    main()

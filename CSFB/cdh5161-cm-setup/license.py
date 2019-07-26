#!/bin/env python

#
# Copyright (c) 2015, BlueData Software, Inc.
#
# The main cloudera deployment script

from cm_api.api_client import ApiResource
from cm_api.endpoints.cms import ClouderaManager
from bd_vlib import *
from bdmacro import *
import os,sys
base_dir = os.environ["CONFIG_BASE_DIR"]
## Get configuration of current cluster from BDVLIB
ConfigMeta = BDVLIB_ConfigMetadata()
NODE_GROUP_ID = ConfigMeta.getWithTokens(["node", "nodegroup_id"])
def get_hosts_for_service(serviceKey, returnSingle=False):
    keylist = ConfigMeta.searchForToken(serviceKey, "fqdns")
    valuelist = []
    for key in keylist:
        valuelist.extend(ConfigMeta.getWithTokens(key))
    if returnSingle:
        if len(valuelist) > 1:
            edit_logger.warn("returnSingle arg for get_hosts_for_service is True, but > 1 service nodes for key " + str(serviceKey))
        elif len(valuelist) > 0:
            return valuelist[0]
    return valuelist


cm_host = get_hosts_for_service(["services", "cloudera_scm_server", NODE_GROUP_ID], True)
cm_port = 7180
cm_username = 'admin'
cm_password = 'admin'

license_file = base_dir+'/'+'license.txt'


api = ApiResource(cm_host, cm_port, cm_username, cm_password, version=7)
cm = ClouderaManager(api)
license = open(license_file, 'r').read()
cm.update_license(license)

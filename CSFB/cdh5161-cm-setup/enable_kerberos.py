#!/bin/env python

#
# Copyright (c) 2016, BlueData Software, Inc.
#
# Script to kerberize compute cluster in cloudera

from cm_api.api_client import *
import logging, sys, traceback
from bd_vlib import *
import time
from subprocess import Popen, PIPE, STDOUT

logger = logging.getLogger("bluedata_kerberize_cluster")
logger.setLevel(logging.DEBUG)
hdlr = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
hdlr.setFormatter(formatter)
hdlr.setLevel(logging.DEBUG)
logger.addHandler(hdlr)

ConfigMeta = BDVLIB_ConfigMetadata(metaFile="/etc/bluedata/.priv_configmeta.json")
ConfigM=BDVLIB_ConfigMetadata()
DISTRO_ID = ConfigM.getWithTokens(["node", "distro_id"])
NODE_GROUP_ID = ConfigM.getWithTokens(["node", "nodegroup_id"])
CM_HOST = ConfigM.getWithTokens(["distros", DISTRO_ID,
  NODE_GROUP_ID, "roles", "cmserver", "fqdns"])[0]
CONFIG_API_VERSION = ConfigM.get("version")
CMD_TIMEOUT = 900

api = ApiResource(CM_HOST, username="admin", password="admin")

# Utility function to execute a CM command. If the command fails with a
# timeout from CM (seen when there is resource constraint), we retry
# the command upto 10 times.
def run_cm_command(LOCAL_ENV, COMMAND, MESSAGE, IGNORE_FAILURE=False):
    # retry cm commands if they fail with a timeout
    logger.info("Executing command to " + str(MESSAGE))
    retry_count = 0
    while retry_count < 10:
        retry_count = retry_count + 1
        try:
            ret = eval(COMMAND, globals(), LOCAL_ENV)
        except ApiException as E:
            if IGNORE_FAILURE:
                logger.warn("Ignoring exception " + str(E))
                break
            if "Error while committing the transaction" in str(E):
                logger.warn("Error while committing the transaction. Retrying...")
                continue
            else:
                raise
        ret = ret.wait(CMD_TIMEOUT)
        if ret.active:
            logger.warn("Command failed to complete within the timeout period." + \
                        " Giving up and continuing with the rest of configuration")
            break
        else:
            if not ret.success:
                if "Command timed-out" in ret.resultMessage:
                   logger.warn("Cloudera manager could not complete the " + \
                               "command within the timeout period. Retrying...")
                elif IGNORE_FAILURE:
                   logger.warn("Command failed with message " + ret.resultMessage)
                   break
                else:
                   raise Exception("Command failed with message " + ret.resultMessage)
            else:
                logger.info("Successfully ran command to " + str(MESSAGE))
                break

def get_cluster():
    # connect to cloudera manager
    api = ApiResource(CM_HOST, username="admin", password="admin")
    # Take care of the case where cluster name has changed
    # Hopefully users wouldn't use this CM to deploy another cluster manually
    cluster_name = api.get_all_clusters()[0].name
    logger.info("Connected to cluster " + str(cluster_name))
    return (api, api.get_cluster(cluster_name))

def verify_packages():
    COMMAND = "rpm -qa |grep -E 'krb5-workstation|openldap|krb5-libs'"
    sp = Popen(COMMAND, shell=True, stdin=PIPE, stdout=PIPE,
               stderr=STDOUT, close_fds=True)
    (sp_out,sp_err) = sp.communicate()
    if "krb5-workstation" in sp_out and "openldap-clients" in sp_out and \
        "krb5-libs" in sp_out:
        logger.info("Packages exist")
    else:
        raise Exception("Kerberos packages do not exist, bailing out.")

def main():
    try:
        if CONFIG_API_VERSION < 3:
            raise Exception("Cannot enable kerberos when config api version is < 3")
        verify_packages()

        ###########################################################
        #### Configuration When Kerberos is set up tenant level ###
        """
        kdc_type = ConfigMeta.getWithTokens(["tenant", "kdc_type"])
        if kdc_type == '':
            raise Exception("No KDC configuration for tenant, cannot enable kerberos")
        kdc_host = ConfigMeta.getWithTokens(["tenant", "kdc_host"])
        kdc_admin_user = ConfigMeta.getWithTokens(["tenant", "kdc_admin_user"])
        kdc_admin_password = ConfigMeta.getWithTokens(["tenant", "kdc_admin_password"])
        kdc_realm = ConfigMeta.getWithTokens(["tenant", "kdc_realm"])
        ad_password_prop = 'length=15,minLowerCaseLetters=2,minUpperCaseLetters=2,minDigits=2,minSpaces=0,minSpecialChars=0,specialChars=?.!$%^*()-_+=~'
        krb_enc_types = 'rc4-hmac aes256-cts-hmac-sha1-96 ' + \
                        'aes128-cts-hmac-sha1-96 des3-cbc-sha1 ' + \
                        'arcfour-hmac des-hmac-sha1 des-cbc-md5'
        DefinedTenantKeys = ConfigMeta.getWithTokens(["tenant"])
        if "krb_enc_types" in DefinedTenantKeys:
            configured_enc_types = ConfigMeta.getWithTokens(["tenant", "krb_enc_types"])
            configured_enc_types = " ".join(configured_enc_types)
            if configured_enc_types != '':
                krb_enc_types = configured_enc_types

        if kdc_type.lower() == 'active directory' or kdc_type.lower() == 'ad':
            kdc_type = 'Active Directory'
            kdc_ad_suffix = ConfigMeta.getWithTokens(["tenant", "kdc_ad_suffix"])
            Config = {
                'KDC_HOST': kdc_host,
                'KRB_ENC_TYPES': krb_enc_types,
                'SECURITY_REALM': kdc_realm,
                'KDC_TYPE': kdc_type,
                'KRB_MANAGE_KRB5_CONF': 'true',
                'AD_KDC_DOMAIN': kdc_ad_suffix,
                'AD_PASSWORD_PROPERTIES': ad_password_prop
            }
            if "kdc_ad_ldaps_port" in DefinedTenantKeys:
                kdc_ad_ldaps_port = ConfigMeta.getWithTokens(["tenant", "kdc_ad_ldaps_port"])
                Config['AD_LDAPS_PORT'] = kdc_ad_ldaps_port
            if "kdc_ad_prefix" in DefinedTenantKeys:
                kdc_ad_prefix = ConfigMeta.getWithTokens(["tenant", "kdc_ad_prefix"])
                Config['AD_ACCOUNT_PREFIX'] = kdc_ad_prefix
        else:
            # this is for mit kdc
            Config = {
                'KDC_HOST': kdc_host,
                'KRB_ENC_TYPES': krb_enc_types,
                'SECURITY_REALM': kdc_realm,
                'KRB_MANAGE_KRB5_CONF': 'true'
            }
        """
        #########################################################
        


        (api, cluster) = get_cluster()
        cm = api.get_cloudera_manager()
        mgmt = cm.get_service()
        logger.info("stoping management services.")
        run_cm_command({'mgmt': mgmt}, "mgmt.stop().wait()", "Stop managment services", True)
        logger.info("management services stopped.")
        logger.info("stoping cluster")
        run_cm_command({'cluster':cluster}, "cluster.stop().wait()", "Stop cluster", True)
        logger.info("cluster stopped.")

        cm.update_config(Config)
        logger.info("Updated cloudera with config " + str(Config))
        run_cm_command(
            {'cm': cm},
            "cm.import_admin_credentials(username=\""+kdc_admin_user+"\", password=\"" + kdc_admin_password + "\")",
            "Import admin credentials"
        )
        logger.info("deploying client configuration.")
        run_cm_command(
            {'cluster':cluster},
            "cluster.deploy_client_config()",
            "Deploy client configuration"
        )
        logger.info("client configuration deployed.")

        logger.info("deploying cluster client configuration.")
        run_cm_command(
            {'cluster':cluster},
            "cluster.deploy_cluster_client_config()",
            "Deploy kerberos client configuration"
        )
        logger.info("cluster client configuration deployed.")

        logger.info("Generate kerberos credentials")
        run_cm_command(
            {'cm': cm},
            "cm.generate_credentials()",
            "Generate kerberos credentials"
        )

        logger.info("Configuring kerberos")
        run_cm_command(
            {'cluster':cluster},
            "cluster.configure_for_kerberos()",
            "Configure kerberos on compute cluster"
        )
        time.sleep(100)
        logger.info("Starting cluster.")
        run_cm_command(
            {'cluster':cluster},
            "cluster.start().wait()",
            "Start cluster"
        )
        logger.info("Starting management services.")
        run_cm_command(
            {'mgmt': mgmt},
            "mgmt.restart().wait()",
            "Starting management services"
        )
     
        logger.info("deploying client configuration.")
        run_cm_command(
            {'cluster':cluster},
            "cluster.deploy_client_config()",
            "Deploy client configuration"
        )
        logger.info("client configuration deployed.")

        logger.info("Restarting only stale services.")
        run_cm_command(
            {'cluster':cluster},
            "cluster.restart(restart_only_stale_services=True).wait()",
            "Restart state services if any",
            True
        )
        logger.info("Successfully enabled kerberos for compute cluster!")
    except:
        # TODO: rollback code
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
   main()


#!/bin/sh
set -o pipefail
SELF=$(readlink -nf $0)
export CONFIG_BASE_DIR=$(dirname ${SELF})
source ${CONFIG_BASE_DIR}/logging.sh
source ${CONFIG_BASE_DIR}/utils.sh
source ${CONFIG_BASE_DIR}/macros.sh
CLUSTER_NAME="$(invoke_bdvcli --get cluster.name)"
FQDN="$(invoke_bdvcli --get node.fqdn)"
ROLE="$(invoke_bdvcli --get node.role_id)"
DISTRO="$(invoke_bdvcli --get node.distro_id)"
NODEGROUP="$(invoke_bdvcli --get node.nodegroup_id)"
TOTAL_VCPU=$(invoke_bdvcli --get distros.${DISTRO}.${NODEGROUP}.roles.${ROLE}.flavor.cores)
TOTAL_VMEM=$(invoke_bdvcli --get distros.${DISTRO}.${NODEGROUP}.roles.${ROLE}.flavor.memory)

#
# Construct ESK_SERVERS_LIST
# #discovery.zen.ping.unicast.hosts: ["host1", "host2"]
ESK_SERVERS_LIST=""
ROLE_ID_RUNNING_ESK=$(invoke_bdvcli --get services.elasticsearch.1| egrep -o "[^, ]+" | sort -V | xargs | sed -e 's/ /,/g')
ESK=$(invoke_bdvcli --get services.elasticsearch.1.$ROLE_ID_RUNNING_ESK.fqdns| egrep -o "[^, ]+" | sort -V | xargs | sed -e 's/ /,/g')
IFS=', ' read -r -a ESK_LIST <<< "$ESK"
COUNT_ESK_LIST=`echo ${#ESK_LIST[@]}`

COUNT_ESK_PING_UNICAST=$COUNT_ESK_LIST
if [[ $COUNT_ESK_LIST -gt "5" ]];then
        COUNT_ESK_PING_UNICAST=5;
fi
if [[ $COUNT_ESK_LIST -eq "4" ]];then
        COUNT_ESK_PING_UNICAST=3;
fi
ESK_SERVERS_LIST=""
ESK_SERVERS_LIST=[\"${ESK_LIST[0]}\"
for ((i=1;i < $COUNT_ESK_PING_UNICAST;i++)){
        ESK_SERVERS_LIST=$ESK_SERVERS_LIST,\"${ESK_LIST[$i]}\"
}
ESK_SERVERS_LIST="$ESK_SERVERS_LIST]"
ESK_SERVERS_LIST="$(echo "$ESK_SERVERS_LIST"|sed -e 's/\"/\\"/g')"


sed --i s/@@@@NAME_CLUSTER@@@@/$CLUSTER_NAME/g /etc/elasticsearch/elasticsearch.yml
sed --i s/@@@@DISCOVERY_HOSTS@@@@/$ESK_SERVERS_LIST/g /etc/elasticsearch/elasticsearch.yml

# $(GET_TOTAL_VMEMORY_MB)=> @@@@MEM_MiB@@@@
MEM_JAVA_HEAP_MiB=`expr $TOTAL_VMEM / 2`
MEM_MiB=$MEM_JAVA_HEAP_MiB\m
sed --i s/@@@@MEM_MiB@@@@/$MEM_MiB/g /etc/elasticsearch/jvm.options

SELF=$(readlink -nf $0)
BASE_DIR=$(dirname ${SELF})

source ${BASE_DIR}/logging.sh || exit 1
source ${BASE_DIR}/utils.sh || exit 1
source ${BASE_DIR}/macros.sh || exit 1

PRIMARY_NODEGROUP=1
NODEGROUP="$(invoke_bdvcli --get node.nodegroup_id)"
DISTRO=$(invoke_bdvcli --get node.distro_id)
ROLE="$(invoke_bdvcli --get node.role_id)"
# ROLE_FQDN="$(invoke_bdvcli --get distros.${DISTRO}.${NODEGROUP}.roles.${role}.fqdns | tr ',' ' ')"
AllFQDNs=$(invoke_bdvcli --get_all_fqdns)
ESURLs=''
MASTERLIST=$(invoke_bdvcli --get distros.${DISTRO}.${NODEGROUP}.roles.master.fqdns)
DATALIST=$(invoke_bdvcli --get distros.${DISTRO}.${NODEGROUP}.roles.data.fqdns)
AllFQDNs="${MASTERLIST},${DATALIST}"

for ip in `echo ${MASTERLIST} | tr "," " "`
do
     esurl="http://${ip}:9200"
     if [[ -z ${ESURLs} ]]
     then
         ESURLs=${esurl}
     else
         ESURLs="${ESURLs}\",\"${esurl}"
     fi
done

for ip in `echo ${DATALIST} | tr "," " "`
do
     esurl="http://${ip}:9200"
     if [[ -z ${ESURLs} ]]
     then
         ESURLs=${esurl}
     else
         ESURLs="${ESURLs}\",\"${esurl}"
     fi
done

PATTERN=@@@@ESNODE_LIST@@@@
ESDESTFILE=/etc/elasticsearch/elasticsearch.yml
KBDESTFILE=/etc/kibana/kibana.yml
LOGDESTFILE=/etc/logstash/conf.d/logstash-filter.conf

if [[ "${ROLE}" == 'master' ]]
then
    sed -i "s!${PATTERN}!${AllFQDNs}!g" ${ESDESTFILE}
    echo "node.master: true" >> ${ESDESTFILE}
elif [[ "${ROLE}" == 'data' ]]
then
    sed -i "s!${PATTERN}!${AllFQDNs}!g" ${ESDESTFILE}
    echo "node.data: true" >> ${ESDESTFILE}
elif [[ "${ROLE}" == 'client' ]]
then
    sed -i "s!${PATTERN}!${AllFQDNs}!g" ${ESDESTFILE}
    echo "node.master: flase" >> ${ESDESTFILE}
    echo "node.data: false" >> ${ESDESTFILE}
elif [[ "${ROLE}" == 'logstash' ]]
then
    sed -i "s!${PATTERN}!${ESURLs}!g" ${LOGDESTFILE}
elif [[ "${ROLE}" == 'kibana' ]]
then
    sed -i "s!${PATTERN}!${ESURLs}!g" ${KBDESTFILE}
else
    echo "Current node does not match current ${ROLE}"
fi

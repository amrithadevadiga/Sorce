set -o pipefail
SELF=$(readlink -nf $0)
export CONFIG_BASE_DIR=$(dirname ${SELF})
source ${CONFIG_BASE_DIR}/logging.sh
source ${CONFIG_BASE_DIR}/utils.sh
source ${CONFIG_BASE_DIR}/macros.sh

ZOO_ID=`UNIQUE_NODE_INT`
SORTED_NODES=$(invoke_bdvcli --get_local_group_fqdns | egrep -o "[^, ]+" | sort -V | head -3 | xargs | sed -e 's/ /,/g')
IFS=', ' read -r -a ZOO_NODES <<< "$SORTED_NODES"
HOST=`hostname -f`

if [[ "${#ZOO_NODES[@]}" > "2" ]]; then
       ZOO_NODES_PORT="${ZOO_NODES[0]}:2181,${ZOO_NODES[1]}:2181,${ZOO_NODES[2]}:2181"
else
       ZOO_NODES_PORT="${ZOO_NODES[0]}:2181"
fi

sed --i s/@@@@NODES_LIST@@@@/$ZOO_NODES_PORT/g /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/nifi.properties
sed --i s/@@@@NODES_LIST@@@@/$ZOO_NODES_PORT/g /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/state-management.xml

#ZOO_HOME=/usr/lib/kafka/confluent-3.2.0/etc/kafka
#zookeeper=/usr/lib/kafka/confluent-3.2.0/etc/kafka/zookeeper.properties
if [[ "${#ZOO_NODES[@]}" > "2" ]]; then
       ZOO1=${ZOO_NODES[0]}
       ZOO1_ID=`UNIQUE_ANOTHER_NODE_INT $ZOO1`
       ZOO2=${ZOO_NODES[1]}
       ZOO2_ID=`UNIQUE_ANOTHER_NODE_INT $ZOO2`
       ZOO3=${ZOO_NODES[2]}
       ZOO3_ID=`UNIQUE_ANOTHER_NODE_INT $ZOO3`
       if [[ "${ZOO1}" == "$HOST" ]]; then
          echo "server.$ZOO1_ID=0.0.0.0:2888:3888" | tee -a /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/zookeeper.properties
          echo "server.$ZOO2_ID=${ZOO2}:2888:3888" | tee -a /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/zookeeper.properties
          echo "server.$ZOO3_ID=${ZOO3}:2888:3888" | tee -a /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/zookeeper.properties
          REGISTER_START_SERVICE_SYSV Zookeeper-service Zookeeper-service
       fi

       if [[ "${ZOO2}" == "$HOST" ]]; then
          echo "server.$ZOO1_ID=${ZOO1}:2888:3888" | tee -a /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/zookeeper.properties
          echo "server.$ZOO2_ID=0.0.0.0:2888:3888" | tee -a /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/zookeeper.properties
          echo "server.$ZOO3_ID=${ZOO3}:2888:3888" | tee -a /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/zookeeper.properties
          REGISTER_START_SERVICE_SYSV Zookeeper-service Zookeeper-service
       fi

       if [[ "${ZOO3}" == "$HOST" ]]; then
          echo "server.$ZOO1_ID=${ZOO1}:2888:3888" | tee -a /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/zookeeper.properties
          echo "server.$ZOO2_ID=${ZOO2}:2888:3888" | tee -a /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/zookeeper.properties
          echo "server.$ZOO3_ID=0.0.0.0:2888:3888" | tee -a /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/zookeeper.properties
          REGISTER_START_SERVICE_SYSV Zookeeper-service Zookeeper-service
       fi


else
       ZOO1=${ZOO_NODES[0]}
       ZOO1_ID=`UNIQUE_ANOTHER_NODE_INT $ZOO1`
       if [[ "${ZOO1}" == "$HOST" ]]; then
          echo "server.$ZOO1_ID=0.0.0.0:2888:3888" | tee -a /opt/hdf/nifi-1.1.0.2.1.2.0-10/conf/zookeeper.properties
          REGISTER_START_SERVICE_SYSV Zookeeper-service Zookeeper-service
       fi
fi

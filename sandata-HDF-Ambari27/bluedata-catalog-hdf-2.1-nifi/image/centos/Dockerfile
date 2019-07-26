FROM       bluedata/centos6:latest
RUN 	   yum -y install java-1.8.0-openjdk-devel
RUN	   yum -y erase java-1.7.0-openjdk-devel
RUN        useradd nifi
RUN	   mkdir /opt/hdf && \
           wget -q http://public-repo-1.hortonworks.com/HDF/2.1.2.0/nifi-1.1.0.2.1.2.0-10-bin.tar.gz -P /opt/hdf/ && \
           tar -xvzf /opt/hdf/nifi-1.1.0.2.1.2.0-10-bin.tar.gz -C /opt/hdf/ && \
           rm -rf /opt/hdf/nifi-1.1.0.2.1.2.0-10-bin.tar.gz
RUN 	   wget -q http://apache-mirror.rbc.ru/pub/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz -P /opt/ &&\
	   tar -xvzf /opt/zookeeper-3.4.6.tar.gz -C /opt/ && \
	   rm -rf /opt//zookeeper-3.4.6.tar.gz
RUN 	   chown -R nifi:nifi /opt/hdf/
RUN 	   chown -R root:root /opt/zookeeper-3.4.6/
RUN	   mv /opt/zookeeper-3.4.6/conf/zoo_sample.cfg /opt/zookeeper-3.4.6/conf/zoo.cfg

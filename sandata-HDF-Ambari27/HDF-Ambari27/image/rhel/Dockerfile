FROM bluedata/rhel7:latest
#Set ${RHEL_USERNAME} and ${RHEL_PASSWORD} environment variables
RUN yum install https://dl.fedoraproject.org/pub/epel/epel-release-latest-$(rpm -E '%{rhel}').noarch.rpm -y
RUN wget http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.5.2.0/ambari.repo -P /etc/yum.repos.d
RUN wget http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.4.0/hdp.repo -P /etc/yum.repos.d
RUN subscription-manager register --username ${RHEL_USERNAME} --password ${RHEL_PASSWORD} && \
    subscription-manager attach --auto && \
    subscription-manager repos --enable=rhel-6-server-optional-rpms && \
    yum install -y deltarpm ambari* ntp yum-utils createrepo postgresql* mariadb* mysql-connector* python-pip zookeeper*\
                   hadoop* snappy* lzo* hbase* phoenix tez pig hive* oozie* ranger* hue sqoop mahout flume* storm*\
                   spark* kafka zeppelin* livy* accumulo falcon knox slider* atlas* krb5* datafu* extjs && \
    subscription-manager unsubscribe --all && subscription-manager unregister && subscription-manager clean

#Ambari Software Requirements
RUN wget --header "Cookie: oraclelicense=accept-securebackup-cookie" \
      "http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.rpm"
RUN wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" \
      "http://download.oracle.com/otn-pub/java/jce/8/jce_policy-8.zip"
RUN yum localinstall -y jdk-8u131-linux-x64.rpm
RUN unzip jce_policy-8.zip
RUN mv /usr/java/default/jre/lib/security/local_policy.jar /usr/java/default/jre/lib/security/local_policy.jar.backup
RUN mv /usr/java/default/jre/lib/security/US_export_policy.jar /usr/java/default/jre/lib/security/US_export_policy.jar.backup
RUN mv UnlimitedJCEPolicyJDK8/*.jar /usr/java/default/jre/lib/security/
RUN rm -f jce_policy-8.zip
#RUN wget http://public-repo-1.hortonworks.com/ambari/centos6/2.x/updates/2.4.2.0/ambari-2.4.2.0-centos6.tar.gz
#RUN wget http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.5.3.0/HDP-2.5.3.0-centos6-rpm.tar.gz
#RUN wget http://public-repo-1.hortonworks.com/HDP-UTILS-1.1.0.21/repos/centos6/HDP-UTILS-1.1.0.21-centos6.tar.gz
RUN pip install requests
RUN rm -rf /etc/yum.repos.d/hdp.repo

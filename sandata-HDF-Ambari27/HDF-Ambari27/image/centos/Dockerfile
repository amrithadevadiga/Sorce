FROM bluedata/centos7:latest

#Ambari Software Requirements
RUN yum -y install epel-release
RUN wget http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.7.1.0/ambari.repo -P /etc/yum.repos.d
RUN wget http://public-repo-1.hortonworks.com/HDF/centos7/3.x/updates/3.3.0.0/hdf.repo -P /etc/yum.repos.d
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
RUN yum install -y ambari* ntp python-pip
RUN pip install requests

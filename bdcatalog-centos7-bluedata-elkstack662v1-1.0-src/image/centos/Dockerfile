# image for RHEL/CentOS

FROM bluedata/centos7:latest

# Install EPEL repo
# RUN yum install -y http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
RUN yum install -y http://download.fedoraproject.org//pub/epel/epel-release-latest-7.noarch.rpm
#RUN yum install -y epel-release-latest

#Remove java7 and install java 8
RUN yum -y erase java-1.7.0-openjdk-devel
## Install java 8 and configure
ADD configure_java8.sh /root/configure_java8.sh

RUN chmod +x /root/configure_java8.sh && /root/configure_java8.sh && rm -f /root/configure_java8.sh

RUN echo 1 | update-alternatives --config java

# Install EPEL repo
RUN yum install -y bzip2 nc unzip wget which
RUN yum install -y expect mysql-server mysql-connector-java \
                   php-5.3.3 php-xml php-pear php-gd libcurl-devel openssl-devel libxml2-devel

RUN sudo yum -y install https://centos7.iuscommunity.org/ius-release.rpm

# Install ElasticSearch
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.6.2.rpm \
    && sudo rpm --install elasticsearch-6.6.2.rpm

# Install Kibana
RUN wget https://artifacts.elastic.co/downloads/kibana/kibana-6.6.2-x86_64.rpm \
    && sudo rpm --install kibana-6.6.2-x86_64.rpm

# Install Logstash
RUN wget https://artifacts.elastic.co/downloads/logstash/logstash-6.6.2.rpm \
    && sudo rpm --install logstash-6.6.2.rpm

# ENV ES_JAVA_OPTS="$ES_JAVA_OPTS -Xms512m -Xmx512m"

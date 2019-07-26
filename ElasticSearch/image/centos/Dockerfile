# Elastic Search 6.2.4  docker image

# 
FROM bluedata/centos7:latest
#RUN wget "http://download.oracle.com/otn-pub/java/jdk/10.0.1+10/fb4372174a714e6b8c52526dc134031e/jdk-10.0.1_linux-x64_bin.rpm"  -O  jdk-10_linux-x64_bin.rpm
#RUN yum -y erase java-1.7.0-openjdk
#RUN yum install -y jdk-10_linux-x64_bin.rpm

# Remove openjdk we installed in base image
RUN yum -y erase java-1.7.0-openjdk
ENV JAVA_HOME ''
## Install java 8 and configure
#ADD configure_java8.sh /root/configure_java8.sh
#RUN chmod +x /root/configure_java8.sh && /root/configure_java8.sh && rm -f /root/configure_java8.sh

#RUN echo 1 | update-alternatives --config java

#RUN echo "export PATH=$PATH:/opt/jdk1.8.0_131/bin" > /etc/profile.d/updatePath.sh


#INSTALL Java Home
#RUN wget -c --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.tar.gz --directory-prefix=/opt/

ADD jdk-8u162-linux-x64.tar.gz /opt/
RUN alternatives --install /usr/bin/java java /opt/jdk1.8.0_162/bin/java 1
RUN alternatives --install /usr/bin/jar jar /opt/jdk1.8.0_162/bin/jar 1
RUN alternatives --install /usr/bin/javac javac /opt/jdk1.8.0_162/bin/javac 1
RUN update-alternatives --set jar /opt/jdk1.8.0_162/bin/jar
RUN update-alternatives --set javac /opt/jdk1.8.0_162/bin/javac

RUN export JAVA_HOME=/opt/jdk1.8.0_162
RUN export PATH=$JAVA_HOME/bin:$PATH



RUN rpm --import https://artifacts.elastic.co/GPG-KEY-elasticsearch

#RUN export JAVA_HOME=/opt/jdk-10.0.1_linux-x64_bin
#RUN export PATH=$JAVA_HOME/bin:$PATH


#RUN printf "[elasticsearch-6.x] \n\
#name=Elasticsearch repository for 6.x packages \n\
#baseurl=https://artifacts.elastic.co/packages/6.x/yum \n\
#gpgcheck=1 \n\
#gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch \n\
#enabled=1 \n\
#autorefresh=1 \n\
#type=rpm-md"  >/etc/yum.repos.d/esk.repo;

#RUN yum install elasticsearch -y



# Install EPEL repo
# RUN yum install -y http://download.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
RUN yum install -y http://download.fedoraproject.org//pub/epel/epel-release-latest-7.noarch.rpm
#RUN yum install -y epel-release-latest


# Install EPEL repo
RUN yum install -y bzip2 nc unzip wget which
RUN yum install -y expect mysql-server mysql-connector-java \
                   php-5.3.3 php-xml php-pear php-gd libcurl-devel openssl-devel libxml2-devel

RUN sudo yum -y install https://centos7.iuscommunity.org/ius-release.rpm

# Install ElasticSearch
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.6.2.rpm \
    && sudo rpm --install elasticsearch-6.6.2.rpm

ENV ES_JAVA_OPTS="$ES_JAVA_OPTS -Xms513m -Xmx512m"


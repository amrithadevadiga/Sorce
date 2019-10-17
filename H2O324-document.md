# Overview

In this image,user can access H2O Master and H2O Slaves.

## What is {name}?

H2O is an in-memory platform for distributed, scalable machine learning. H2O uses familiar interfaces like R, Python, Scala, Java, JSON and the Flow notebook/web interface, and works seamlessly with big data technologies like Hadoop and Spark.

For more details about H2O, refer to this link:
[H20_Docs](http://docs.h2o.ai/h2o/latest-stable/h2o-docs/welcome.html)


# Details:
## Application Image Details:

### AppName: 
{name}
### DistroId:
{distroid}
### Version: 
{version}

### Cluster Type: 
H2O
### Software Included: 
EPEL Repo 7.11,Shell in a box 2.20-5.el7,Java 1.8.0,AWS Java SDK jar 1.7.4,Hadoop AWS jar 2.7.1 ,H2O 3.24

### H2O access:   
To access the H2O GUI navigate to <IP_Address>:<Port>, from the H2O Cluster.
This GUI allows to Monitor and Manage the data, Create Alerts.

### Systemctl service names and commands:  
sudo systemctl h2o h2o.service

sudo systemctl webssh shellinaboxd.service

### OS: 
Centos7. Works with both Bluedata Centos and RHEL hosts

## Testcases:

[ H2O_testcases](https://github.com/bluedatainc/solutions/blob/master/AppImage_Testcases/)

## Prerequisites:

The user should have prior experience working with the App.
Prior Knowledge working with EPIC.
Basic understanding of Docker, Python/Java and Confluent Kafka for running testcases.

## Release notes:

None


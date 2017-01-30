# Axon JMS

Integrates [AxonFramework](https://github.com/AxonFramework/AxonFramework) with a JMS Message/Event Broker.

[![Build Status](https://travis-ci.org/sventorben/axon-jms.svg?branch=master)](https://travis-ci.org/sventorben/axon-jms)
[![Code Coverage](https://img.shields.io/codecov/c/github/sventorben/axon-jms/master.svg)](https://codecov.io/github/sventorben/axon-jms?branch=master)

This is basically an adaptation of [Axon's AMQP implementation](https://github.com/AxonFramework/AxonFramework/tree/axon-3.0.1/amqp).

## Maven Coordinates

**_ONLY SNAPSHOTS FOR NOW_**

The latest RELEASE artifacts are available via Maven Central. 

```
    <dependency>
        <groupId>de.sven-torben.axon</groupId>
        <artifactId>axon-jms</artifactId>
        <version>0.0.1</version>
    </dependency>
```

For latest SNAPHOT artifacts you need to integrate the Sonatype Maven repository like this: 

```
    <repository>
        <id>sonatype-nexus-snapshots</id>
        <name>Sonatype Nexus Snapshots</name>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        <releases>
            <enabled>false</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository
```

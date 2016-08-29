**kafka-connect-dynamodb** is a `Kafka Connector <http://kafka.apache.org/documentation.html#connect>`_ for loading data from and to Amazon DynamoDB.

It is implemented using the AWS Java SDK for DynamoDB.
For authentication, the `DefaultAWSCredentialsProviderChain <http://docs.aws.amazon.com/java-sdk/latest/developer-guide/credentials.html#id6>`_ is used.

Source Connector
================

Example configuration
---------------------

This will attempt to ingest all DynamoDB tables in the specified region, to Kafka topics with the same name as the source table::

    name=mytest
    connector.class=dynamok.source.DynamoDbSourceConnector
    region=us-west-2

Limitations
-----------

DynamoDB records containing nested structures like heterogeneous lists (``L``) or maps (``M``) are not fully supported, these fields will be dropped.
It will be possible to add support for them with the implementation of `KAFKA-3910 <https://issues.apache.org/jira/browse/KAFKA-3910>`_.

Configuration options
---------------------

``region``
  AWS region for the source DynamoDB.

  * Type: string
  * Default: ""
  * Importance: high

``topic.format``
  Format string for destination Kafka topic, use ``${table}`` as placeholder for source table name.

  * Type: string
  * Default: "${table}"
  * Importance: high

``tables.blacklist``
  Blacklist for DynamoDB tables to source from.

  * Type: string
  * Importance: medium

``tables.regex``
  Prefix for DynamoDB tables to source from.

  * Type: string
  * Importance: medium

``tables.whitelist``
  Whitelist for DynamoDB tables to source from.

  * Type: string
  * Importance: medium


Sink Connector
==============

TODO

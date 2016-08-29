**kafka-connect-dynamodb** is a `Kafka Connector <http://kafka.apache.org/documentation.html#connect>`_ for loading data from and to Amazon DynamoDB.

Source Connector
================

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

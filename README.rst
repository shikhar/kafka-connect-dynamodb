**kafka-connect-dynamodb** is a `Kafka Connector <http://kafka.apache.org/documentation.html#connect>`_ for loading data to and from Amazon DynamoDB.

It is implemented using the AWS Java SDK for DynamoDB.
For authentication, the `DefaultAWSCredentialsProviderChain <http://docs.aws.amazon.com/java-sdk/latest/developer-guide/credentials.html#id6>`_ is used.

Building
========

Run::

    $ mvn clean package

Then you will find this connector and required JARs it depends upon in ``target/kafka-connect-dynamodb-$version-SNAPSHOT-package/share/java/kafka-connect-dynamodb/*``.

To create an uber JAR::

    $ mvn -P standalone clean package

The uber JAR will be created at ``target/kafka-connect-dynamodb-$version-SNAPSHOT-standalone.jar``.

Sink Connector
==============

Example configuration
---------------------

Ingest the ``orders`` topic to a DynamoDB table of the same name in the specified region::

    name=dynamodb-sink-test
    topics=orders
    connector.class=dynamok.sink.DynamoDbSinkConnector
    region=us-west-2
    ignore.record.key=true

Record conversion
-----------------

Refer to `DynamoDB Data Types <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes>`_.

At the top-level, we need to either be converting to the DynamoDB ``Map`` data type,
or the ``top.key.attribute`` or ``top.value.attribute`` configuration options for the Kafka record key or value as applicable should be configured,
so we can ensure being able to hoist the converted value as a DynamoDB record.

Schema present
^^^^^^^^^^^^^^

================================================================================  =============
**Connect Schema Type**                                                           **DynamoDB**
--------------------------------------------------------------------------------  -------------
``INT8``, ``INT16``, ``INT32``, ``INT64``, ``FLOAT32``, ``FLOAT64``, ``Decimal``  ``Number``
``BOOL``                                                                          ``Boolean``
``BYTES``                                                                         ``Binary``
``STRING``                                                                        ``String``
``ARRAY``                                                                         ``List``
``MAP`` [#]_, ``STRUCT``                                                          ``Map``
================================================================================  =============

.. [#] Map keys must be primitive types, and cannot be optional.

``null`` values for optional schemas are translated to the ``Null`` type.

Schemaless
^^^^^^^^^^

======================================================================================= ==============
**Java**                                                                                **DynamoDB**
--------------------------------------------------------------------------------------- --------------
``null``                                                                                ``Null``
``Number`` [#]_                                                                         ``Number``
``Boolean``                                                                             ``Boolean``
``byte[]``, ``ByteBuffer``                                                              ``Binary``
``String``                                                                              ``String``
``List``                                                                                ``List``
Empty ``Set`` [#]_                                                                      ``Null``
``Set<String>``                                                                         ``String Set``
``Set<Number>``                                                                         ``Number Set``
``Set<byte[]>``, ``Set<ByteBuffer>``                                                    ``Binary Set``
``Map`` [#]_                                                                            ``Map``
======================================================================================= ==============

Any other datatype will result in the connector to fail.

.. [#] i.e. ``Byte``, ``Short``, ``Integer``, ``Long``, ``Float``, ``Double``, ``BigInteger``, ``BigDecimal``

.. [#] It is not possible to determine the element type of an empty set.

.. [#] Map keys must be primitive types, and cannot be optional.

Configuration options
---------------------

``region``
  AWS region for the source DynamoDB.

  * Type: string
  * Default: ""
  * Importance: high

``batch.size``
  Batch size between 1 (dedicated ``PutItemRequest`` for each record) and 25 (which is the maximum number of items in a ``BatchWriteItemRequest``)

  * Type: int
  * Default: 1
  * Importance: high

``kafka.attributes``
  Trio of ``topic,partition,offset`` attribute names to include in records, set to empty to omit these attributes.

  * Type: list
  * Default: [kafka_topic, kafka_partition, kafka_offset]
  * Importance: high

``table.format``
  Format string for destination DynamoDB table name, use ``${topic}`` as placeholder for source topic.

  * Type: string
  * Default: "${topic}"
  * Importance: high

``ignore.record.key``
  Whether to ignore Kafka record keys in preparing the DynamoDB record.

  * Type: boolean
  * Default: false
  * Importance: medium

``ignore.record.value``
  Whether to ignore Kafka record value in preparing the DynamoDB record.

  * Type: boolean
  * Default: false
  * Importance: medium

``top.key.attribute``
  DynamoDB attribute name to use for the record key. Leave empty if no top-level envelope attribute is desired.

  * Type: string
  * Default: ""
  * Importance: medium

``top.value.attribute``
  DynamoDB attribute name to use for the record value. Leave empty if no top-level envelope attribute is desired.

  * Type: string
  * Default: ""
  * Importance: medium

``max.retries``
  The maximum number of times to retry on errors before failing the task.

  * Type: int
  * Default: 10
  * Importance: medium

``retry.backoff.ms``
  The time in milliseconds to wait following an error before a retry attempt is made.

  * Type: int
  * Default: 3000
  * Importance: medium

Source Connector
================

Example configuration
---------------------

Ingest all DynamoDB tables in the specified region, to Kafka topics with the same name as the source table::

    name=dynamodb-source-test
    connector.class=dynamok.source.DynamoDbSourceConnector
    region=us-west-2

Record conversion
-----------------

*TODO describe conversion scheme*

Limitations
^^^^^^^^^^^

DynamoDB records containing heterogeneous lists (``L``) or maps (``M``) are not currently supported, these fields will be silently dropped.
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

``tables.prefix``
  Prefix for DynamoDB tables to source from.

  * Type: string
  * Default: ""
  * Importance: medium

``tables.whitelist``
  Whitelist for DynamoDB tables to source from.

  * Type: list
  * Default: ""
  * Importance: medium

``tables.blacklist``
  Blacklist for DynamoDB tables to source from.

  * Type: list
  * Default: ""
  * Importance: medium

